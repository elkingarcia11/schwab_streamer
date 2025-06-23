import os
import time
import pandas as pd
import requests
import pytz
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from schwab_auth import SchwabAuth
from indicator_generator import IndicatorGenerator
from signal_processor import SignalProcessor, Trade
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp


class DataManager:
    def __init__(self, schwab_auth: SchwabAuth, symbols: List[str], timeframes: List[str]):
        """
        Initialize the DataManager class
        """
        self.schwab_auth = schwab_auth
        self.indicator_generator = IndicatorGenerator()
        self.signal_processor = SignalProcessor()

        # Store the most recent DataFrame for each (symbol, timeframe) pair
        self.latestDF: Dict[str, pd.DataFrame] = {}
        
        # Import symbols from symbols.txt
        self.symbols = symbols
        # Valid timeframes for API calls
        self.timeframes = timeframes
        # Create data directory and timeframe subdirectories if they don't exist
        os.makedirs('data', exist_ok=True)
        for timeframe in self.timeframes:
            os.makedirs(f'data/{timeframe}', exist_ok=True)
        
        # Eastern Time zone
        self.et_tz = pytz.timezone('US/Eastern')
        
        # Market hours (Eastern Time)
        self.market_open = datetime.strptime('09:30', '%H:%M').time()
        self.market_close = datetime.strptime('16:00', '%H:%M').time()

        # Load dataframes from csv files
        for symbol in self.symbols:
            for timeframe in self.timeframes:
                # Load from csv files or create new one
                df = self.initialize_dataframe(symbol, timeframe)
                self.indicator_generator.initialize_indicators_state(symbol, timeframe, df)

    def initialize_dataframe(self, symbol: str, timeframe: str):
        """ Initialize DataFrame for a given symbol and timeframe """
        # Check if DataFrame already exists in memory
        df = self._load_df_from_csv(symbol, timeframe)
        if df is None:
            print(f"🔄 Initializing new DataFrame for {symbol} {timeframe}")
            df = pd.DataFrame(columns=pd.Index(['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'ema', 'vwma', 'roc', 'macd', 'macd_signal']))
            # Save empty DataFrame to CSV to create the file
            self._save_df_to_csv(df, symbol, timeframe)
        else:
            print(f"🔄 Loading existing DataFrame for {symbol} {timeframe}")
        self.latestDF[f"{symbol}_{timeframe}"] = df
        return df 

    def _get_dataframe(self, symbol: str, timeframe: str):
        """Fetch the latest DataFrame for a given symbol and timeframe"""
        df = self.latestDF[f"{symbol}_{timeframe}"]
        return df
    
    def _extract_frequency_number(self, interval) -> int:
        """Extract numeric frequency from interval string (e.g., '5m' -> 5) or return int if already int"""
        try:
            # If it's already an integer, return it directly
            if isinstance(interval, int):
                return interval
            # If it's a string, extract the number
            return int(interval.replace('m', '').replace('h', '').replace('d', ''))
        except (ValueError, AttributeError):
            print(f"⚠️  Invalid interval format: {interval}, defaulting to 1")
            return 1

    def _get_valid_period(self, days_to_end: int) -> int:
        """
        Get the largest valid period that's not greater than days_to_end.
        Valid periods for periodType=day are [1, 2, 3, 4, 5, 10].
        
        Args:
            days_to_end: Number of days to the end date
            timeframe: Timeframe string (e.g., '1m', '5m') to determine default period
            
        Returns:
            int: Valid period to use for API call
        """
        valid_periods = [1, 2, 3, 4, 5, 10]
        
        # Use period=10 for better compatibility with the API
        default_period = 10
        
        # Find the largest valid period that fits within our date range
        # If default_period fits, use it; otherwise use the largest available
        if default_period <= days_to_end:
            return default_period
        else:
            return max([p for p in valid_periods if p <= days_to_end])

    def _get_market_open_today(self) -> datetime:
        """Get today's market open time (9:30 AM ET)"""
        today_et = datetime.now(self.et_tz).date()
        market_open_dt = datetime.combine(today_et, self.market_open)
        return self.et_tz.localize(market_open_dt)

    def _get_last_completed_timestamp(self) -> datetime:
        """Get the last completed timestamp during market hours (ET)"""
        now_et = datetime.now(self.et_tz)
        today_et = now_et.date()
        
        # Check if it's currently market hours
        current_time = now_et.time()
        
        # If it's before market open or after market close, use previous trading day
        if current_time < self.market_open or current_time > self.market_close:
            # Use previous trading day (skip weekends)
            days_back = 1
            if today_et.weekday() == 0:  # Monday
                days_back = 3  # Go back to Friday
            elif today_et.weekday() == 6:  # Sunday
                days_back = 2  # Go back to Friday
            
            previous_trading_day = today_et - timedelta(days=days_back)
            end_time = datetime.combine(previous_trading_day, self.market_close)
            return self.et_tz.localize(end_time)
        else:
            # During market hours, use current time
            return now_et

    def _get_csv_filename(self, symbol: str, timeframe: str) -> str:
        """Get the CSV filename for a symbol and timeframe"""
        return f"data/{timeframe}/{symbol}.csv"

    def _load_df_from_csv(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Load DataFrame from CSV file"""
        csv_filename = self._get_csv_filename(symbol, timeframe)
        try:
            df = pd.read_csv(csv_filename)
            return df
        except FileNotFoundError:
            print(f"❌ Error loading file {csv_filename}: File not found")
            return None
        except Exception as e:
            print(f"❌ Error loading CSV file {csv_filename}: {e}")
            return None

    def _save_df_to_csv(self, df: pd.DataFrame, symbol: str, timeframe: str):
        """Save DataFrame to CSV file"""
        csv_filename = self._get_csv_filename(symbol, timeframe)
        try:
            df.to_csv(csv_filename, index=False)
            print(f"💾 Saved {len(df)} records to {csv_filename}")
        except Exception as e:
            print(f"❌ Error saving CSV {csv_filename}: {e}")

    def append_row_to_csv(self, row: pd.Series, symbol: str, timeframe: str):
        """
        Efficiently append a single row to CSV file for real-time streaming
        """
        csv_filename = self._get_csv_filename(symbol, timeframe)
        try:
            # Convert row to DataFrame for to_csv compatibility
            row_df = row.to_frame().T
            # Prevent duplicate: check if last row in file has same timestamp
            try:
                # Count lines in file to get last row
                with open(csv_filename, 'r') as f:
                    line_count = sum(1 for _ in f)
                
                if line_count > 1:  # File has data beyond header
                    last_row = pd.read_csv(csv_filename, nrows=1, skiprows=line_count-1)
                    if 'timestamp' in last_row.columns and row['timestamp'] == last_row['timestamp'].iloc[0]:
                        print(f"⚠️  Duplicate row detected for {symbol} {timeframe} (timestamp={row['timestamp']}), skipping append.")
                        return
            except Exception as e:
                print(f"⚠️  Could not check for duplicate row in {csv_filename}: {e}")
            # Append to CSV (file should exist from initialize_dataframe)
            row_df.to_csv(csv_filename, mode='a', header=False, index=False)
        except Exception as e:
            print(f"❌ Error appending to CSV {csv_filename}: {e}")
            # Fallback to full save if append fails
            try:
                df_key = f"{symbol}_{timeframe}"
                if df_key in self.latestDF:
                    self._save_df_to_csv(self.latestDF[df_key], symbol, timeframe)
            except Exception as fallback_error:
                print(f"❌ Fallback save also failed: {fallback_error}")

    def _fetch_data_from_schwab(self, symbol: str, start_date: datetime, end_date: datetime, 
                               interval_to_fetch: int) -> Optional[pd.DataFrame]:
        """
        Fetch data from Schwab API for a given date range
        
        Args:
            symbol: Stock symbol
            start_date: Start datetime
            end_date: End datetime
            interval_to_fetch: Interval in minutes
            
        Returns:
            DataFrame with fetched data or None if failed
        """
        if not symbol:
            print("❌ Invalid symbol provided")
            return None

        symbol = symbol

        # Get access token (this handles all credential validation and token refresh internally)
        access_token = self.schwab_auth.get_access_token()
        if not access_token:
            print("❌ Failed to get valid access token")
            return None

        headers = {
            'Authorization': f'Bearer {access_token}'
        }

        url = "https://api.schwabapi.com/marketdata/v1/pricehistory"

        all_candles = []
        current_start_dt = start_date

        while current_start_dt <= end_date:
            # Calculate period based on date range
            days_to_end = (end_date - current_start_dt).days + 1
            
            # Get the largest valid period that fits within our date range
            period = self._get_valid_period(days_to_end)
            
            # Calculate the actual end date based on the period
            current_end_dt = min(current_start_dt + timedelta(days=period-1), end_date)

            # Convert start and end dates to UNIX epoch milliseconds
            # Ensure datetime objects are timezone-aware before conversion
            if not current_start_dt.tzinfo:
                current_start_dt = self.et_tz.localize(current_start_dt)
            if not current_end_dt.tzinfo:
                current_end_dt = self.et_tz.localize(current_end_dt)

            start_time_ms = int(current_start_dt.timestamp() * 1000)
            end_time_ms = int(current_end_dt.timestamp() * 1000)

            params = {
                'symbol': symbol,  # Use single symbol parameter to match working URL
                'periodType': 'day',
                'period': period,
                'frequencyType': 'minute',
                'frequency': self._extract_frequency_number(interval_to_fetch),
                'startDate': start_time_ms,
                'endDate': end_time_ms,
                'needExtendedHoursData': 'false',
                'needPreviousClose': 'false'
            }

            print(f"📡 Fetching price history for {symbol} ({interval_to_fetch}m) from {current_start_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} to {current_end_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} (period={period})")

            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                # Sleep for 1 second to avoid rate limiting
                time.sleep(1)

                if response.status_code == 200:
                    data = response.json()
                    
                    if 'candles' in data and data['candles']:
                        candles = data['candles']
                        print(f"✅ Retrieved {len(candles)} candles from Schwab API")
                        all_candles.extend(candles)
                    else:
                        print("📊 No candle data found in API response")
                else:
                    print(f"❌ API request failed: {response.status_code}")
                    if response.text:
                        print(f"Response: {response.text[:200]}...")
                    return None

            except requests.exceptions.RequestException as e:
                print(f"❌ Network error fetching price history: {e}")
                return None
            except Exception as e:
                print(f"❌ Unexpected error fetching price history: {e}")
                return None

            # Move to next time window
            current_start_dt = current_end_dt + timedelta(days=1)  # Add 1 day to avoid overlap

        # Process the fetched data
        if all_candles:
            try:
                # Convert candles to DataFrame with proper structure
                df_data = []
                for candle in all_candles:
                    # Convert timestamp to ET datetime
                    timestamp_ms = candle.get('datetime', 0)
                    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                    dt_et = dt_utc.astimezone(self.et_tz)
                    
                    df_data.append({
                        'timestamp': timestamp_ms,
                        'datetime': dt_et.strftime('%Y-%m-%d %H:%M:%S %Z'),
                        'open': candle.get('open', 0),
                        'high': candle.get('high', 0),
                        'low': candle.get('low', 0),
                        'close': candle.get('close', 0),
                        'volume': candle.get('volume', 0)
                    })

                df = pd.DataFrame(df_data)

                # Sort by timestamp and remove duplicates
                df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'])
                
                print(f"✅ Processed {len(df)} records from API")
                return df

            except Exception as e:
                print(f"❌ Error processing data: {e}")
                return None
        else:
            print(f"⚠️  No data retrieved for {symbol}_{interval_to_fetch}m")
            return None

    def _fetch_base_symbol_data(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Fetch the latest data for a base symbol (not inverse) with incremental updates
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            timeframe: Timeframe (e.g., '1m', '5m', '10m', '15m', '30m')
            
        Returns:
            DataFrame with latest data and indicators, or None if failed
        """
        df_key = f"{symbol}_{timeframe}"
        interval_minutes = self._extract_frequency_number(timeframe)
        
        print(f"🔄 Fetching base symbol data for {symbol} {timeframe}")
        
        # Check if DataFrame exists in memory
        if df_key in self.latestDF and len(self.latestDF[df_key]) > 0:
            original_df = self.latestDF[df_key]
            if timeframe == '1m':
                # For 1m: fetch from today's market open to last completed timestamp
                last_datetime_et = self._get_market_open_today()
            else:
                # Get the last timestamp from existing data
                last_timestamp = original_df['timestamp'].max()
                # Convert last timestamp to datetime in UTC and then to ET
                last_datetime_utc = datetime.fromtimestamp(last_timestamp / 1000, tz=pytz.UTC)
                last_datetime_et = last_datetime_utc.astimezone(self.et_tz)
                
            # Fetch new data from last timestamp to current time
            end_date = self._get_last_completed_timestamp()
            
            if last_datetime_et < end_date:
                print(f"📡 Fetching new data from {last_datetime_et.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                new_df = self._fetch_data_from_schwab(symbol, last_datetime_et, end_date, interval_minutes)
                
                if new_df is not None and len(new_df) > 0:
                    # Filter out data that might overlap with existing data
                    new_df = new_df[new_df['timestamp'] > last_timestamp]
                    
                    if len(new_df) > 0:
                        print(f"📈 Processing {len(new_df)} new records incrementally")
                        
                        # Calculate indicators incrementally
                        result_df = self.indicator_generator.smart_indicator_calculation(
                            symbol, timeframe, original_df, new_df if isinstance(new_df, pd.DataFrame) else pd.DataFrame([new_df])
                        )
                        
                        # Check if result is valid
                        if result_df is None or (hasattr(result_df, 'empty') and result_df.empty) or len(result_df) == 0:
                            print(f"❌ Error: Indicator calculation returned empty result for {symbol} {timeframe}")
                            return original_df
                        
                        # Save updated result and update memory
                        self._save_df_to_csv(result_df, symbol, timeframe)
                        self.latestDF[df_key] = result_df
                        
                        print(f"✅ Updated {symbol} {timeframe} with {len(new_df)} new records")
                        return result_df
                    else:
                        print(f"📊 No new data available for {symbol} {timeframe}")
                        return original_df
                else:
                    print(f"📊 No new data fetched for {symbol} {timeframe}")
                    return original_df
            else:
                print(f"📊 Data is already up to date for {symbol} {timeframe}")
                return original_df          
        else:
            # Perform initial historical fetch
            # No existing data, use default start dates based on timeframe
            if timeframe == '1m':
                # For 1m: fetch from today's market open to last completed timestamp
                start_date = self._get_market_open_today()
            else:
                # For 5m, 10m, 15m, 30m: fetch from January 1, 2025 to last completed timestamp
                start_date = self.et_tz.localize(datetime(2025, 1, 1))
                print(f"📊 No existing data found, using default start date: {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
            end_date = self._get_last_completed_timestamp()
            
            print(f"📡 Initial fetch for {symbol} {timeframe} from {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            df = self._fetch_data_from_schwab(symbol, start_date, end_date, interval_minutes)
            
            if df is not None and len(df) > 0:
                # Save to memory and CSV
                self.latestDF[df_key] = df
                self._save_df_to_csv(df, symbol, timeframe)
                
                # Calculate indicators on the initial data
                print(f"📊 Calculating indicators on initial data for {symbol} {timeframe}")
                result_df = self.indicator_generator.smart_indicator_calculation(
                    symbol, timeframe, df, pd.DataFrame()
                )
                
                # Check if result is valid
                if result_df is None or (hasattr(result_df, 'empty') and result_df.empty) or len(result_df) == 0:
                    print(f"❌ Error: Indicator calculation returned empty result for {symbol} {timeframe}")
                    return None
                
                # Save result and update memory
                self._save_df_to_csv(result_df, symbol, timeframe)
                self.latestDF[df_key] = result_df
                
                print(f"✅ Initial fetch completed for {symbol} {timeframe} with {len(result_df)} records")
                return result_df
            else:
                print(f"❌ Failed to fetch initial data for {symbol} {timeframe}")
                return None

    def fetchLatest(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Fetch the latest data for a symbol and timeframe with incremental updates
        
        Args:
            symbol: Stock symbol (e.g., 'SPY' or 'SPY_inverse')
            timeframe: Timeframe (e.g., '1m', '5m', '10m', '15m', '30m')
            ema_period: EMA period for indicators
            vwma_period: VWMA period for indicators
            roc_period: ROC period for indicators
            fast_ema: Fast EMA for MACD
            slow_ema: Slow EMA for MACD
            signal_ema: Signal EMA for MACD
            
        Returns:
            DataFrame with latest data and indicators, or None if failed
        """
        if timeframe not in self.timeframes:
            print(f"❌ Invalid timeframe: {timeframe}. Valid timeframes: {self.timeframes}")
            return None
            
        # Check if this is an inverse symbol
        is_inverse = symbol.endswith('_inverse')
        base_symbol = symbol.replace('_inverse', '') if is_inverse else symbol
        
        symbol = symbol
        base_symbol = base_symbol
        
        print(f"🔄 Fetching latest data for {symbol} {timeframe} (inverse: {is_inverse})")
        
        # For inverse symbols, fetch the base symbol data first
        if is_inverse:
            # Fetch base symbol data
            base_df = self._fetch_base_symbol_data(base_symbol, timeframe)
            
            if base_df is not None and len(base_df) > 0:
                # Generate inverse data
                inverse_df = self._generate_inverse_data(base_df)
                
                # Calculate indicators for inverse data
                result_df = self.indicator_generator.smart_indicator_calculation(
                    symbol, timeframe, inverse_df, pd.DataFrame()
                )
                
                # Check if result is valid
                if result_df is None or (hasattr(result_df, 'empty') and result_df.empty) or len(result_df) == 0:
                    print(f"❌ Error: Indicator calculation returned empty result for {symbol} {timeframe}")
                    return None
                
                # Save inverse data
                self._save_df_to_csv(result_df, symbol, timeframe)
                df_key = f"{symbol}_{timeframe}"
                self.latestDF[df_key] = result_df
                
                print(f"✅ Generated inverse data for {symbol} {timeframe} with {len(result_df)} records")
                return result_df
            else:
                print(f"❌ Failed to fetch base symbol data for {base_symbol}")
                return None
        else:
            # Regular symbol - use existing logic
            return self._fetch_base_symbol_data(symbol, timeframe)

    def reset_memory(self, symbol: Optional[str] = None, timeframe: Optional[str] = None):
        """
        Reset the in-memory DataFrame cache
        
        Args:
            symbol: Specific symbol to reset (if None, reset all)
            timeframe: Specific timeframe to reset (if None, reset all for symbol)
        """
        if not symbol:
            # Reset all
            self.latestDF.clear()
            print("🔄 Reset all in-memory DataFrames")
        elif not timeframe:
            # Reset all timeframes for symbol
            symbol = symbol
            keys_to_remove = [key for key in self.latestDF.keys() if key.startswith(f"{symbol}_")]
            for key in keys_to_remove:
                del self.latestDF[key]
            print(f"🔄 Reset all timeframes for {symbol}")
        else:
            # Reset specific symbol/timeframe
            symbol = symbol
            df_key = f"{symbol}_{timeframe}"
            if df_key in self.latestDF:
                del self.latestDF[df_key]
                print(f"🔄 Reset {symbol} {timeframe} from memory")

    def fetchLatestWithSignals(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Fetch the latest data for a symbol and timeframe with incremental updates and process trading signals
        
        Args:
            symbol: Stock symbol (e.g., 'SPY') - will also process 'SPY_inverse'
            timeframe: Timeframe (e.g., '1m', '5m', '10m', '15m', '30m')
            ema_period: EMA period for indicators
            vwma_period: VWMA period for indicators
            roc_period: ROC period for indicators
            fast_ema: Fast EMA for MACD
            slow_ema: Slow EMA for MACD
            signal_ema: Signal EMA for MACD
            
        Returns:
            DataFrame with latest data and indicators, or None if failed
        """
        # Check if signals have already been processed for these symbols
        original_signals_processed = self.signal_processor.has_processed_signals(symbol, timeframe)
        inverse_signals_processed = self.signal_processor.has_processed_signals(f"{symbol}_inverse", timeframe)
        
        # Fetch data for original symbol
        result_df = self.fetchLatest(symbol, timeframe)
        
        # Fetch data for inverse symbol
        inverse_symbol = f"{symbol}_inverse"
        result_df_inverse = self.fetchLatest(inverse_symbol, timeframe)
        
        # Only process signals if they haven't been processed before
        if result_df is not None:
            if not original_signals_processed:
                # Process trading signals for the original symbol
                self.process_signals(symbol, timeframe, result_df)
                print(f"✅ Processed signals for {symbol} {timeframe}")
            else:
                print(f"📊 Skipped signal processing for {symbol} {timeframe} (already processed)")
            
        if result_df_inverse is not None:
            if not inverse_signals_processed:
                # Process trading signals for the inverse symbol
                self.process_signals(inverse_symbol, timeframe, result_df_inverse)
                print(f"✅ Processed signals for {inverse_symbol} {timeframe}")
            else:
                print(f"📊 Skipped signal processing for {inverse_symbol} {timeframe} (already processed)")
            
        # Return the original symbol's data (for backward compatibility)
        return result_df
            
    def process_signals(self, symbol: str, timeframe: str, df: pd.DataFrame) -> List:
        """
        Process trading signals for a DataFrame
        
        Args:
            symbol: Stock symbol
            timeframe: Timeframe
            df: DataFrame with OHLCV data and indicators
            
        Returns:
            List of trades generated
        """
        return self.signal_processor.process_historical_signals(symbol, timeframe, df)
        
    def process_latest_signal(self, symbol: str, timeframe: str, row: pd.Series):
        """
        Process signal for latest incoming data (single row)
        
        Args:
            symbol: Stock symbol
            timeframe: Timeframe
            row: DataFrame row with OHLCV data and indicators
            
        Returns:
            Optional[Trade]: New trade if opened, None otherwise
        """
        # Process signal for original symbol
        original_trade = self.signal_processor.process_latest_signal(symbol, timeframe, row)
        
        # Generate inverse data from the original row
        inverse_row = self._generate_inverse_row(row)
        
        # Process signal for inverse symbol
        inverse_symbol = f"{symbol}_inverse"
        inverse_trade = self.signal_processor.process_latest_signal(inverse_symbol, timeframe, inverse_row)
        
        # Return the original trade for backward compatibility
        return original_trade
        
    def get_trade_summary(self) -> Dict[str, float]:
        """Get a summary of trading signals and performance"""
        return self.signal_processor.get_trade_summary()
        
    def get_open_trades(self) -> Dict[Tuple[str, str], Trade]:
        """Get all currently open trades"""
        return self.signal_processor.get_open_trades()
        
    def get_all_trades(self) -> List[Trade]:
        """Get all trades (open and closed)"""
        return self.signal_processor.get_all_trades()
        
    def email_trade_summary(self, subject: Optional[str] = None, include_open_trades: bool = True) -> bool:
        """
        Send a comprehensive trade summary email
        
        Args:
            subject: Custom email subject (optional)
            include_open_trades: Whether to include current open trades in the email
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        return self.signal_processor.email_trade_summary(subject if subject else "", include_open_trades)
        
    def email_daily_summary(self) -> bool:
        """
        Send a daily trade summary email
        
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        return self.signal_processor.email_daily_summary()
        
    def email_weekly_summary(self) -> bool:
        """
        Send a weekly trade summary email
        
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        return self.signal_processor.email_weekly_summary()
        
    def get_trade_summary_by_symbol_timeframe(self, symbol: str, timeframe: str) -> Dict[str, float]:
        """
        Get a summary of trading signals and performance for a specific symbol and timeframe
        
        Args:
            symbol: Trading symbol (e.g., 'SPY')
            timeframe: Timeframe (e.g., '5m')
            
        Returns:
            Dictionary with trade statistics for the specific symbol-timeframe
        """
        return self.signal_processor.get_trade_summary_by_symbol_timeframe(symbol, timeframe)
    
    def email_trade_summary_by_symbol_timeframe(self, symbol: str, timeframe: str, 
                                              subject: Optional[str] = None, include_open_trades: bool = True) -> bool:
        """
        Send a comprehensive trade summary email for a specific symbol and timeframe
        
        Args:
            symbol: Trading symbol (e.g., 'SPY')
            timeframe: Timeframe (e.g., '5m')
            subject: Custom email subject (optional)
            include_open_trades: Whether to include current open trades in the email
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        return self.signal_processor.email_trade_summary_by_symbol_timeframe(
            symbol, timeframe, subject if subject else "", include_open_trades
        )
        
    def reset_trades(self, symbol: Optional[str] = None, timeframe: Optional[str] = None):
        """
        Reset trades for specific symbol/timeframe or all trades
        
        Args:
            symbol: Specific symbol to reset (if None, reset all)
            timeframe: Specific timeframe to reset (if None, reset all for symbol)
        """
        if not symbol:
            # Reset all trades
            self.signal_processor = SignalProcessor()
            print("🔄 Reset all trades")
        elif not timeframe:
            # Reset all timeframes for symbol
            symbol = symbol
            # This would require more complex logic to reset specific symbol trades
            print(f"🔄 Reset trades for {symbol} (all timeframes)")
        else:
            # Reset specific symbol/timeframe
            symbol = symbol
            print(f"🔄 Reset trades for {symbol} {timeframe}")

    def _generate_inverse_row(self, row: pd.Series) -> pd.Series:
        """
        Generate inverse OHLC data from a single row of original symbol data.
        
        Args:
            row: Series with OHLCV data
            
        Returns:
            Series with inverse OHLC data (1/price) and same volume/timestamps
        """
        inverse_row = row.copy()
        
        # Generate inverse prices (1/price)
        inverse_row['open'] = 1.0 / row['open']
        inverse_row['high'] = 1.0 / row['high']
        inverse_row['low'] = 1.0 / row['low']
        inverse_row['close'] = 1.0 / row['close']
        
        # Note: For inverse data, we need to swap high/low since 1/high < 1/low
        # when original high > low
        temp_high = inverse_row['high']
        inverse_row['high'] = inverse_row['low']
        inverse_row['low'] = temp_high
        
        # Keep volume, timestamps, and indicators the same
        # (volume, timestamp, and indicator columns remain unchanged)
        
        return inverse_row

    def _generate_inverse_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate inverse OHLC data from original symbol data.
        
        Args:
            df: DataFrame with OHLCV data
            
        Returns:
            DataFrame with inverse OHLC data (1/price) and same volume/timestamps
        """
        if df is None or len(df) == 0:
            return df
            
        inverse_df = df.copy()
        
        # Generate inverse prices (1/price)
        inverse_df['open'] = 1.0 / df['open']
        inverse_df['high'] = 1.0 / df['high']
        inverse_df['low'] = 1.0 / df['low']
        inverse_df['close'] = 1.0 / df['close']
        
        # Note: For inverse data, we need to swap high/low since 1/high < 1/low
        # when original high > low
        temp_high = inverse_df['high'].copy()
        inverse_df['high'] = inverse_df['low']
        inverse_df['low'] = temp_high
        
        # Keep volume and timestamps the same
        # (volume and timestamp columns remain unchanged)
        
        return inverse_df

    # ======================== PARALLEL PROCESSING METHODS ========================
    
    def process_symbol_timeframe_parallel(self, symbol: str, timeframe: str) -> Tuple[str, str, bool]:
        """
        Process a single symbol-timeframe combination (for parallel execution)
        
        Returns:
            Tuple of (symbol, timeframe, success)
        """
        try:
            print(f"🔄 Processing {symbol} {timeframe}")
            result = self.fetchLatestWithSignals(
                symbol, timeframe
            )
            success = result is not None and len(result) > 0
            print(f"✅ Completed {symbol} {timeframe} - {'Success' if success else 'Failed'}")
            return symbol, timeframe, success
        except Exception as e:
            print(f"❌ Error processing {symbol} {timeframe}: {str(e)}")
            return symbol, timeframe, False
    
    def process_symbols_parallel(self, symbols: List[str], timeframes: List[str],
                               max_workers: Optional[int] = None) -> Dict[str, Dict[str, bool]]:
        """
        Process multiple symbols and timeframes in parallel using ThreadPoolExecutor
        
        Args:
            symbols: List of symbols to process
            timeframes: List of timeframes to process
            max_workers: Maximum number of parallel workers (default: CPU count)
            
        Returns:
            Dict with results: {symbol: {timeframe: success}}
        """
        if max_workers is None:
            max_workers = min(len(symbols) * len(timeframes), mp.cpu_count() * 2)
        
        print(f"🚀 Starting parallel processing with {max_workers} workers")
        print(f"📊 Processing {len(symbols)} symbols × {len(timeframes)} timeframes = {len(symbols) * len(timeframes)} combinations")
        
        # Create all symbol-timeframe combinations
        tasks = []
        for symbol in symbols:
            for timeframe in timeframes:
                tasks.append((symbol, timeframe))
        
        results = {}
        completed_count = 0
        total_tasks = len(tasks)
        
        # Use ThreadPoolExecutor for I/O bound operations (API calls)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(
                    self.process_symbol_timeframe_parallel,
                    symbol, timeframe
                ): (symbol, timeframe) 
                for symbol, timeframe in tasks
            }
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                symbol, timeframe = future_to_task[future]
                try:
                    result_symbol, result_timeframe, success = future.result()
                    
                    if result_symbol not in results:
                        results[result_symbol] = {}
                    results[result_symbol][result_timeframe] = success
                    
                    completed_count += 1
                    progress = (completed_count / total_tasks) * 100
                    print(f"📈 Progress: {completed_count}/{total_tasks} ({progress:.1f}%)")
                    
                except Exception as e:
                    print(f"❌ Task failed for {symbol} {timeframe}: {str(e)}")
                    if symbol not in results:
                        results[symbol] = {}
                    results[symbol][timeframe] = False
        
        return results
    
    def batch_process_signals(self, symbol: str, timeframe: str, df: pd.DataFrame, 
                            batch_size: int = 100) -> List:
        """
        Process signals in batches for better performance
        
        Args:
            symbol: Trading symbol
            timeframe: Time interval
            df: DataFrame with OHLCV data and indicators
            batch_size: Number of rows to process per batch
            
        Returns:
            List of all trades from batch processing
        """
        if df is None or len(df) == 0:
            return []
        
        all_trades = []
        total_rows = len(df)
        
        print(f"🔄 Batch processing {total_rows} rows in batches of {batch_size}")
        
        for i in range(0, total_rows, batch_size):
            end_idx = min(i + batch_size, total_rows)
            batch_df = df.iloc[i:end_idx]
            
            # Process each row in the batch
            for idx, row in batch_df.iterrows():
                trade = self.process_latest_signal(symbol, timeframe, row)
                if trade:
                    all_trades.append(trade)
            
            progress = (end_idx / total_rows) * 100
            print(f"📊 Batch progress: {end_idx}/{total_rows} ({progress:.1f}%)")
        
        return all_trades
    
    def optimize_memory_usage(self):
        """
        Optimize memory usage by cleaning up old data and forcing garbage collection
        """
        import gc

        for key, df in self.latestDF.items():
            if df is not None and len(df) > 10000:  # Keep only recent data
                # Keep only last 5000 rows
                self.latestDF[key] = df.tail(5000).copy()
                print(f"🧹 Trimmed {key} from {len(df)} to 5000 rows")
        
        # Force garbage collection
        gc.collect()
        print("🧹 Memory optimization completed")
    
    def get_processing_stats(self) -> Dict[str, int]:
        """
        Get statistics about current data processing state
        
        Returns:
            Dict with processing statistics
        """
        stats = {
            'total_dataframes': len(self.latestDF),
            'total_rows': sum(len(df) if df is not None else 0 for df in self.latestDF.values()),
            'memory_usage_mb': sum(df.memory_usage(deep=True).sum() if df is not None else 0 
                                 for df in self.latestDF.values()) / (1024 * 1024),
            'open_trades': len(self.get_open_trades()),
            'total_trades': len(self.get_all_trades())
        }
        return stats



    def bootstrap(self):
        print(f"📊 Loaded {len(self.symbols)} symbols: {', '.join(self.symbols)}")
        print(f"⏱️  Processing {len(self.timeframes)} timeframes: {', '.join(self.timeframes)}")
        
        # Option 1: Use parallel processing (recommended for multiple symbols/timeframes)
        if len(self.symbols) > 1 or len(self.timeframes) > 2:
            print("\n🚀 Using PARALLEL PROCESSING mode")
            
            # Process all symbols and timeframes in parallel
            results = self.process_symbols_parallel(
                symbols=self.symbols,
                timeframes=self.timeframes,
                max_workers=None
            )
            
            # Print results summary
            print("\n📊 PARALLEL PROCESSING RESULTS:")
            total_success = 0
            total_tasks = 0
            for symbol, timeframe_results in results.items():
                for timeframe, success in timeframe_results.items():
                    total_tasks += 1
                    if success:
                        total_success += 1
                    status = "✅" if success else "❌"
                    print(f"{status} {symbol} {timeframe}")
            
            success_rate = (total_success / total_tasks) * 100 if total_tasks > 0 else 0
            print(f"\n🎯 Success Rate: {total_success}/{total_tasks} ({success_rate:.1f}%)")
            
        else:
            print("\n🔄 Using SEQUENTIAL PROCESSING mode (single symbol/few timeframes)")
            
            # Sequential processing for single symbol or few timeframes
            for symbol in self.symbols:
                for timeframe in self.timeframes:
                    print(f"🔄 Processing {symbol} {timeframe}")
                    result = self.fetchLatestWithSignals(
                        symbol, timeframe
                    )
                    
                    if result is not None and len(result) > 0:
                        print(f"✅ Successfully processed {symbol} {timeframe}")
                    else:
                        print(f"❌ Failed to process {symbol} {timeframe}")
        # Email general summary
        print("\n📧 Sending general summary email...")
        email_success = self.email_trade_summary(
            subject=f"📊 Trade Summary for {', '.join(self.symbols)}",
            include_open_trades=True
        )
        if email_success:
            print("✅ Email sent successfully")
        else:
            print("❌ Email sending failed")
        # Memory optimization
        print("\n🧹 Optimizing memory usage...")
        self.optimize_memory_usage()
        
        # After bootstrap, drop closed trades and keep only open trades
        self.signal_processor.drop_closed_trades_and_save_only_open()
        # After 4pm, send trades.csv via email
        self.signal_processor.send_trades_csv_after_4pm()

        # Get processing statistics
        stats = self.get_processing_stats()
        print(f"\n📊 PROCESSING STATISTICS:")
        print(f"📈 Total DataFrames: {stats['total_dataframes']}")
        print(f"📊 Total Rows: {stats['total_rows']:,}")
        print(f"💾 Memory Usage: {stats['memory_usage_mb']:.1f} MB")
        print(f"📈 Open Trades: {stats['open_trades']}")
        print(f"📊 Total Trades: {stats['total_trades']}")
        
        print("🎉 Data Manager execution completed!")
    
    def get_comma_separated_items_from_file(self, filepath: str) -> List[str]:
        """Get comma-separated items from file (e.g., symbols or timeframes)"""
        try:
            with open(filepath, 'r') as file:
                content = file.read().strip()
                
                # Split by comma and clean up each item
                items = [item.strip() for item in content.split(',') if item.strip()]
                return items
        except FileNotFoundError:
            print(f"⚠️ File not found: {filepath}")
            return []
        except Exception as e:
            print(f"❌ Error reading file {filepath}: {e}")
            return []

    def get_symbols_from_file(self, symbols_filepath: str) -> List[str]:
        """Get symbols from file (alias for get_comma_separated_items_from_file)"""
        return self.get_comma_separated_items_from_file(symbols_filepath)

    def get_timeframes_from_file(self, timeframes_filepath: str) -> List[str]:
        """Get timeframes from file (alias for get_comma_separated_items_from_file)"""
        return self.get_comma_separated_items_from_file(timeframes_filepath)
