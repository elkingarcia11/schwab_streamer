import os
import time
import pandas as pd
import requests
import pytz
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from schwab_auth import SchwabAuth
from indicator_generator import IndicatorGenerator
from signal_processor import SignalProcessor
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
import numpy as np


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
                inverse_df = self.initialize_dataframe(f"{symbol}_inverse", timeframe)
                self.indicator_generator.initialize_indicators_state(symbol, timeframe, df)
                self.indicator_generator.initialize_indicators_state(f"{symbol}_inverse", timeframe, inverse_df)
                self.latestDF[f"{symbol}_{timeframe}"] = df
                self.latestDF[f"{symbol}_inverse_{timeframe}"] = inverse_df

    def initialize_dataframe(self, symbol: str, timeframe: str):
        """ Initialize DataFrame for a given symbol and timeframe """
        # Check if DataFrame already exists in memory
        df = self._load_df_from_csv(symbol, timeframe)
        if df is None:
            print(f"🔄 Initializing new DataFrame for {symbol} {timeframe}")
            df = pd.DataFrame(columns=pd.Index(['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'ema', 'vwma', 'roc', 'macd_line', 'macd_signal']))
            # Save empty DataFrame to CSV to create the file
            self.save_df_to_csv(df, symbol, timeframe)
        else:
            print(f"🔄 Loading existing DataFrame for {symbol} {timeframe}")
        self.latestDF[f"{symbol}_{timeframe}"] = df
        return df 

    def _load_df_from_csv(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Load DataFrame from CSV file"""
        csv_filename = self._get_csv_filename(symbol, timeframe)
        try:
            df = pd.read_csv(csv_filename)
            
            # Ensure indicator columns exist (for backward compatibility with old CSV files)
            required_columns = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'ema', 'vwma', 'roc', 'macd_line', 'macd_signal']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"📊 Adding missing indicator columns to {symbol} {timeframe}: {missing_columns}")
                for col in missing_columns:
                    df[col] = np.nan
            
            return df
        except FileNotFoundError:
            print(f"❌ Error loading file {csv_filename}: File not found")
            return None
        except Exception as e:
            print(f"❌ Error loading CSV file {csv_filename}: {e}")
            return None

    ########################################################
    # Backfill functions
    ########################################################

    def bootstrap(self):
        """
        Back fill any missing historic data, then generate indicators, then process trades for all symbols and timeframes in parallel
        """   
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
        
        # Send email with open trades data after all signals are processed
        print("\n📧 Sending open trades email after bootstrap completion...")
        try:
            success = self.signal_processor.email_trade_summary(
                subject="📊 Open Trades Report - Bootstrap Complete",
                include_open_trades=True
            )
            if success:
                print("✅ Open trades email sent successfully")
            else:
                print("❌ Failed to send open trades email")
        except Exception as e:
            print(f"❌ Error sending open trades email: {e}")
        
        print("✅ Bootstrap process completed")

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
    
    def process_symbol_timeframe_parallel(self, symbol: str, timeframe: str) -> Tuple[str, str, bool]:
        """
        Process a single symbol-timeframe combination (for parallel execution)
        
        Returns:
            Tuple of (symbol, timeframe, success)
        """
        try:
            print(f"🔄 Processing {symbol} {timeframe}")
            result = self.process_symbol_timeframe(
                symbol, timeframe
            )
            print(f"✅ Completed {symbol} {timeframe} - {'Success' if result else 'Failed'}")
            return symbol, timeframe, result
        except Exception as e:
            print(f"❌ Error processing {symbol} {timeframe}: {str(e)}")
            return symbol, timeframe, False

    def process_symbol_timeframe(self, symbol: str, timeframe: str) -> bool:
        """
        Fetch the latest data for a symbol and timeframe with incremental updates, then generate indicators, then process trades on data fetched
        
        Args:
            symbol: Stock symbol (e.g., 'SPY' or 'SPY_inverse')
            timeframe: Timeframe (e.g., '1m', '5m', '10m', '15m', '30m')
            
        Returns:
            DataFrame with latest data and indicators, or None if failed
        """

        new_df = self._fetch_base_symbol_data(symbol, timeframe)
        
        # Only generate inverse data if we have new data
        if new_df is None:
            return False

        # Fetch original data 
        original_df = self._get_dataframe(symbol, timeframe)

        # Calculate indicators for original data
        result_df, index_of_first_new_row = self.indicator_generator.calculate_real_time_indicators(
            symbol, timeframe, original_df, new_df
        )

        # Generate inverse data
        new_inverse_df = self._generate_inverse_data(new_df)

        # Calculate indicators for inverse data
        result_inverse_df, index_of_first_new_row_inverse = self.indicator_generator.calculate_real_time_indicators(
            f"{symbol}_inverse", timeframe, original_df,new_inverse_df
        )

        # Process signals (returns List of trades, not DataFrame)
        self.process_signals(symbol, timeframe, result_df, index_of_first_new_row)
        print(f"✅ Processed signals for {symbol} {timeframe}")

        # Process trading signals for the inverse symbol
        self.process_signals(f"{symbol}_inverse", timeframe, result_inverse_df, index_of_first_new_row_inverse)
        print(f"✅ Processed signals for {symbol}_inverse {timeframe}")
            
        # Save data
        self.save_df_to_csv(result_df, symbol, timeframe)
        self.save_df_to_csv(result_inverse_df, f"{symbol}_inverse", timeframe)
        self.latestDF[f"{symbol}_{timeframe}"] = result_df
        self.latestDF[f"{symbol}_inverse_{timeframe}"] = result_inverse_df
                    
        print(f"✅ Generated data for {symbol} {timeframe} and {symbol}_inverse {timeframe} with {len(result_df)} records")
        return True

    def _fetch_base_symbol_data(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Fetch the latest data for a base symbol (not inverse) with incremental updates
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            timeframe: Timeframe (e.g., '1m', '5m', '10m', '15m', '30m')
            
        Returns:
            Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]: Tuple containing original data and new data for the symbol and inverse symbol
        """
        df_key = f"{symbol}_{timeframe}"
        interval_minutes = self._extract_frequency_number(timeframe)
        
        print(f"🔄 Fetching base symbol data for {symbol} {timeframe}")
        
        # Check if DataFrame exists in memory
        if df_key in self.latestDF and len(self.latestDF[df_key]) > 0:
            original_df = self.latestDF[df_key]
            # Get the last timestamp from existing data
            last_timestamp = original_df['timestamp'].max()
            # Convert last timestamp to datetime in UTC and then to ET
            last_datetime_utc = datetime.fromtimestamp(last_timestamp / 1000, tz=pytz.UTC)
            last_datetime_et = last_datetime_utc.astimezone(self.et_tz)
            
            # For 1m data, ensure we don't fetch before today's market open
            if timeframe == '1m':
                market_open_today = self._get_market_open_today()
                if last_datetime_et < market_open_today:
                    last_datetime_et = market_open_today
                    print(f"📊 Adjusting start time to today's market open: {last_datetime_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                
            # Fetch new data from last timestamp to current time
            end_date = self._get_last_completed_timestamp()
            
            # Ensure we're not trying to fetch outside market hours
            if last_datetime_et < end_date:
                # For non-1m timeframes, check if the gap crosses market hours boundaries
                if timeframe != '1m':
                    # If the gap crosses overnight, we need to handle it properly
                    last_date = last_datetime_et.date()
                    end_date_obj = end_date.date()
                    
                    if last_date < end_date_obj:
                        # Gap crosses overnight - we should only fetch up to yesterday's market close
                        # and then separately fetch from today's market open
                        yesterday_close = datetime.combine(last_date, self.market_close)
                        yesterday_close = self.et_tz.localize(yesterday_close)
                        
                        if last_datetime_et < yesterday_close:
                            print(f"📡 Fetching data from {last_datetime_et.strftime('%Y-%m-%d %H:%M:%S %Z')} to {yesterday_close.strftime('%Y-%m-%d %H:%M:%S %Z')} (yesterday's close)")
                            new_df = self._fetch_data_from_schwab(symbol, last_datetime_et, yesterday_close, interval_minutes)
                            
                            if new_df is not None and len(new_df) > 0:
                                # Filter out data that might overlap with existing data
                                new_df = new_df[new_df['timestamp'] > last_timestamp]
                                
                                if len(new_df) > 0:
                                    print(f"📈 Processing {len(new_df)} new records from yesterday")
                                    if isinstance(new_df, pd.Series):
                                        new_df = new_df.to_frame().T
                                    return new_df
                        
                        # Now check if we should fetch today's data
                        today_open = self._get_market_open_today()
                        if end_date > today_open:
                            print(f"📡 Fetching data from {today_open.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')} (today's market hours)")
                            today_df = self._fetch_data_from_schwab(symbol, today_open, end_date, interval_minutes)
                            
                            if today_df is not None and len(today_df) > 0:
                                print(f"📈 Processing {len(today_df)} new records from today")
                                if isinstance(today_df, pd.Series):
                                    today_df = today_df.to_frame().T
                                return today_df
                        
                        print(f"📊 No new data available for {symbol} {timeframe}")
                        return None
                    else:
                        # Same day fetch
                        print(f"📡 Fetching new data from {last_datetime_et.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        new_df = self._fetch_data_from_schwab(symbol, last_datetime_et, end_date, interval_minutes)
                else:
                    # For 1m data, just fetch the gap
                    print(f"📡 Fetching new data from {last_datetime_et.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    new_df = self._fetch_data_from_schwab(symbol, last_datetime_et, end_date, interval_minutes)
                
                if new_df is not None and len(new_df) > 0:
                    # Filter out data that might overlap with existing data
                    new_df = new_df[new_df['timestamp'] > last_timestamp]
                    
                    if len(new_df) > 0:
                        print(f"📈 Processing {len(new_df)} new records incrementally")
                        # Ensure new_df is a DataFrame, not a Series
                        if isinstance(new_df, pd.Series):
                            new_df = new_df.to_frame().T
                        return new_df
                    else:
                        print(f"📊 No new data available for {symbol} {timeframe}")
                        return None
                else:
                    print(f"📊 No new data fetched for {symbol} {timeframe}")
                    return None
            else:
                print(f"📊 Data is already up to date for {symbol} {timeframe}")
                return None
        else:
            # Perform initial historical fetch
            # No existing data, use default start dates based on timeframe
            end_date = self._get_last_completed_timestamp()
            
            if timeframe == '1m':
                # For 1m: check if we're outside market hours
                now_et = datetime.now(self.et_tz)
                current_time = now_et.time()
                
                # IF before market hours return, if after market hours return today's market open and market close
                if current_time < self.market_open or current_time > self.market_close:
                    print(f"📊 Before market hours, returning None for {symbol} {timeframe}")
                    return None
                else:
                    # During market hours, use today's market open
                    start_date = self._get_market_open_today()
                    end_date = self._get_last_completed_timestamp()
            else:
                # For 5m, 10m, 15m, 30m: fetch from January 1, 2025 to last completed timestamp
                start_date = self.et_tz.localize(datetime(2025, 1, 1))
                print(f"📊 No existing data found, using default start date: {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                end_date = self._get_last_completed_timestamp()
            
            print(f"📡 Initial fetch for {symbol} {timeframe} from {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            df = self._fetch_data_from_schwab(symbol, start_date, end_date, interval_minutes)
            
            return df
    
    def _get_dataframe(self, symbol: str, timeframe: str):
        """Fetch the latest DataFrame for a given symbol and timeframe"""
        df = self.latestDF[f"{symbol}_{timeframe}"]
        return df
    
    def _generate_inverse_data(self, new_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate inverse OHLC data from original symbol data.
        
        Args:
            df: DataFrame with OHLCV data
            
        Returns:
            DataFrame with inverse OHLC data (1/price) and same volume/timestamps
        """
        new_inverse_df = new_df.copy()
        
        # Generate inverse prices (1/price)
        new_inverse_df['open'] = 1.0 / new_df['open']
        new_inverse_df['high'] = 1.0 / new_df['low']
        new_inverse_df['low'] = 1.0 / new_df['high']
        new_inverse_df['close'] = 1.0 / new_df['close']

        return new_inverse_df


    def process_signals(self, symbol: str, timeframe: str, df: pd.DataFrame, index_of_first_new_row: int) -> List:
        """
        Process trading signals for a DataFrame
        
        Args:
            symbol: Stock symbol
            timeframe: Timeframe
            df: DataFrame with OHLCV data and indicators
            
        Returns:
            List of trades generated
        """
        print(f"🔍 [DEBUG] process_signals called for {symbol} {timeframe}")
        print(f"🔍 [DEBUG] DataFrame shape: {df.shape}, index_of_first_new_row: {index_of_first_new_row}")
        
        if len(df) > 0:
            print(f"🔍 [DEBUG] Last row data: {df.iloc[-1].to_dict()}")
        
        result = self.signal_processor.process_historical_signals(symbol, timeframe, df, index_of_first_new_row)
        print(f"🔍 [DEBUG] process_historical_signals returned: {len(result) if result else 0} trades")
        return result
        
    def save_df_to_csv(self, df: pd.DataFrame, symbol: str, timeframe: str):
        """Save DataFrame to CSV file"""
        csv_filename = self._get_csv_filename(symbol, timeframe)
        try:
            df.to_csv(csv_filename, index=False)
            print(f"💾 Saved {len(df)} records to {csv_filename}")
        except Exception as e:
            print(f"❌ Error saving CSV {csv_filename}: {e}")

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
                    
                    # Filter out data outside market hours (9:30 AM - 4:00 PM ET)
                    candle_time = dt_et.time()
                    if candle_time < self.market_open or candle_time > self.market_close:
                        continue  # Skip this candle
                    
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
                
                print(f"✅ Processed {len(df)} records from API (filtered to market hours only)")
                return df

            except Exception as e:
                print(f"❌ Error processing data: {e}")
                return None
        else:
            print(f"⚠️  No data retrieved for {symbol}_{interval_to_fetch}m")
            return None

    def _get_market_open_today(self) -> datetime:
        """Get today's market open time (9:30 AM ET)"""
        today_et = datetime.now(self.et_tz).date()
        market_open_dt = datetime.combine(today_et, self.market_open)
        return self.et_tz.localize(market_open_dt)

    def _get_market_close_today(self) -> datetime:
        """Get today's market close time (4:00 PM ET)"""
        today_et = datetime.now(self.et_tz).date()
        market_close_dt = datetime.combine(today_et, self.market_close)
        return self.et_tz.localize(market_close_dt)

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
    
    def _get_csv_filename(self, symbol: str, timeframe: str) -> str:
        """Get the CSV filename for a symbol and timeframe"""
        return f"data/{timeframe}/{symbol}.csv"


    ########################################################
    # Streaming functions
    ########################################################

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
                    self.save_df_to_csv(self.latestDF[df_key], symbol, timeframe)
            except Exception as fallback_error:
                print(f"❌ Fallback save also failed: {fallback_error}")

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

def main():
    """
    Main function to run the DataManager
    """
    schwab_auth = SchwabAuth()
    symbols = ['SPY']
    timeframes = ['1m','5m']
    data_manager = DataManager(schwab_auth, symbols, timeframes)
    data_manager.bootstrap()

if __name__ == "__main__":
    main()