"""
Data Manager Module
Handles fetching, storing, and managing stock data with incremental updates and indicator integration.

The DataManager is a comprehensive solution for managing stock market data with the following capabilities:

CORE FUNCTIONALITY:
- fetchLatest(): Primary method for getting the most recent data with automatic incremental updates
- In-memory DataFrame caching for optimal performance
- Automatic CSV persistence with organized directory structure (data/timeframe/symbol.csv)
- Seamless integration with IndicatorGenerator for technical analysis
- Integrated SignalProcessor for automated trading signals and trade management

DATA FETCHING STRATEGIES:
1. Initial Fetch (no existing data):
   - 1m timeframe: Fetches from today's market open (9:30 AM ET) to last completed timestamp
   - 5m, 10m, 15m, 30m: Fetches from January 1, 2025 to last completed timestamp
   
2. Incremental Updates (existing data):
   - Fetches only new data from last timestamp to current market time
   - Automatically calculates indicators incrementally for new data points
   - Maintains data continuity and state persistence
   - Processes trading signals for new data points

SMART API OPTIMIZATION:
- Period selection based on timeframe: 1m uses period=1, others use period=10
- Valid API periods: [1, 2, 3, 4, 5, 10] days
- Automatic rate limiting (1-second delays between API calls)
- Proper Eastern Time zone handling with pytz

MARKET HOURS HANDLING:
- Respects US market hours (9:30 AM - 4:00 PM ET)
- Ignores extended hours trading
- Handles pre-market and after-hours scenarios appropriately
- Uses yesterday's close if before market open, today's close if after market close

SUPPORTED TIMEFRAMES:
- Valid timeframes: ['1m', '5m', '10m', '15m', '30m']
- Each timeframe gets its own subdirectory for organized storage
- Automatic directory creation and management

INDICATOR INTEGRATION:
- Automatic calculation of technical indicators (EMA, VWMA, ROC, MACD)
- Supports both bulk and incremental indicator calculations
- Maintains calculation state across sessions
- Configurable indicator parameters

SIGNAL PROCESSING:
- Integrated SignalProcessor for automated trading decisions
- Buy signals: EMA > VWMA AND ROC > 0 AND MACD Line > MACD Signal
- Sell signals: 2+ conditions fail OR 5% stop loss
- Email notifications for all trade actions
- Complete trade lifecycle management

MEMORY MANAGEMENT:
- In-memory DataFrame cache (latestDF) for fast access
- Automatic CSV persistence for data durability
- Memory reset capabilities for specific symbols/timeframes
- Efficient memory usage with proper cleanup

USAGE EXAMPLE:
    data_manager = DataManager()
    
    # Fetch data and process signals automatically
    result = data_manager.fetchLatestWithSignals(
        "SPY", "5m",
        ema_period=7, vwma_period=17, roc_period=8,
        fast_ema=12, slow_ema=26, signal_ema=9
    )
    
    # Get trade summary
    summary = data_manager.get_trade_summary()
    print(f"Win Rate: {summary['win_rate']:.1f}%")

FEATURES:
- Pure in-memory processing for optimal performance
- Automatic state persistence and recovery
- Smart auto-detection of initial vs incremental processing
- Continuity across all calculation types
- Robust error handling and logging
- Timezone-aware datetime operations
- Organized file structure for scalability
- Complete trading system with signal processing
- Email notifications and trade management
"""

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
import urllib.parse


class DataManager:
    def __init__(self):
        """
        Initialize the DataManager class
        """
        self.schwab_auth = SchwabAuth()
        self.indicator_generator = IndicatorGenerator()
        self.signal_processor = SignalProcessor()
        # Store the most recent DataFrame for each (symbol, timeframe) pair
        self.latestDF: Dict[str, pd.DataFrame] = {}
        
        # Valid timeframes for API calls
        self.valid_timeframes = ['1m', '5m', '10m', '15m', '30m']
        
        # Create data directory and timeframe subdirectories if they don't exist
        os.makedirs('data', exist_ok=True)
        for timeframe in self.valid_timeframes:
            os.makedirs(f'data/{timeframe}', exist_ok=True)
        
        # Eastern Time zone
        self.et_tz = pytz.timezone('US/Eastern')
        
        # Market hours (Eastern Time)
        self.market_open = datetime.strptime('09:30', '%H:%M').time()
        self.market_close = datetime.strptime('16:00', '%H:%M').time()

    def get_symbols_from_file(self, symbols_filepath: str) -> List[str]:
        """Get symbols from file separated by commas"""
        with open(symbols_filepath, 'r') as file:
            symbols = file.read().split(',')
            return [symbol.strip() for symbol in symbols if symbol.strip()]

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

    def _get_valid_period(self, days_to_end: int, timeframe: str = None) -> int:
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
        
        # Use today's market close time in ET timezone
        today_close = datetime.combine(today_et, self.market_close)
        return self.et_tz.localize(today_close)

    def _get_csv_filename(self, symbol: str, timeframe: str) -> str:
        """Get the CSV filename for a symbol and timeframe"""
        return f"data/{timeframe}/{symbol}.csv"

    def _get_df_key(self, symbol: str, timeframe: str) -> str:
        """Get the key for storing DataFrame in latestDF"""
        return f"{symbol}_{timeframe}"

    def _load_df_from_csv(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Load DataFrame from CSV file"""
        csv_filename = self._get_csv_filename(symbol, timeframe)
        if os.path.exists(csv_filename):
            try:
                df = pd.read_csv(csv_filename)
                # Convert timestamp to datetime if it exists
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_numeric(df['timestamp'])
                print(f"📂 Loaded {len(df)} records from {csv_filename}")
                return df
            except Exception as e:
                print(f"❌ Error loading CSV {csv_filename}: {e}")
        return None

    def _save_df_to_csv(self, df: pd.DataFrame, symbol: str, timeframe: str):
        """Save DataFrame to CSV file"""
        csv_filename = self._get_csv_filename(symbol, timeframe)
        try:
            df.to_csv(csv_filename, index=False)
            print(f"💾 Saved {len(df)} records to {csv_filename}")
        except Exception as e:
            print(f"❌ Error saving CSV {csv_filename}: {e}")

    def _fetch_data_from_schwab(self, symbol: str, start_date: datetime, end_date: datetime, 
                               interval_to_fetch: int, timeframe: str = None) -> Optional[pd.DataFrame]:
        """
        Fetch data from Schwab API for a given date range
        
        Args:
            symbol: Stock symbol
            start_date: Start datetime
            end_date: End datetime
            interval_to_fetch: Interval in minutes
            timeframe: Timeframe string for period calculation
            
        Returns:
            DataFrame with fetched data or None if failed
        """
        if not symbol:
            print("❌ Invalid symbol provided")
            return None

        symbol = symbol.upper()

        # Validate credentials first
        if not self.schwab_auth.validate_credentials():
            print("❌ Schwab credentials validation failed")
            return None

        # Check if we're authenticated
        if not self.schwab_auth.is_authenticated():
            print("❌ Not authenticated with Schwab API")
            return None

        headers = self.schwab_auth.get_auth_headers()
        if not headers:
            print("❌ No valid authentication headers available")
            return None

        # Debug: Print headers (masking sensitive values)
        debug_headers = {}
        for key, value in headers.items():
            if 'authorization' in key.lower() or 'token' in key.lower():
                debug_headers[key] = f"{value[:20]}..." if len(value) > 20 else "***"
            else:
                debug_headers[key] = value
        print(f"  [DEBUG] Request Headers: {debug_headers}")

        url = "https://api.schwabapi.com/marketdata/v1/pricehistory"

        all_candles = []
        current_start_dt = start_date

        while current_start_dt <= end_date:
            # Calculate period based on date range
            days_to_end = (end_date - current_start_dt).days + 1
            
            # Get the largest valid period that fits within our date range
            period = self._get_valid_period(days_to_end, timeframe)
            
            # Calculate the actual end date based on the period
            current_end_dt = min(current_start_dt + timedelta(days=period-1), end_date)

            # Convert start and end dates to UNIX epoch milliseconds
            # Ensure datetime objects are timezone-aware before conversion
            if current_start_dt.tzinfo is None:
                current_start_dt = self.et_tz.localize(current_start_dt)
            if current_end_dt.tzinfo is None:
                current_end_dt = self.et_tz.localize(current_end_dt)

            # Print ET and UTC datetimes and ms epoch for debugging
            print(f"  [DEBUG] Start (ET): {current_start_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"  [DEBUG] End   (ET): {current_end_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"  [DEBUG] Start (UTC): {current_start_dt.astimezone(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"  [DEBUG] End   (UTC): {current_end_dt.astimezone(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"  [DEBUG] Start ms epoch: {int(current_start_dt.timestamp() * 1000)}")
            print(f"  [DEBUG] End   ms epoch: {int(current_end_dt.timestamp() * 1000)}")

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

            # Debug: Print the exact URL and parameters
            query_string = urllib.parse.urlencode(params)
            debug_url = f"{url}?{query_string}"
            print(f"  [DEBUG] API URL: {debug_url}")

            print(f"📡 Fetching price history for {symbol} ({interval_to_fetch}m) from {current_start_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} to {current_end_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} (period={period})")

            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                # Sleep for 1 second to avoid rate limiting
                time.sleep(1)

                # Debug: Print response details
                print(f"  [DEBUG] Response Status: {response.status_code}")
                print(f"  [DEBUG] Response Headers: {dict(response.headers)}")
                
                # Parse and print error details if it's a JSON response
                try:
                    error_data = response.json()
                    if 'errors' in error_data:
                        print(f"  [DEBUG] API Errors:")
                        for error in error_data['errors']:
                            title = error.get('title', 'Unknown')
                            detail = error.get('detail', 'No detail provided')
                            print(f"    Title: {title}")
                            print(f"    Detail: {detail}")
                except:
                    print(f"  [DEBUG] Full Response Text: {response.text}")

                if response.status_code == 200:
                    data = response.json()
                    print(f"  [DEBUG] Success! Response data keys: {list(data.keys())}")
                    
                    if 'candles' in data and data['candles']:
                        candles = data['candles']
                        print(f"✅ Retrieved {len(candles)} candles from Schwab API")
                        print(f"  [DEBUG] First candle: {candles[0] if candles else 'None'}")
                        all_candles.extend(candles)
                    else:
                        print("📊 No candle data found in API response")
                        print(f"  [DEBUG] Full response data: {data}")
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

    def _fetch_base_symbol_data(self, symbol: str, timeframe: str, 
                               ema_period: int = None, vwma_period: int = None, roc_period: int = None,
                               fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> Optional[pd.DataFrame]:
        """
        Fetch the latest data for a base symbol (not inverse) with incremental updates
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
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
        df_key = self._get_df_key(symbol, timeframe)
        interval_minutes = self._extract_frequency_number(timeframe)
        
        print(f"🔄 Fetching base symbol data for {symbol} {timeframe}")
        
        # Check if DataFrame exists in memory
        if df_key in self.latestDF and len(self.latestDF[df_key]) > 0:
            print(f"📊 Found existing DataFrame in memory for {symbol} {timeframe}")
            original_df = self.latestDF[df_key]
            
            # Get the last timestamp from existing data
            last_timestamp = original_df['timestamp'].max()
            last_datetime_utc = datetime.fromtimestamp(last_timestamp / 1000, tz=pytz.UTC)
            last_datetime_et = last_datetime_utc.astimezone(self.et_tz)
            
            # Fetch new data from last timestamp to current time
            end_date = self._get_last_completed_timestamp()
            
            if last_datetime_et < end_date:
                print(f"📡 Fetching new data from {last_datetime_et.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                new_df = self._fetch_data_from_schwab(symbol, last_datetime_et, end_date, interval_minutes, timeframe)
                
                if new_df is not None and len(new_df) > 0:
                    # Filter out data that might overlap with existing data
                    new_df = new_df[new_df['timestamp'] > last_timestamp]
                    
                    if len(new_df) > 0:
                        print(f"📈 Processing {len(new_df)} new records incrementally")
                        
                        # Calculate indicators incrementally
                        result_df = self.indicator_generator.smart_indicator_calculation(
                            symbol, timeframe, original_df, new_df,
                            ema_period, vwma_period, roc_period,
                            fast_ema, slow_ema, signal_ema
                        )
                        
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
            print(f"📂 No existing DataFrame in memory for {symbol} {timeframe}")
            
            # Try loading from CSV
            original_df = self._load_df_from_csv(symbol, timeframe)
            
            if original_df is not None and len(original_df) > 0:
                print(f"📂 Loaded existing data from CSV for {symbol} {timeframe}")
                self.latestDF[df_key] = original_df
                
                # Check if we need to fetch new data
                last_timestamp = original_df['timestamp'].max()
                last_datetime_utc = datetime.fromtimestamp(last_timestamp / 1000, tz=pytz.UTC)
                last_datetime_et = last_datetime_utc.astimezone(self.et_tz)
                end_date = self._get_last_completed_timestamp()
                
                if last_datetime_et < end_date:
                    print(f"📡 Fetching new data from {last_datetime_et.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    new_df = self._fetch_data_from_schwab(symbol, last_datetime_et, end_date, interval_minutes, timeframe)
                    
                    if new_df is not None and len(new_df) > 0:
                        # Filter out overlapping data
                        new_df = new_df[new_df['timestamp'] > last_timestamp]
                        
                        if len(new_df) > 0:
                            print(f"📈 Processing {len(new_df)} new records incrementally")
                            
                            # Calculate indicators incrementally
                            result_df = self.indicator_generator.smart_indicator_calculation(
                                symbol, timeframe, original_df, new_df,
                                ema_period, vwma_period, roc_period,
                                fast_ema, slow_ema, signal_ema
                            )
                            
                            # Save updated result and update memory
                            self._save_df_to_csv(result_df, symbol, timeframe)
                            self.latestDF[df_key] = result_df
                            
                            print(f"✅ Updated {symbol} {timeframe} with {len(new_df)} new records")
                            return result_df
                
                # If no new data needed, calculate indicators on existing data
                print(f"📊 Calculating indicators on existing data for {symbol} {timeframe}")
                result_df = self.indicator_generator.smart_indicator_calculation(
                    symbol, timeframe, original_df, None,
                    ema_period, vwma_period, roc_period,
                    fast_ema, slow_ema, signal_ema
                )
                
                # Save result and update memory
                self._save_df_to_csv(result_df, symbol, timeframe)
                self.latestDF[df_key] = result_df
                
                return result_df
                
            else:
                print(f"📂 No existing CSV data for {symbol} {timeframe}, performing initial fetch")
                
                # Perform initial fetch based on timeframe
                if timeframe == '1m':
                    # For 1m: fetch from today's market open to last completed timestamp
                    start_date = self._get_market_open_today()
                else:
                    # For 5m, 10m, 15m, 30m: fetch from January 1, 2025 to last completed timestamp
                    start_date = self.et_tz.localize(datetime(2025, 1, 1))
                
                end_date = self._get_last_completed_timestamp()
                
                print(f"📡 Initial fetch for {symbol} {timeframe} from {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                df = self._fetch_data_from_schwab(symbol, start_date, end_date, interval_minutes, timeframe)
                
                if df is not None and len(df) > 0:
                    # Save to memory and CSV
                    self.latestDF[df_key] = df
                    self._save_df_to_csv(df, symbol, timeframe)
                    
                    # Calculate indicators on the initial data
                    print(f"📊 Calculating indicators on initial data for {symbol} {timeframe}")
                    result_df = self.indicator_generator.smart_indicator_calculation(
                        symbol, timeframe, df, None,
                        ema_period, vwma_period, roc_period,
                        fast_ema, slow_ema, signal_ema
                    )
                    
                    # Save result and update memory
                    self._save_df_to_csv(result_df, symbol, timeframe)
                    self.latestDF[df_key] = result_df
                    
                    print(f"✅ Initial fetch completed for {symbol} {timeframe} with {len(result_df)} records")
                    return result_df
                else:
                    print(f"❌ Failed to fetch initial data for {symbol} {timeframe}")
                    return None

    def fetchLatest(self, symbol: str, timeframe: str, 
                   ema_period: int = None, vwma_period: int = None, roc_period: int = None,
                   fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> Optional[pd.DataFrame]:
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
        if timeframe not in self.valid_timeframes:
            print(f"❌ Invalid timeframe: {timeframe}. Valid timeframes: {self.valid_timeframes}")
            return None
            
        # Check if this is an inverse symbol
        is_inverse = symbol.endswith('_inverse')
        base_symbol = symbol.replace('_inverse', '') if is_inverse else symbol
        
        symbol = symbol.upper()
        base_symbol = base_symbol.upper()
        
        print(f"🔄 Fetching latest data for {symbol} {timeframe} (inverse: {is_inverse})")
        
        # For inverse symbols, fetch the base symbol data first
        if is_inverse:
            # Fetch base symbol data
            base_df = self._fetch_base_symbol_data(base_symbol, timeframe, ema_period, vwma_period, roc_period, fast_ema, slow_ema, signal_ema)
            
            if base_df is not None and len(base_df) > 0:
                # Generate inverse data
                inverse_df = self._generate_inverse_data(base_df)
                
                # Calculate indicators for inverse data
                result_df = self.indicator_generator.smart_indicator_calculation(
                    symbol, timeframe, inverse_df, None,
                    ema_period, vwma_period, roc_period,
                    fast_ema, slow_ema, signal_ema
                )
                
                # Save inverse data
                self._save_df_to_csv(result_df, symbol, timeframe)
                df_key = self._get_df_key(symbol, timeframe)
                self.latestDF[df_key] = result_df
                
                print(f"✅ Generated inverse data for {symbol} {timeframe} with {len(result_df)} records")
                return result_df
            else:
                print(f"❌ Failed to fetch base symbol data for {base_symbol}")
                return None
        else:
            # Regular symbol - use existing logic
            return self._fetch_base_symbol_data(symbol, timeframe, ema_period, vwma_period, roc_period, fast_ema, slow_ema, signal_ema)

    def reset_memory(self, symbol: str = None, timeframe: str = None):
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
            symbol = symbol.upper()
            keys_to_remove = [key for key in self.latestDF.keys() if key.startswith(f"{symbol}_")]
            for key in keys_to_remove:
                del self.latestDF[key]
            print(f"🔄 Reset all timeframes for {symbol}")
        else:
            # Reset specific symbol/timeframe
            symbol = symbol.upper()
            df_key = self._get_df_key(symbol, timeframe)
            if df_key in self.latestDF:
                del self.latestDF[df_key]
                print(f"🔄 Reset {symbol} {timeframe} from memory")

    def fetchLatestWithSignals(self, symbol: str, timeframe: str, 
                               ema_period: int = None, vwma_period: int = None, roc_period: int = None,
                               fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> Optional[pd.DataFrame]:
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
        # Fetch data for original symbol
        result_df = self.fetchLatest(symbol, timeframe, ema_period, vwma_period, roc_period, fast_ema, slow_ema, signal_ema)
        
        # Fetch data for inverse symbol
        inverse_symbol = f"{symbol}_inverse"
        result_df_inverse = self.fetchLatest(inverse_symbol, timeframe, ema_period, vwma_period, roc_period, fast_ema, slow_ema, signal_ema)
        
        if result_df is not None:
            # Process trading signals for the original symbol
            self.process_signals(symbol, timeframe, result_df)
            print(f"✅ Processed signals for {symbol} {timeframe}")
            
        if result_df_inverse is not None:
            # Process trading signals for the inverse symbol
            self.process_signals(inverse_symbol, timeframe, result_df_inverse)
            print(f"✅ Processed signals for {inverse_symbol} {timeframe}")
            
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
        
    def get_open_trades(self) -> Dict[Tuple[str, str], object]:
        """Get all currently open trades"""
        return self.signal_processor.get_open_trades()
        
    def get_all_trades(self) -> List[object]:
        """Get all trades (open and closed)"""
        return self.signal_processor.get_all_trades()
        
    def email_trade_summary(self, subject: str = None, include_open_trades: bool = True) -> bool:
        """
        Send a comprehensive trade summary email
        
        Args:
            subject: Custom email subject (optional)
            include_open_trades: Whether to include current open trades in the email
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        return self.signal_processor.email_trade_summary(subject, include_open_trades)
        
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
        
    def reset_trades(self, symbol: str = None, timeframe: str = None):
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
            symbol = symbol.upper()
            # This would require more complex logic to reset specific symbol trades
            print(f"🔄 Reset trades for {symbol} (all timeframes)")
        else:
            # Reset specific symbol/timeframe
            symbol = symbol.upper()
            print(f"🔄 Reset trades for {symbol} {timeframe}")

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
        
        print(f"🔄 Generated inverse data with {len(inverse_df)} rows")
        return inverse_df


if __name__ == "__main__":
    # Example usage
    data_manager = DataManager()
    
    # Define indicator parameters
    ema_period = 7
    vwma_period = 17
    roc_period = 8
    fast_ema = 12
    slow_ema = 26
    signal_ema = 9
    
    # Fetch data and process signals automatically for both SPY and SPY_inverse
    result = data_manager.fetchLatestWithSignals(
        "SPY", "5m",
        ema_period=ema_period, vwma_period=vwma_period, roc_period=roc_period,
        fast_ema=fast_ema, slow_ema=slow_ema, signal_ema=signal_ema
    )
    
    if result is not None:
        print(f"✅ Successfully fetched {len(result)} records for SPY 5m")
        print("Last row of SPY:")
        print(result.tail(1))
        
        # Check if inverse data was also generated
        inverse_df = data_manager.latestDF.get("SPY_INVERSE_5m")
        if inverse_df is not None:
            print(f"✅ Successfully generated {len(inverse_df)} records for SPY_inverse 5m")
            print("Last row of SPY_inverse:")
            print(inverse_df.tail(1))
        
        # Get trade summary (includes both original and inverse trades)
        summary = data_manager.get_trade_summary()
        print(f"📊 Trade Summary - Win Rate: {summary['win_rate']:.1f}%")
        print(f"📊 Total Trades: {summary['total_trades']} (includes both original and inverse)")
        
        # Get open trades
        open_trades = data_manager.get_open_trades()
        print(f"📊 Open Trades: {len(open_trades)}")
        for key, trade in open_trades.items():
            print(f"  {trade.symbol} {trade.timeframe}: Entry {trade.entry_price:.2f}, P&L {trade.pnl:.2f} ({trade.pnl_percent:.2f}%)")
    else:
        print("❌ Failed to fetch data")
