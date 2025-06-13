import requests
import pandas as pd
import pandas_ta as ta
from typing import Optional, Dict, Any, List
from schwab_auth import SchwabAuth  # Import the SchwabAuth class
from datetime import datetime
import pytz
import os
import time
import logging
import numpy as np
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('asset_manager.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AssetManager:
    """
    Fetches historical price data for a given symbol using the PriceHistory API.
    """

    # Configuration
    MARKET_OPEN_HOUR = 9
    MARKET_OPEN_MINUTE = 30
    MARKET_CLOSE_HOUR = 16
    MARKET_CLOSE_MINUTE = 0
    TIMEZONE = 'US/Eastern'
    
    # Technical Indicator Parameters
    ROC_PERIOD = 8
    EMA_PERIOD = 7
    VWMA_PERIOD = 17
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9

    def __init__(
        self,
        auth: SchwabAuth,
        symbols: List[str],
        timeframes: List[str],
        start_date: int,
        end_date: int
    ):
        """
        Initialize the AssetManager.

        Args:
            auth (SchwabAuth): Authenticated Schwab API client.
            symbols (List[str]): List of symbols to track.
            timeframes (List[str]): List of timeframes to track.
            start_date (int): Start date in milliseconds.
            end_date (int): End date in milliseconds.
        """
        self.auth = auth
        self.symbols = symbols
        self.timeframes = timeframes
        self.start_date = start_date
        self.end_date = end_date
        self.trades = {}
        
        # Initialize trade tracking and fetch data for each symbol
        for symbol in symbols:
            self.fetch(symbol)
            for timeframe in timeframes:
                self.trades[symbol + "_" + timeframe] = None
                self.trades[symbol + "_inverse_" + timeframe] = None
                self.aggregate_timeframe(symbol, "1m", timeframe)
                self.generate_inverse_ohlc(symbol, timeframe)
                self.calculate_indicators(symbol, timeframe)
                self.process_signals(symbol, timeframe)
    
    def process_signals(self, symbol: str, timeframe: str) -> None:
        """
        Open csv, iterate through each row, check if buy or sell conditions are met, if so, record the action 
        Args:
            symbol (str): The symbol to open a trade for.
            timeframe (str): The timeframe to check indicators on.
        """
        try:
            # Read the latest data to check indicators
            data_path = f"data/{timeframe}/{symbol}.csv"
            if not os.path.exists(data_path):
                logger.error(f"Data file {data_path} does not exist")
                return

            inverse_data_path = f"data/{timeframe}/{symbol}_inverse.csv"
            if not os.path.exists(inverse_data_path):
                logger.error(f"Inverse data file {inverse_data_path} does not exist")
                return

            # Read the data
            df = pd.read_csv(data_path)
            inverse_df = pd.read_csv(inverse_data_path)

            # Process regular trades
            for index, row in df.iterrows():
                trade_key = f"{symbol}_{timeframe}"
                has_open_trade = self.trades[trade_key] is not None

                # Check technical conditions
                conditions_met = (
                    row['roc8'] > 0 and
                    row['ema7'] > row['vwma17'] and
                    row['macd_line'] > row['macd_signal']
                )

                if has_open_trade and not conditions_met:
                    # Close Trade if conditions are no longer met
                    self.close_trade(symbol, timeframe, row['close'])
                elif not has_open_trade and conditions_met:
                    # Open Trade if conditions are met and no trade is open
                    self.open_trade(symbol, timeframe, row['close'], row['timestamp_ms'])

            # Process inverse trades
            for index, row in inverse_df.iterrows():
                trade_key = f"{symbol}_inverse_{timeframe}"
                has_open_trade = self.trades[trade_key] is not None

                # Use the same technical conditions for inverse trades (not inverted)
                conditions_met = (
                    row['roc8'] > 0 and
                    row['ema7'] > row['vwma17'] and
                    row['macd_line'] > row['macd_signal']
                )

                if has_open_trade and not conditions_met:
                    self.close_trade(symbol, timeframe, row['close'], row['timestamp_ms'], is_inverse=True)
                elif not has_open_trade and conditions_met:
                    self.open_trade(symbol, timeframe, row['close'], row['timestamp_ms'], is_inverse=True)
        except Exception as e:
            logger.error(f"Error processing signals for {symbol} on {timeframe}: {str(e)}")

    def open_trade(self, symbol: str, timeframe: str, entry_price: float, entry_date: int, is_inverse: bool = False) -> None:
        """
        Record open trade in trades dictionary and data/trades/open_trades.csv
        """
        try:
            trade_key = f"{symbol}_{'inverse_' if is_inverse else ''}{timeframe}"
            if trade_key not in self.trades:
                logger.error(f"Symbol {symbol} with timeframe {timeframe} is not being tracked")
                return

            if self.trades[trade_key] is not None:
                logger.error(f"Symbol {symbol} already has an open trade")
                return  
            self.trades[trade_key] = {
                "entry_date": entry_date,
                "entry_price": entry_price,
                "status": "open"
            }
            logger.info(f"Opened {'inverse ' if is_inverse else ''}trade for {symbol} on {timeframe} at {entry_price}")

            # Write to data/trades/open_trades.csv with header
            os.makedirs('data/trades', exist_ok=True)
            file_path = 'data/trades/open_trades.csv'
            file_exists = os.path.isfile(file_path)
            with open(file_path, 'a', newline='') as f:
                writer = csv.writer(f)
                if not file_exists or os.stat(file_path).st_size == 0:
                    writer.writerow(["symbol", "timeframe", "entry_date", "entry_price", "status"])
                # Convert entry_date (ms) to ET string for CSV
                entry_dt = pd.to_datetime(entry_date, unit='ms', utc=True).dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S') if isinstance(entry_date, (int, float)) else entry_date
                writer.writerow([symbol, timeframe, entry_dt, entry_price, "open"])
        except Exception as e:
            logger.error(f"Error opening trade for {symbol}: {str(e)}")

    def close_trade(self, symbol: str, timeframe: str, exit_price: float, exit_date: int = None, is_inverse: bool = False) -> None:
        """
        Update trades dictionary and data/trades/open_trades.csv and record in data/trades/closed_trades.csv
        """
        try:
            trade_key = f"{symbol}_{'inverse_' if is_inverse else ''}{timeframe}"
            if trade_key not in self.trades:
                logger.error(f"Symbol {symbol} with timeframe {timeframe} is not being tracked")
                return

            if self.trades[trade_key] is None:
                logger.error(f"No open trade for symbol {symbol} on {timeframe}")
                return

            trade = self.trades[trade_key]
            exit_date_val = exit_date or int(time.time() * 1000)
            trade.update({
                "exit_date": exit_date_val,
                "exit_price": exit_price,
                "status": "closed",
                "pnl": exit_price - trade["entry_price"],
                "pnl_pct": ((exit_price - trade["entry_price"]) / trade["entry_price"]) * 100
            })
            logger.info(f"Closed {'inverse ' if is_inverse else ''}trade for {symbol} at {exit_price}")
            logger.info(f"P&L: ${trade['pnl']:.2f} ({trade['pnl_pct']:.2f}%)")

            # Convert entry_date and exit_date (ms) to ET string for CSV
            entry_dt = pd.to_datetime(trade["entry_date"], unit='ms', utc=True).tz_convert('US/Eastern').strftime('%Y-%m-%d %H:%M:%S') if isinstance(trade["entry_date"], (int, float)) else trade["entry_date"]
            exit_dt = pd.to_datetime(trade["exit_date"], unit='ms', utc=True).tz_convert('US/Eastern').strftime('%Y-%m-%d %H:%M:%S') if isinstance(trade["exit_date"], (int, float)) else trade["exit_date"]

            # Write to data/trades/closed_trades.csv with header
            os.makedirs('data/trades', exist_ok=True)
            closed_file = 'data/trades/closed_trades.csv'
            closed_exists = os.path.isfile(closed_file)
            with open(closed_file, 'a', newline='') as f:
                writer = csv.writer(f)
                if not closed_exists or os.stat(closed_file).st_size == 0:
                    writer.writerow(["symbol", "timeframe", "entry_date", "exit_date", "entry_price", "exit_price", "pnl", "pnl_pct"])
                writer.writerow([symbol, timeframe, entry_dt, exit_dt, trade["entry_price"], trade["exit_price"], trade["pnl"], trade["pnl_pct"]])

            # Update open_trades.csv (overwrite with only open trades, sorted by entry_date ascending)
            open_file = 'data/trades/open_trades.csv'
            open_trades = []
            for tkey, tval in self.trades.items():
                if tval is not None and tval.get("status") == "open":
                    s, tf = tkey.rsplit('_', 1)
                    entry_dt = pd.to_datetime(tval["entry_date"], unit='ms', utc=True).tz_convert('US/Eastern').strftime('%Y-%m-%d %H:%M:%S') if isinstance(tval["entry_date"], (int, float)) else tval["entry_date"]
                    open_trades.append([s, tf, entry_dt, tval["entry_price"], "open", tval["entry_date"]])  # Add raw timestamp for sorting
            # Sort by raw timestamp (last column)
            open_trades.sort(key=lambda x: x[-1])
            with open(open_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["symbol", "timeframe", "entry_date", "entry_price", "status"])
                for row in open_trades:
                    writer.writerow(row[:-1])  # Exclude raw timestamp

            # Remove trade from trades dictionary
            self.trades[trade_key] = None
            logger.info(f"Removed trade for {symbol} on {timeframe}")
            return None
        except Exception as e:
            logger.error(f"Error closing trade for {symbol}: {str(e)}")

    def fetch(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Fetch historical price data for a symbol.

        Args:
            symbol (str): The symbol to fetch data for.

        Returns:
            Optional[pd.DataFrame]: DataFrame containing the historical data, or None if the request fails.
        """
        try:
            url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
            access_token = self.auth.get_access_token()
            if not access_token:
                logger.error("No valid access token available.")
                return None
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {access_token}"
            }
            params = {
                "symbol": symbol,
                "periodType": "day",
                "period": 1,
                "frequencyType": "minute",
                "frequency": 1,
                "startDate": self.start_date,
                "endDate": self.end_date,
            }

            # Print params and headers for debugging
            print(f"Request params: {params}")
            print(f"Request headers: {headers}")

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Print the response data for debugging
            print(f"Response data for {symbol}: {data}")

            # Convert to DataFrame
            df = pd.DataFrame(data.get('candles', []))
            if df.empty:
                logger.warning(f"No data received for {symbol}")
                return None

            # Use 'datetime' instead of 'timestamp'
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
                if df['datetime'].dt.tz is None:
                    df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(self.TIMEZONE)
                else:
                    df['datetime'] = df['datetime'].dt.tz_convert(self.TIMEZONE)
                df['timestamp_ms'] = df['datetime'].astype(np.int64) // 10**6
            else:
                logger.error("No 'datetime' column in data")
                return None
            
            # Filter market hours
            df = self.filter_market_hours(df)

            # Only add timestamps > last timestamp in the data/1m/symbol.csv if it exists, otherwise add all timestamps
            if os.path.exists(f"data/1m/{symbol}.csv"):
                existing_df = pd.read_csv(f"data/1m/{symbol}.csv")
                last_timestamp = existing_df['timestamp_ms'].max()
                df = df[df['timestamp_ms'] > last_timestamp]
                        # Export to CSV
            self.export_to_csv(df, symbol, "1m")
            
            # Generate inverse data
            self.generate_inverse_ohlc(symbol, "1m")

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def export_to_csv(self, df: pd.DataFrame, symbol: str, timeframe: str) -> None:
        """
        Export the fetched price history data to a CSV file.

        Args:
            df (pd.DataFrame): The price history data to export.
            symbol (str): The equity symbol (e.g., 'AAPL').
            timeframe (str): The timeframe of the data (e.g., '1m', '5m', '1h').
        """
        # Handle timestamp/datetime
        if 'timestamp' in df.columns:
            df['timestamp_ms'] = df['timestamp']
            # Convert to ET timezone and remove timezone info
            dt_col = pd.to_datetime(df['timestamp'], unit='ms')
            if dt_col.dt.tz is None:
                dt_col = dt_col.dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
            else:
                dt_col = dt_col.dt.tz_convert('US/Eastern')
            df['datetime'] = dt_col.dt.strftime('%Y-%m-%d %H:%M:%S')
        elif 'datetime' in df.columns:
            df['timestamp_ms'] = df['datetime']
            dt_col = pd.to_datetime(df['datetime'], unit='ms')
            if dt_col.dt.tz is None:
                dt_col = dt_col.dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
            else:
                dt_col = dt_col.dt.tz_convert('US/Eastern')
            df['datetime'] = dt_col.dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Filter for market hours (9:30 AM to 4:00 PM ET)
        market_start = pd.Timestamp.now(tz='US/Eastern').replace(hour=9, minute=30, second=0, microsecond=0)
        market_end = pd.Timestamp.now(tz='US/Eastern').replace(hour=16, minute=0, second=0, microsecond=0)
        
        # Create time mask for market hours
        df['time'] = pd.to_datetime(df['datetime']).dt.time
        market_hours_mask = (
            (pd.to_datetime(df['datetime']).dt.time >= market_start.time()) & 
            (pd.to_datetime(df['datetime']).dt.time <= market_end.time())
        )
        
        # Apply the filter
        df = df[market_hours_mask]
        
        # Add symbol column
        df['symbol'] = symbol
        
        # Ensure all required columns exist
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"Warning: Column '{col}' not found in data")
                df[col] = None
        
        # Reorder columns to have timestamp_ms first, followed by datetime and other columns
        columns = ['timestamp_ms', 'datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        df = df[columns]
        
        # Define the target file path
        target_path = f"data/{timeframe}/{symbol}.csv"
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        
        # Export the DataFrame to a CSV file
        df.to_csv(target_path, index=False)
        logger.info(f"Data exported to {target_path}")
        logger.info(f"Filtered data points: {len(df)} rows")

    def generate_inverse_ohlc(self, symbol: str, timeframe: str) -> None:
        """
        Generate inverse OHLC data from an existing CSV file.
        The inverse OHLC values are calculated as 1/OHLC while keeping timestamp and volume the same.

        Args:
            symbol (str): The equity symbol (e.g., 'AAPL').
            timeframe (str): The timeframe of the data (e.g., '1m', '5m', '1h').
        """
        # Read the original CSV file
        source_path = f"data/{timeframe}/{symbol}.csv"
        if not os.path.exists(source_path):
            logger.error(f"Error: Source file {source_path} does not exist")
            return

        # Read the CSV file
        df = pd.read_csv(source_path)

        # Create a copy of the DataFrame
        df_inverse = df.copy()

        # Calculate inverse OHLC values
        ohlc_columns = ['open', 'high', 'low', 'close']
        for col in ohlc_columns:
            df_inverse[col] = 1 / df[col]

        # Define the target file path for inverse data
        target_path = f"data/{timeframe}/{symbol}_inverse.csv"

        # Export the inverse DataFrame to a CSV file
        df_inverse.to_csv(target_path, index=False)
        logger.info(f"Inverse OHLC data exported to {target_path}")

    def aggregate_timeframe(self, symbol: str, source_timeframe: str, target_timeframe: str) -> None:
        """
        Aggregate data from one timeframe to another and generate both regular and inverse files.

        Args:
            symbol (str): The equity symbol (e.g., 'AAPL').
            source_timeframe (str): The source timeframe (e.g., '1m', '5m').
            target_timeframe (str): The target timeframe (e.g., '5m', '15m').
        """
        # Read the source CSV file
        source_path = f"data/{source_timeframe}/{symbol}.csv"
        if not os.path.exists(source_path):
            logger.error(f"Error: Source file {source_path} does not exist")
            return

        # Read the CSV file
        df = pd.read_csv(source_path)
        
        # Convert datetime to pandas datetime
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Parse target_timeframe to minutes
        if target_timeframe.endswith('m'):
            target_minutes = int(target_timeframe.replace('m', ''))
        elif target_timeframe.endswith('h'):
            target_minutes = int(target_timeframe.replace('h', '')) * 60
        else:
            raise ValueError(f"Unsupported timeframe format: {target_timeframe}")
        
        # Create a resampling rule
        rule = f"{target_minutes}T"  # e.g., "5T" for 5 minutes
        
        # Group by the new timeframe
        grouped = df.groupby(pd.Grouper(key='datetime', freq=rule))
        
        # Aggregate the data
        agg_df = grouped.agg({
            'timestamp_ms': 'first',  # Keep the first timestamp
            'symbol': 'first',        # Keep the symbol
            'open': 'first',          # First price in the period
            'high': 'max',            # Highest price in the period
            'low': 'min',             # Lowest price in the period
            'close': 'last',          # Last price in the period
            'volume': 'sum'           # Sum of volume in the period
        }).reset_index()
        
        # Drop any rows with NaN values (incomplete periods)
        agg_df = agg_df.dropna()
        
        # Convert datetime back to string format
        agg_df['datetime'] = agg_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Reorder columns
        columns = ['timestamp_ms', 'datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        agg_df = agg_df[columns]
        
        # Create target directory
        target_dir = f"data/{target_timeframe}"
        os.makedirs(target_dir, exist_ok=True)
        
        # Save regular aggregated data
        target_path = f"{target_dir}/{symbol}.csv"
        agg_df.to_csv(target_path, index=False)
        logger.info(f"Aggregated data exported to {target_path}")
        
        # Generate inverse OHLC data
        df_inverse = agg_df.copy()
        ohlc_columns = ['open', 'high', 'low', 'close']
        for col in ohlc_columns:
            df_inverse[col] = 1 / df_inverse[col]
        
        # Save inverse aggregated data
        inverse_path = f"{target_dir}/{symbol}_inverse.csv"
        df_inverse.to_csv(inverse_path, index=False)
        logger.info(f"Inverse aggregated data exported to {inverse_path}")

    def calculate_indicators(self, symbol: str, timeframe: str) -> None:
        """
        Calculate technical indicators for both regular and inverse data.

        Args:
            symbol (str): The equity symbol (e.g., 'AAPL').
            timeframe (str): The timeframe of the data (e.g., '1m', '5m', '1h').
        """
        # Process regular data
        regular_path = f"data/{timeframe}/{symbol}.csv"
        if not os.path.exists(regular_path):
            logger.error(f"Error: Regular data file {regular_path} does not exist")
            return

        # Read regular data
        df_regular = pd.read_csv(regular_path)
        
        # Calculate indicators for regular data
        # EMA 7
        df_regular['ema7'] = ta.ema(df_regular['close'], length=7)
        
        # VWMA 17
        df_regular['vwma17'] = ta.vwma(df_regular['close'], df_regular['volume'], length=17)
        
        # ROC 8
        df_regular['roc8'] = ta.roc(df_regular['close'], length=8)
        
        # MACD
        try:
            macd = ta.macd(df_regular['close'])
            if macd is not None and macd.notnull().all().all():
                df_regular['macd_line'] = macd['MACD_12_26_9']
                df_regular['macd_signal'] = macd['MACDs_12_26_9']
            else:
                df_regular['macd_line'] = np.nan
                df_regular['macd_signal'] = np.nan
        except Exception as e:
            logger.error(f"MACD calculation failed for {symbol} {timeframe}: {e}")
            df_regular['macd_line'] = np.nan
            df_regular['macd_signal'] = np.nan
        
        # Save regular data with indicators
        df_regular.to_csv(regular_path, index=False)
        logger.info(f"Regular data with indicators exported to {regular_path}")
        
        # Process inverse data
        inverse_path = f"data/{timeframe}/{symbol}_inverse.csv"
        if not os.path.exists(inverse_path):
            logger.error(f"Error: Inverse data file {inverse_path} does not exist")
            return

        # Read inverse data
        df_inverse = pd.read_csv(inverse_path)
        
        # Calculate indicators for inverse data
        # EMA 7
        df_inverse['ema7'] = ta.ema(df_inverse['close'], length=7)
        
        # VWMA 17
        df_inverse['vwma17'] = ta.vwma(df_inverse['close'], df_inverse['volume'], length=17)
        
        # ROC 8
        df_inverse['roc8'] = ta.roc(df_inverse['close'], length=8)
        
        # MACD
        try:
            macd = ta.macd(df_inverse['close'])
            if macd is not None and macd.notnull().all().all():
                df_inverse['macd_line'] = macd['MACD_12_26_9']
                df_inverse['macd_signal'] = macd['MACDs_12_26_9']
            else:
                df_inverse['macd_line'] = np.nan
                df_inverse['macd_signal'] = np.nan
        except Exception as e:
            logger.error(f"MACD calculation failed for {symbol} {timeframe} (inverse): {e}")
            df_inverse['macd_line'] = np.nan
            df_inverse['macd_signal'] = np.nan
        
        # Save inverse data with indicators
        df_inverse.to_csv(inverse_path, index=False)
        logger.info(f"Inverse data with indicators exported to {inverse_path}")

    def filter_market_hours(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter the DataFrame to only include rows within regular US stock market hours (9:30 AM to 4:00 PM ET).
        Assumes 'datetime' column is timezone-aware and in US/Eastern.
        """
        if 'datetime' not in df.columns:
            logger.error("No 'datetime' column in DataFrame for market hours filtering.")
            return df
        # Ensure datetime is timezone-aware and in US/Eastern
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['datetime'] = df['datetime'].dt.tz_localize('US/Eastern', ambiguous='NaT', nonexistent='shift_forward') if df['datetime'].dt.tz is None else df['datetime']
        # Filter for market hours
        market_open = df['datetime'].dt.time >= pd.to_datetime('09:30:00').time()
        market_close = df['datetime'].dt.time <= pd.to_datetime('16:00:00').time()
        return df[market_open & market_close]

    def aggregate_new_candle(self, symbol: str, candle: Dict[str, Any], source_timeframe: str, target_timeframe: str) -> bool:
        """
        Efficiently aggregates a new candle to a higher timeframe without reprocessing the entire file.
        Returns True if the timeframe was updated, False otherwise.
        """
        try:
            # Read existing data
            source_path = f"data/{source_timeframe}/{symbol}.csv"
            target_path = f"data/{target_timeframe}/{symbol}.csv"
            
            if not os.path.exists(source_path) or not os.path.exists(target_path):
                logger.error(f"Source or target file does not exist for {symbol}")
                return False

            # Read existing data
            source_df = pd.read_csv(source_path)
            target_df = pd.read_csv(target_path)
            
            # Convert timestamp to datetime
            candle_time = pd.to_datetime(candle['timestamp'], unit='ms')
            
            # Determine the target timeframe's period
            if target_timeframe == '3m':
                period = '3T'
            elif target_timeframe == '5m':
                period = '5T'
            elif target_timeframe == '10m':
                period = '10T'
            elif target_timeframe == '15m':
                period = '15T'
            elif target_timeframe == '30m':
                period = '30T'
            else:
                logger.error(f"Unsupported target timeframe: {target_timeframe}")
                return False

            # Get the period start time for the new candle
            period_start = candle_time.floor(period)
            
            # Check if we need to update an existing candle or create a new one
            mask = target_df['timestamp'] == period_start.timestamp() * 1000
            if mask.any():
                # Update existing candle
                idx = mask.idxmax()
                target_df.loc[idx, 'open'] = candle['open']
                target_df.loc[idx, 'high'] = max(target_df.loc[idx, 'high'], candle['high'])
                target_df.loc[idx, 'low'] = min(target_df.loc[idx, 'low'], candle['low'])
                target_df.loc[idx, 'close'] = candle['close']
                target_df.loc[idx, 'volume'] += candle['volume']
            else:
                # Create new candle
                new_candle = {
                    'timestamp': int(period_start.timestamp() * 1000),
                    'open': candle['open'],
                    'high': candle['high'],
                    'low': candle['low'],
                    'close': candle['close'],
                    'volume': candle['volume']
                }
                target_df = pd.concat([target_df, pd.DataFrame([new_candle])], ignore_index=True)
                target_df = target_df.sort_values('timestamp')

            # Save updated data
            target_df.to_csv(target_path, index=False)
            logger.info(f"Successfully aggregated new candle for {symbol} from {source_timeframe} to {target_timeframe}")
            return True

        except Exception as e:
            logger.error(f"Error aggregating new candle for {symbol}: {str(e)}")
            return False

    def calculate_latest_indicators(self, symbol: str, timeframe: str, new_candle: Dict[str, Any]) -> None:
        """
        Efficiently calculates indicators for the latest candle only, using necessary historical data.
        """
        try:
            data_path = f"data/{timeframe}/{symbol}.csv"
            if not os.path.exists(data_path):
                logger.error(f"Data file {data_path} does not exist")
                return

            # Read the data
            df = pd.read_csv(data_path)
            
            # Calculate indicators for the latest candle
            latest_idx = len(df) - 1
            
            # ROC (Rate of Change)
            if latest_idx >= self.ROC_PERIOD:
                df.loc[latest_idx, 'roc8'] = ((df.loc[latest_idx, 'close'] - df.loc[latest_idx - self.ROC_PERIOD, 'close']) 
                                             / df.loc[latest_idx - self.ROC_PERIOD, 'close'] * 100)
            
            # EMA (Exponential Moving Average)
            if latest_idx >= self.EMA_PERIOD:
                df.loc[latest_idx, 'ema7'] = ta.ema(df['close'], length=self.EMA_PERIOD).iloc[-1]
            
            # VWMA (Volume Weighted Moving Average)
            if latest_idx >= self.VWMA_PERIOD:
                df.loc[latest_idx, 'vwma17'] = ta.vwma(df['close'], df['volume'], length=self.VWMA_PERIOD).iloc[-1]
            
            # MACD
            if latest_idx >= self.MACD_SLOW:
                macd = ta.macd(df['close'], 
                             fast=self.MACD_FAST, 
                             slow=self.MACD_SLOW, 
                             signal=self.MACD_SIGNAL)
                df.loc[latest_idx, 'macd_line'] = macd['MACD_12_26_9'].iloc[-1]
                df.loc[latest_idx, 'macd_signal'] = macd['MACDs_12_26_9'].iloc[-1]
                df.loc[latest_idx, 'macd_hist'] = macd['MACDh_12_26_9'].iloc[-1]
            
            # Save the updated data
            df.to_csv(data_path, index=False)
            logger.info(f"Successfully calculated latest indicators for {symbol} on {timeframe}")

        except Exception as e:
            logger.error(f"Error calculating latest indicators for {symbol} on {timeframe}: {str(e)}")

    def process_latest_signals(self, symbol: str, timeframe: str) -> None:
        """
        Efficiently processes signals for the latest entry only, checking against conditions and open trades.
        """
        try:
            # Read the latest data
            data_path = f"data/{timeframe}/{symbol}.csv"
            inverse_data_path = f"data/{timeframe}/{symbol}_inverse.csv"
            
            if not os.path.exists(data_path) or not os.path.exists(inverse_data_path):
                logger.error(f"Data files do not exist for {symbol}")
                return

            # Read only the latest row
            df = pd.read_csv(data_path).iloc[-1:]
            inverse_df = pd.read_csv(inverse_data_path).iloc[-1:]

            # Process regular trades
            trade_key = f"{symbol}_{timeframe}"
            has_open_trade = self.trades[trade_key] is not None

            # Check technical conditions for latest entry
            conditions_met = (
                df['roc8'].iloc[0] > 0 and
                df['ema7'].iloc[0] > df['vwma17'].iloc[0] and
                df['macd_line'].iloc[0] > df['macd_signal'].iloc[0]
            )

            if has_open_trade and not conditions_met:
                # Close Trade if conditions are no longer met
                self.close_trade(symbol, timeframe, df['close'].iloc[0], int(df['timestamp'].iloc[0]))
            elif not has_open_trade and conditions_met:
                # Open Trade if conditions are met and no trade is open
                self.open_trade(symbol, timeframe, df['close'].iloc[0], int(df['timestamp'].iloc[0]))

            # Process inverse trades
            trade_key = f"{symbol}_inverse_{timeframe}"
            has_open_trade = self.trades[trade_key] is not None

            # Check technical conditions for latest inverse entry
            conditions_met = (
                inverse_df['roc8'].iloc[0] > 0 and
                inverse_df['ema7'].iloc[0] > inverse_df['vwma17'].iloc[0] and
                inverse_df['macd_line'].iloc[0] > inverse_df['macd_signal'].iloc[0]
            )

            if has_open_trade and not conditions_met:
                self.close_trade(symbol, timeframe, inverse_df['close'].iloc[0], 
                               int(inverse_df['timestamp'].iloc[0]), is_inverse=True)
            elif not has_open_trade and conditions_met:
                self.open_trade(symbol, timeframe, inverse_df['close'].iloc[0], 
                              int(inverse_df['timestamp'].iloc[0]), is_inverse=True)

        except Exception as e:
            logger.error(f"Error processing latest signals for {symbol} on {timeframe}: {str(e)}")

    def add_new_candle(self, symbol: str, candle: Dict[str, Any]) -> None:
        """
        Adds a new candle's OHLCV data to both regular and inverse 1-minute timeframes,
        efficiently aggregates to higher timeframes, calculates indicators for the latest candle,
        and processes signals for the latest entry only for timeframes that were updated.
        """
        try:
            # Create inverse candle data
            inverse_candle = {
                'timestamp': candle['timestamp'],
                'open': 1 / candle['open'],
                'high': 1 / candle['low'],  # Note: high/low are inverted
                'low': 1 / candle['high'],  # Note: high/low are inverted
                'close': 1 / candle['close'],
                'volume': candle['volume']
            }

            # Add the new candle to both regular and inverse 1-minute timeframes
            self.export_to_csv(pd.DataFrame([candle]), symbol, "1m")
            self.export_to_csv(pd.DataFrame([inverse_candle]), f"{symbol}_inverse", "1m")

            # Track which timeframes were updated
            updated_timeframes = {"1m"}  # Always include 1m as it's always updated

            # Efficiently aggregate to higher timeframes for both regular and inverse
            for timeframe in self.timeframes:
                if timeframe != "1m":
                    # Check if we need to update this timeframe
                    regular_updated = self.aggregate_new_candle(symbol, candle, "1m", timeframe)
                    inverse_updated = self.aggregate_new_candle(f"{symbol}_inverse", inverse_candle, "1m", timeframe)
                    
                    if regular_updated or inverse_updated:
                        updated_timeframes.add(timeframe)

            # Calculate indicators and process signals only for updated timeframes
            for timeframe in updated_timeframes:
                # Calculate for regular data
                self.calculate_latest_indicators(symbol, timeframe, candle)
                # Calculate for inverse data
                self.calculate_latest_indicators(f"{symbol}_inverse", timeframe, inverse_candle)
                # Process regular signals
                self.process_latest_signals(symbol, timeframe)
                # Process inverse signals
                self.process_latest_signals(f"{symbol}_inverse", timeframe)

        except Exception as e:
            logger.error(f"Error adding new candle for {symbol}: {str(e)}")

def main():
    # Define symbols and timeframes
    symbols = ["AAPL", "SPY", "QQQ", "TSLA", "NVDA", "AMZN", "META", "MSFT"]
    timeframes = ["3m", "5m","10m", "15m", "30m"]
    
    # Create a SchwabAuth instance
    auth = SchwabAuth()
    
    #Get 9:30am ET in current timezone
    start_time = pd.Timestamp.now(tz='US/Eastern').replace(hour=9, minute=30, second=0, microsecond=0)
    print(start_time)
    #Get 4:00pm ET in current timezone
    end_time = pd.Timestamp.now(tz='US/Eastern').replace(hour=16, minute=0, second=0, microsecond=0)    
    print(end_time)
    # Get 9:30am ET in epoch milliseconds
    start_date = int(start_time.timestamp() * 1000)
    # Get 4:00pm ET in epoch milliseconds
    end_date = int(end_time.timestamp() * 1000)
    
    logger.info(f"Fetching data from {pd.Timestamp(start_date, unit='ms', tz='US/Eastern')} to {pd.Timestamp(end_date, unit='ms', tz='US/Eastern')}")
    # Repeat this every minute
    while True:
        AssetManager(auth, symbols, timeframes, start_date, end_date)
        # Read current and previous open trades
        open_trades = pd.read_csv('data/trades/open_trades.csv')
        prev_path = 'data/trades/prev_open_trades.csv'
        if os.path.exists(prev_path):
            prev_open_trades = pd.read_csv(prev_path)
        else:
            prev_open_trades = pd.DataFrame(columns=open_trades.columns)
        # Find new open trades
        merged = open_trades.merge(prev_open_trades, how='left', indicator=True)
        new_trades = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
        if not new_trades.empty:
            # Prepare last 5 new open trades for email
            last5 = new_trades.tail(5)
            open_positions = []
            for _, row in last5.iterrows():
                open_positions.append({
                    'symbol': row['symbol'],
                    'timeframe': row['timeframe'],
                    'entry_price': float(row['entry_price']),
                    'current_price': float(row['entry_price']),
                    'entry_time': row['entry_date'],
                    'option_type': 'EQUITY',
                    'option_symbol': 'N/A',
                    'strike_price': 0,
                    'expiry_date': 'N/A'
                })
            from email_manager.email_manager import EmailManager
            EmailManager().send_open_positions_email(open_positions)
        # Overwrite prev_open_trades.csv with current open_trades
        open_trades.to_csv(prev_path, index=False)
        time.sleep(60)

if __name__ == "__main__":
    main()  