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
from email_manager.email_manager import EmailManager

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
    MARKET_CLOSE_HOUR = 15
    MARKET_CLOSE_MINUTE = 59
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
        self.email_manager = EmailManager()
        
        # Initialize trade tracking and fetch data for each symbol
        for symbol in symbols:
            self.fetch(symbol)
            for timeframe in timeframes:
                self.trades[symbol + "_" + timeframe] = None
                self.trades[symbol + "_inverse_" + timeframe] = None
                # First aggregate the timeframe
                self.aggregate_timeframe(symbol, "1m", timeframe)
                # Then calculate indicators for both regular and inverse data
                self.calculate_indicators(symbol, timeframe)
                # Finally process signals (without sending emails during initialization)
                self.process_signals(symbol, timeframe, send_email=False)
        
        # Send email with initial positions after processing all signals
        open_positions = []
        for symbol in symbols:
            for timeframe in timeframes:
                trade_key = f"{symbol}_{timeframe}"
                inverse_trade_key = f"{symbol}_inverse_{timeframe}"
                if self.trades[trade_key] is not None:
                    open_positions.append({
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'position_type': 'LONG',
                        'entry_price': self.trades[trade_key]['entry_price'],
                        'current_price': self.trades[trade_key]['entry_price'],
                        'entry_time': pd.to_datetime(self.trades[trade_key]['entry_date'], unit='ms', utc=True).tz_convert('US/Eastern').strftime('%Y-%m-%d %H:%M:%S')
                    })
                if self.trades[inverse_trade_key] is not None:
                    open_positions.append({
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'position_type': 'SHORT',
                        'entry_price': self.trades[inverse_trade_key]['entry_price'],
                        'current_price': self.trades[inverse_trade_key]['entry_price'],
                        'entry_time': pd.to_datetime(self.trades[inverse_trade_key]['entry_date'], unit='ms', utc=True).tz_convert('US/Eastern').strftime('%Y-%m-%d %H:%M:%S')
                    })
        if open_positions:
            self.email_manager.send_open_positions_email(open_positions)

    def process_signals(self, symbol: str, timeframe: str, send_email: bool = True) -> None:
        """
        Open csv, iterate through each row, check if buy or sell conditions are met, if so, record the action 
        Args:
            symbol (str): The symbol to open a trade for.
            timeframe (str): The timeframe to check indicators on.
            send_email (bool): Whether to send email notifications (default: True)
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

            logger.info(f"Processing signals for {symbol} on {timeframe}")
            logger.info(f"Number of rows in regular data: {len(df)}")
            logger.info(f"Number of rows in inverse data: {len(inverse_df)}")

            # Process regular trades
            for index, row in df.iterrows():
                trade_key = f"{symbol}_{timeframe}"
                has_open_trade = self.trades[trade_key] is not None

                # Check if any required indicators are NaN
                if pd.isna(row['roc8']) or pd.isna(row['ema7']) or pd.isna(row['vwma17']) or pd.isna(row['macd_line']) or pd.isna(row['macd_signal']):
                    logger.debug(f"Skipping row {index} due to NaN values in indicators")
                    continue

                # Check individual conditions
                roc_condition = row['roc8'] > 0
                ema_condition = row['ema7'] > row['vwma17']
                macd_condition = row['macd_line'] > row['macd_signal']

                # Log the indicator values and conditions
                logger.info(f"Row {index} indicators for {symbol}:")
                logger.info(f"  ROC8: {row['roc8']:.4f}")
                logger.info(f"  EMA7: {row['ema7']:.4f}")
                logger.info(f"  VWMA17: {row['vwma17']:.4f}")
                logger.info(f"  MACD Line: {row['macd_line']:.4f}")
                logger.info(f"  MACD Signal: {row['macd_signal']:.4f}")
                logger.info(f"  ROC Condition: {roc_condition}")
                logger.info(f"  EMA Condition: {ema_condition}")
                logger.info(f"  MACD Condition: {macd_condition}")

                # Buy signal: All 3 conditions must be met
                buy_signal = roc_condition and ema_condition and macd_condition

                # Sell signal: At least 2 conditions must fail
                conditions_failed = sum(1 for c in [roc_condition, ema_condition, macd_condition] if not c)
                sell_signal = conditions_failed >= 2

                logger.info(f"  Buy Signal: {buy_signal}")
                logger.info(f"  Sell Signal: {sell_signal}")

                if has_open_trade and sell_signal:
                    # Close Trade if at least 2 conditions fail
                    logger.info(f"Closing trade for {symbol} at price {row['close']}")
                    self.close_trade(symbol, timeframe, row['close'], row['timestamp'], send_email=send_email)
                elif not has_open_trade and buy_signal:
                    # Open Trade if all conditions are met and no trade is open
                    logger.info(f"Opening trade for {symbol} at price {row['close']}")
                    self.open_trade(symbol, timeframe, row['close'], row['timestamp'], send_email=send_email)

            # Process inverse trades
            for index, row in inverse_df.iterrows():
                trade_key = f"{symbol}_inverse_{timeframe}"
                has_open_trade = self.trades[trade_key] is not None

                # Check if any required indicators are NaN
                if pd.isna(row['roc8']) or pd.isna(row['ema7']) or pd.isna(row['vwma17']) or pd.isna(row['macd_line']) or pd.isna(row['macd_signal']):
                    logger.debug(f"Skipping inverse row {index} due to NaN values in indicators")
                    continue

                # Check individual conditions
                roc_condition = row['roc8'] > 0
                ema_condition = row['ema7'] > row['vwma17']
                macd_condition = row['macd_line'] > row['macd_signal']

                # Log the indicator values and conditions for inverse
                logger.info(f"Inverse row {index} indicators for {symbol}:")
                logger.info(f"  ROC8: {row['roc8']:.4f}")
                logger.info(f"  EMA7: {row['ema7']:.4f}")
                logger.info(f"  VWMA17: {row['vwma17']:.4f}")
                logger.info(f"  MACD Line: {row['macd_line']:.4f}")
                logger.info(f"  MACD Signal: {row['macd_signal']:.4f}")
                logger.info(f"  ROC Condition: {roc_condition}")
                logger.info(f"  EMA Condition: {ema_condition}")
                logger.info(f"  MACD Condition: {macd_condition}")

                # Buy signal: All 3 conditions must be met
                buy_signal = roc_condition and ema_condition and macd_condition

                # Sell signal: At least 2 conditions must fail
                conditions_failed = sum(1 for c in [roc_condition, ema_condition, macd_condition] if not c)
                sell_signal = conditions_failed >= 2

                logger.info(f"  Buy Signal: {buy_signal}")
                logger.info(f"  Sell Signal: {sell_signal}")

                if has_open_trade and sell_signal:
                    logger.info(f"Closing inverse trade for {symbol} at price {row['close']}")
                    self.close_trade(symbol, timeframe, row['close'], row['timestamp'], is_inverse=True, send_email=send_email)
                elif not has_open_trade and buy_signal:
                    logger.info(f"Opening inverse trade for {symbol} at price {row['close']}")
                    self.open_trade(symbol, timeframe, row['close'], row['timestamp'], is_inverse=True, send_email=send_email)
        except Exception as e:
            logger.error(f"Error processing signals for {symbol} on {timeframe}: {str(e)}")

    def open_trade(self, symbol: str, timeframe: str, entry_price: float, entry_date: int, is_inverse: bool = False, send_email: bool = True) -> None:
        """
        Record open trade in trades dictionary and data/trades/open_trades.csv
        Args:
            symbol (str): The symbol to open a trade for
            timeframe (str): The timeframe for the trade
            entry_price (float): The entry price
            entry_date (int): The entry timestamp in milliseconds
            is_inverse (bool): Whether this is an inverse trade
            send_email (bool): Whether to send email notification (default: True)
        """
        try:
            # Validate entry_date is not in the future
            current_time = int(time.time() * 1000)
            if entry_date > current_time:
                logger.error(f"Entry date {entry_date} is in the future")
                return

            trade_key = f"{symbol}_{'inverse_' if is_inverse else ''}{timeframe}"
            if trade_key not in self.trades:
                logger.error(f"Symbol {symbol} with timeframe {timeframe} is not being tracked")
                return

            # Only check for open trades in the same timeframe
            if self.trades[trade_key] is not None and self.trades[trade_key].get("status") == "open":
                logger.error(f"Symbol {symbol} already has an open trade in timeframe {timeframe}")
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

            # Send email notification for trade opening only if requested
            if send_email:
                position_type = "SHORT" if is_inverse else "LONG"
                self.email_manager.send_position_notification(
                    symbol=symbol,
                    period=timeframe,
                    position_type=position_type,
                    action="OPEN",
                    price=entry_price,
                    timestamp=entry_date
                )

        except Exception as e:
            logger.error(f"Error opening trade for {symbol}: {str(e)}")

    def close_trade(self, symbol: str, timeframe: str, exit_price: float, exit_date: int = None, is_inverse: bool = False, send_email: bool = True) -> None:
        """
        Update trades dictionary and data/trades/open_trades.csv and record in data/trades/closed_trades.csv
        Args:
            symbol (str): The symbol to close a trade for
            timeframe (str): The timeframe for the trade
            exit_price (float): The exit price
            exit_date (int): The exit timestamp in milliseconds
            is_inverse (bool): Whether this is an inverse trade
            send_email (bool): Whether to send email notification (default: True)
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
            if trade.get("status") == "closed":
                logger.error(f"Trade for {symbol} on {timeframe} is already closed")
                return

            # Validate exit_date is not in the future and is after entry_date
            current_time = int(time.time() * 1000)
            exit_date_val = exit_date or current_time
            if exit_date_val > current_time:
                logger.error(f"Exit date {exit_date_val} is in the future")
                return
            if exit_date_val <= trade["entry_date"]:
                logger.error(f"Exit date {exit_date_val} is before or equal to entry date {trade['entry_date']}")
                return

            # Update trade with exit information
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

            # Send email notification for trade closing only if requested
            if send_email:
                position_type = "SHORT" if is_inverse else "LONG"
                self.email_manager.send_position_notification(
                    symbol=symbol,
                    period=timeframe,
                    position_type=position_type,
                    action="CLOSE",
                    price=exit_price,
                    timestamp=exit_date_val,
                    pnl=trade["pnl"],
                    pnl_pct=trade["pnl_pct"]
                )

            # Clear the trade from trades dictionary
            self.trades[trade_key] = None

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
            
            # Calculate the last complete minute (current minute - 1)
            current_time = pd.Timestamp.now(tz=self.TIMEZONE)
            last_complete_minute = current_time.floor('min') - pd.Timedelta(minutes=1)
            last_complete_timestamp = int(last_complete_minute.timestamp() * 1000)
            
            logger.info(f"Current time: {current_time}")
            logger.info(f"Last complete minute: {last_complete_minute}")
            logger.info(f"Last complete timestamp: {last_complete_timestamp}")
            
            params = {
                "symbol": symbol,
                "periodType": "day",
                "period": 1,
                "frequencyType": "minute",
                "frequency": 1,
                "startDate": self.start_date,
                "endDate": last_complete_timestamp,  # Use last complete minute as end date
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

            logger.info(f"Received {len(df)} rows of data for {symbol}")

            # Convert datetime to pandas datetime for filtering
            df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
            df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(self.TIMEZONE)
            
            # Log the datetime range in the data
            logger.info(f"Data datetime range: {df['datetime'].min()} to {df['datetime'].max()}")
            
            # Filter for market hours (9:30 AM to 3:59 PM ET)
            df['time'] = df['datetime'].dt.time
            market_open = pd.to_datetime('09:30:00').time()
            market_close = pd.to_datetime('15:59:00').time()
            
            # Log the time range we're filtering for
            logger.info(f"Filtering for market hours: {market_open} to {market_close}")
            
            # Log some sample times from data
            logger.info(f"Sample times from data: {df['time'].head()}")
            
            # Apply market hours filter
            df = df[(df['time'] >= market_open) & (df['time'] <= market_close)]
            
            logger.info(f"After market hours filter: {len(df)} rows")
            
            # Drop the temporary time column
            df = df.drop('time', axis=1)
            
            # Filter out the current minute's data
            df = df[df['datetime'].dt.floor('min') < current_time.floor('min')]
            
            if df.empty:
                logger.warning(f"No completed candles to save for {symbol}")
                return None
                
            logger.info(f"After filtering current minute: {len(df)} rows")
            logger.info(f"Final datetime range: {df['datetime'].min()} to {df['datetime'].max()}")
            
            # Convert back to milliseconds for storage
            df['datetime'] = df['datetime'].dt.tz_convert('UTC').astype('int64') // 10**6
            df = df.rename(columns={'datetime': 'timestamp'})  # Rename datetime to timestamp

            # Create all required data directories
            timeframes = ['1m', '3m', '5m', '10m', '15m', '30m']
            for tf in timeframes:
                os.makedirs(f"data/{tf}", exist_ok=True)

            # Export to CSV
            self.export_to_csv(df, symbol, "1m")
            
            # Generate inverse data
            self.generate_inverse_ohlc(symbol, "1m")
            
            # Aggregate to other timeframes
            for tf in self.timeframes:
                self.aggregate_timeframe(symbol, "1m", tf)
                self.generate_inverse_ohlc(symbol, tf)
            
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def export_to_csv(self, df: pd.DataFrame, symbol: str, timeframe: str) -> None:
        """
        Export DataFrame to CSV file, handling both new files and appending to existing ones.

        Args:
            df (pd.DataFrame): The DataFrame to export.
            symbol (str): The equity symbol (e.g., 'AAPL').
            timeframe (str): The timeframe of the data (e.g., '1m', '5m', '1h').
        """
        try:
            # Ensure the data directory exists
            os.makedirs(f"data/{timeframe}", exist_ok=True)
            
            # Convert timestamp to datetime in ET
            if 'timestamp' in df.columns:
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
                df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Add symbol column if it doesn't exist
            if 'symbol' not in df.columns:
                df['symbol'] = symbol
            
            # Reorder columns
            columns = ['timestamp', 'datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            df = df[columns]
            
            # Save to CSV
            target_path = f"data/{timeframe}/{symbol}.csv"
            
            # Check if file exists
            if os.path.exists(target_path):
                # Read existing data
                existing_df = pd.read_csv(target_path)
                
                # Convert timestamp to int for comparison
                existing_df['timestamp'] = existing_df['timestamp'].astype(int)
                df['timestamp'] = df['timestamp'].astype(int)
                
                # Get the last timestamp from existing data
                last_timestamp = existing_df['timestamp'].max()
                
                # Only keep new candles with timestamps greater than the last timestamp
                df = df[df['timestamp'] > last_timestamp]
                
                if not df.empty:
                    # Concatenate existing and new data
                    df = pd.concat([existing_df, df], ignore_index=True)
                    
                    # Sort by timestamp
                    df = df.sort_values('timestamp')
                    
                    # Save to CSV
                    df.to_csv(target_path, index=False)
                    logger.info(f"Successfully appended {len(df) - len(existing_df)} new candles to {target_path}")
            else:
                # If file doesn't exist, just save the new data
                df.to_csv(target_path, index=False)
                logger.info(f"Successfully created new file {target_path} with {len(df)} candles")
            
        except Exception as e:
            logger.error(f"Error exporting data to CSV for {symbol} {timeframe}: {str(e)}")

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
        
        # Ensure 'timestamp' column exists
        if 'timestamp' not in df.columns:
            # If 'datetime' is in string format, convert to pandas datetime, then to ms
            if 'datetime' in df.columns:
                dt_col = pd.to_datetime(df['datetime'])
                # If timezone-naive, localize to US/Eastern, then convert to UTC
                if dt_col.dt.tz is None:
                    dt_col = dt_col.dt.tz_localize('US/Eastern').dt.tz_convert('UTC')
                else:
                    dt_col = dt_col.dt.tz_convert('UTC')
                df['timestamp'] = (dt_col.astype('int64') // 10**6).astype(int)
            else:
                logger.error("No 'datetime' column to create 'timestamp' from.")
                return
        
        # Convert datetime to pandas datetime
        df['datetime'] = pd.to_datetime(df['datetime'])
        
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
        try:
            # Create all required data directories
            timeframes = ['1m', '3m', '5m', '10m', '15m', '30m']
            for tf in timeframes:
                os.makedirs(f"data/{tf}", exist_ok=True)
                
            # Read the source CSV file
            source_path = f"data/{source_timeframe}/{symbol}.csv"
            if not os.path.exists(source_path):
                logger.error(f"Error: Source file {source_path} does not exist")
                return

            # Read the CSV file
            df = pd.read_csv(source_path)
            
            if df.empty:
                logger.warning(f"No data to aggregate for {symbol} {source_timeframe}")
                return
                
            # Convert datetime to pandas datetime
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # Parse target_timeframe to minutes
            if target_timeframe.endswith('m'):
                target_minutes = int(target_timeframe.replace('m', ''))
            elif target_timeframe.endswith('h'):
                target_minutes = int(target_timeframe.replace('h', '')) * 60
            else:
                raise ValueError(f"Unsupported timeframe format: {target_timeframe}")
            
            # Create a resampling rule using 'min' instead of 'T'
            rule = f"{target_minutes}min"
            
            # Group by the new timeframe
            grouped = df.groupby(pd.Grouper(key='datetime', freq=rule))
            
            # Aggregate the data
            agg_df = grouped.agg({
                'timestamp': 'first',    # Keep the first timestamp
                'symbol': 'first',       # Keep the symbol
                'open': 'first',         # First price in the period
                'high': 'max',           # Highest price in the period
                'low': 'min',            # Lowest price in the period
                'close': 'last',         # Last price in the period
                'volume': 'sum'          # Sum of volume in the period
            }).reset_index()
            
            # Drop any rows with NaN values (incomplete periods)
            agg_df = agg_df.dropna()
            
            # Convert datetime back to string format
            agg_df['datetime'] = agg_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Reorder columns
            columns = ['timestamp', 'datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            agg_df = agg_df[columns]
            
            # Save regular aggregated data
            target_path = f"data/{target_timeframe}/{symbol}.csv"
            agg_df.to_csv(target_path, index=False)
            logger.info(f"Aggregated data exported to {target_path}")
            
            # Generate inverse OHLC data
            df_inverse = agg_df.copy()
            ohlc_columns = ['open', 'high', 'low', 'close']
            for col in ohlc_columns:
                df_inverse[col] = 1 / df_inverse[col]
            
            # Save inverse aggregated data with correct naming
            inverse_path = f"data/{target_timeframe}/{symbol}_inverse.csv"
            df_inverse.to_csv(inverse_path, index=False)
            logger.info(f"Inverse aggregated data exported to {inverse_path}")
            
        except Exception as e:
            logger.error(f"Error aggregating timeframe for {symbol} from {source_timeframe} to {target_timeframe}: {str(e)}")

    def calculate_indicators(self, symbol: str, timeframe: str) -> None:
        """
        Calculate technical indicators for both regular and inverse data using the entire dataset.

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
        
        # Calculate indicators for regular data using entire dataset
        # EMA 7
        df_regular['ema7'] = ta.ema(df_regular['close'], length=self.EMA_PERIOD)
        
        # EMAs for MACD
        df_regular['ema12'] = ta.ema(df_regular['close'], length=self.MACD_FAST)
        df_regular['ema26'] = ta.ema(df_regular['close'], length=self.MACD_SLOW)
        
        # VWMA 17
        df_regular['vwma17'] = ta.vwma(df_regular['close'], df_regular['volume'], length=self.VWMA_PERIOD)
        
        # ROC 8
        df_regular['roc8'] = ta.roc(df_regular['close'], length=self.ROC_PERIOD)
        
        # MACD
        try:
            # Calculate MACD line (12-day EMA - 26-day EMA)
            df_regular['macd_line'] = df_regular['ema12'] - df_regular['ema26']
            # Calculate Signal line (9-day EMA of MACD line)
            df_regular['macd_signal'] = ta.ema(df_regular['macd_line'], length=self.MACD_SIGNAL)
        except Exception as e:
            logger.error(f"MACD calculation failed for {symbol} {timeframe}: {e}")
            df_regular['macd_line'] = np.nan
            df_regular['macd_signal'] = np.nan
        
        # Drop intermediate columns used for calculations
        df_regular = df_regular.drop(['ema12', 'ema26'], axis=1)
        
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
        
        # Calculate indicators for inverse data using entire dataset
        # EMA 7
        df_inverse['ema7'] = ta.ema(df_inverse['close'], length=self.EMA_PERIOD)
        
        # EMAs for MACD
        df_inverse['ema12'] = ta.ema(df_inverse['close'], length=self.MACD_FAST)
        df_inverse['ema26'] = ta.ema(df_inverse['close'], length=self.MACD_SLOW)
        
        # VWMA 17
        df_inverse['vwma17'] = ta.vwma(df_inverse['close'], df_inverse['volume'], length=self.VWMA_PERIOD)
        
        # ROC 8
        df_inverse['roc8'] = ta.roc(df_inverse['close'], length=self.ROC_PERIOD)
        
        # MACD
        try:
            # Calculate MACD line (12-day EMA - 26-day EMA)
            df_inverse['macd_line'] = df_inverse['ema12'] - df_inverse['ema26']
            # Calculate Signal line (9-day EMA of MACD line)
            df_inverse['macd_signal'] = ta.ema(df_inverse['macd_line'], length=self.MACD_SIGNAL)
        except Exception as e:
            logger.error(f"MACD calculation failed for {symbol} {timeframe} (inverse): {e}")
            df_inverse['macd_line'] = np.nan
            df_inverse['macd_signal'] = np.nan
        
        # Drop intermediate columns used for calculations
        df_inverse = df_inverse.drop(['ema12', 'ema26'], axis=1)
        
        # Save inverse data with indicators
        df_inverse.to_csv(inverse_path, index=False)
        logger.info(f"Inverse data with indicators exported to {inverse_path}")

    def filter_market_hours(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter the DataFrame to only include rows within regular US stock market hours (9:30 AM to 3:59 PM ET).
        Assumes 'datetime' column is timezone-aware and in US/Eastern.
        """
        if 'datetime' not in df.columns:
            logger.error("No 'datetime' column in DataFrame for market hours filtering.")
            return df
            
        # Convert datetime to pandas datetime if it's not already
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Handle timezone conversion properly
        if df['datetime'].dt.tz is None:
            # If naive, localize to US/Eastern
            df['datetime'] = df['datetime'].dt.tz_localize('US/Eastern', ambiguous='NaT', nonexistent='shift_forward')
        else:
            # If already timezone-aware, convert to US/Eastern
            df['datetime'] = df['datetime'].dt.tz_convert('US/Eastern')
            
        # Filter for market hours
        market_open = df['datetime'].dt.time >= pd.to_datetime('09:30:00').time()
        market_close = df['datetime'].dt.time <= pd.to_datetime('15:59:00').time()
        return df[market_open & market_close]

    def aggregate_new_candle(self, symbol: str, candle: dict, source_timeframe: str, target_timeframe: str) -> bool:
        """
        Aggregate a new candle from source timeframe to target timeframe.

        Args:
            symbol (str): The equity symbol (e.g., 'AAPL').
            candle (dict): The new candle data.
            source_timeframe (str): The source timeframe (e.g., '1m', '5m').
            target_timeframe (str): The target timeframe (e.g., '5m', '15m').

        Returns:
            bool: True if the aggregation was successful, False otherwise.
        """
        try:
            # Create a DataFrame from the new candle
            new_candle_df = pd.DataFrame([candle])
            
            # Read existing data
            source_path = f"data/{source_timeframe}/{symbol}.csv"
            if not os.path.exists(source_path):
                logger.error(f"Error: Source file {source_path} does not exist")
                return False

            df = pd.read_csv(source_path)
            
            # Append new candle
            df = pd.concat([df, new_candle_df], ignore_index=True)
            
            # Convert datetime to pandas datetime
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # Parse target_timeframe to minutes
            if target_timeframe.endswith('m'):
                target_minutes = int(target_timeframe.replace('m', ''))
            elif target_timeframe.endswith('h'):
                target_minutes = int(target_timeframe.replace('h', '')) * 60
            else:
                raise ValueError(f"Unsupported timeframe format: {target_timeframe}")
            
            # Create a resampling rule using 'min' instead of 'T'
            rule = f"{target_minutes}min"
            
            # Group by the new timeframe
            grouped = df.groupby(pd.Grouper(key='datetime', freq=rule))
            
            # Aggregate only the OHLCV data
            agg_df = grouped.agg({
                'timestamp': 'first',    # Keep the first timestamp
                'symbol': 'first',       # Keep the symbol
                'open': 'first',         # First price in the period
                'high': 'max',           # Highest price in the period
                'low': 'min',            # Lowest price in the period
                'close': 'last',         # Last price in the period
                'volume': 'sum'          # Sum of volume in the period
            }).reset_index()
            
            # Drop any rows with NaN values (incomplete periods)
            agg_df = agg_df.dropna()
            
            # Convert datetime back to string format
            agg_df['datetime'] = agg_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Reorder columns
            columns = ['timestamp', 'datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            agg_df = agg_df[columns]
            
            # Save regular aggregated data
            target_path = f"data/{target_timeframe}/{symbol}.csv"
            agg_df.to_csv(target_path, index=False)
            
            # Generate inverse OHLC data
            df_inverse = agg_df.copy()
            ohlc_columns = ['open', 'high', 'low', 'close']
            for col in ohlc_columns:
                df_inverse[col] = 1 / df_inverse[col]
            
            # Save inverse aggregated data with correct naming
            inverse_path = f"data/{target_timeframe}/{symbol}_inverse.csv"
            df_inverse.to_csv(inverse_path, index=False)
            
            logger.info(f"Successfully aggregated new candle for {symbol} from {source_timeframe} to {target_timeframe}")
            return True

        except Exception as e:
            logger.error(f"Error aggregating new candle for {symbol} from {source_timeframe} to {target_timeframe}: {str(e)}")
            return False

    def add_new_candle(self, symbol: str, candle: dict) -> None:
        """
        Add a new candle to the data and update all timeframes.
        Only processes candles that are from the last complete minute.

        Args:
            symbol (str): The symbol to add the candle for.
            candle (dict): The candle data to add.
        """
        try:
            # Convert timestamp to datetime for comparison
            candle_time = pd.Timestamp(candle['timestamp'], unit='ms', tz='UTC')
            current_time = pd.Timestamp.now(tz='UTC')
            
            # Only process if the candle is from the last complete minute
            last_complete_minute = current_time.floor('min') - pd.Timedelta(minutes=1)
            if candle_time.floor('min') >= current_time.floor('min'):
                logger.info(f"Skipping incomplete candle for {symbol} at {candle_time}")
                return

            # Add the new candle to the 1-minute data
            self.export_to_csv(pd.DataFrame([candle]), symbol, "1m")
            
            # Update all other timeframes
            for tf in self.timeframes:
                # First aggregate the new candle to the target timeframe
                success = self.aggregate_new_candle(symbol, candle, "1m", tf)
                if success:
                    # Then calculate indicators for both regular and inverse data
                    self.calculate_indicators(symbol, tf)
                    # Finally process signals (with email notifications during streaming)
                    self.process_signals(symbol, tf, send_email=True)
                else:
                    logger.error(f"Failed to aggregate new candle for {symbol} to {tf} timeframe")

        except Exception as e:
            logger.error(f"Error adding new candle for {symbol}: {str(e)}")

    def read_inverse_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """
        Read inverse OHLC data for a symbol and timeframe.

        Args:
            symbol (str): The equity symbol (e.g., 'AAPL').
            timeframe (str): The timeframe (e.g., '1m', '5m').

        Returns:
            pd.DataFrame: DataFrame containing inverse OHLC data.
        """
        try:
            # Ensure the data directory exists
            os.makedirs(f"data/{timeframe}", exist_ok=True)
            
            # Read the inverse data file
            inverse_path = f"data/{timeframe}/{symbol}_inverse.csv"
            if not os.path.exists(inverse_path):
                logger.error(f"Inverse data file {inverse_path} does not exist, fix it for all time frames")
                return pd.DataFrame()
                
            df = pd.read_csv(inverse_path)
            return df

        except Exception as e:
            logger.error(f"Error reading inverse data for {symbol} {timeframe}: {str(e)}")
            return pd.DataFrame()