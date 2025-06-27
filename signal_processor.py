import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import Optional, List, Dict
import os
from email_manager import EmailManager
import math


class Trade:
    """
    Represents a single trade with entry/exit details and performance metrics.
    """
    def __init__(self, symbol: str, timeframe: str, entry_time: datetime, entry_price: float):
        self.symbol = symbol
        self.timeframe = timeframe
        self.entry_time = entry_time
        self.entry_price = entry_price
        self.exit_time = None
        self.exit_price = None
        self.pnl = 0.0  # Absolute P&L
        self.pnl_percent = 0.0  # Percentage P&L
        self.max_unrealized_gain = 0.0
        self.max_unrealized_loss = 0.0
        self.trade_duration = None
        
    def update_unrealized_pnl(self, current_price: float):
        """Update unrealized P&L and track max gain/loss"""
        if self.exit_time is not None:
            return
            
        self.pnl = current_price - self.entry_price
        self.pnl_percent = (self.pnl / self.entry_price) * 100
        
        # Track max unrealized gain/loss
        if self.pnl > self.max_unrealized_gain:
            self.max_unrealized_gain = self.pnl
        if self.pnl < self.max_unrealized_loss:
            self.max_unrealized_loss = self.pnl
            
    def close_trade(self, exit_time: datetime, exit_price: float):
        """Close the trade and calculate final metrics"""
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.pnl = exit_price - self.entry_price
        self.pnl_percent = (self.pnl / self.entry_price) * 100
        self.trade_duration = exit_time - self.entry_time
        
    def to_dict(self) -> dict:
        """Convert trade to dictionary for CSV storage"""
        return {
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'entry_time': self.entry_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'entry_price': self.entry_price,
            'exit_time': self.exit_time.strftime('%Y-%m-%d %H:%M:%S %Z') if self.exit_time else None,
            'exit_price': self.exit_price,
            'pnl': self.pnl,
            'pnl_percent': self.pnl_percent,
            'max_unrealized_gain': self.max_unrealized_gain,
            'max_unrealized_loss': self.max_unrealized_loss,
            'trade_duration_seconds': self.trade_duration.total_seconds() if self.trade_duration else None,
        }
        
    @classmethod
    def from_dict(cls, data: dict) -> 'Trade':
        """Create trade from dictionary (for CSV loading)"""
        et_tz = pytz.timezone('US/Eastern')
        
        # Parse entry time with better error handling
        entry_time_str = str(data['entry_time']).strip()  # Remove trailing spaces
        entry_time = None
        
        # Try different timestamp formats
        timestamp_formats = [
            '%Y-%m-%d %H:%M:%S %Z',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S EST',
            '%Y-%m-%d %H:%M:%S EDT'
        ]
        
        for fmt in timestamp_formats:
            try:
                entry_time = datetime.strptime(entry_time_str, fmt)
                break
            except ValueError:
                continue
                
        if entry_time is None:
            # Fallback: try to parse without timezone and add EST
            try:
                entry_time = datetime.strptime(entry_time_str.split(' ')[0] + ' ' + entry_time_str.split(' ')[1], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise ValueError(f"Unable to parse entry_time: '{entry_time_str}'")
        
        # Ensure timezone is set
        if not entry_time.tzinfo:
            entry_time = et_tz.localize(entry_time)
            
        # Create trade
        trade = cls(data['symbol'], data['timeframe'], entry_time, data['entry_price'])
        
        # Set other attributes if trade is closed
        if data['exit_time'] and str(data['exit_time']).strip() != 'nan':
            exit_time_str = str(data['exit_time']).strip()  # Remove trailing spaces
            exit_time = None
            
            # Try different timestamp formats for exit time
            for fmt in timestamp_formats:
                try:
                    exit_time = datetime.strptime(exit_time_str, fmt)
                    break
                except ValueError:
                    continue
                    
            if exit_time is None:
                # Fallback: try to parse without timezone and add EST
                try:
                    exit_time = datetime.strptime(exit_time_str.split(' ')[0] + ' ' + exit_time_str.split(' ')[1], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    print(f"⚠️  Unable to parse exit_time: '{exit_time_str}', skipping")
                    exit_time = None
            
            if exit_time:
                if not exit_time.tzinfo:
                    exit_time = et_tz.localize(exit_time)
                trade.close_trade(exit_time, data['exit_price'])
        
        # Set numeric attributes with safe conversion
        trade.pnl = float(data.get('pnl', 0))
        trade.pnl_percent = float(data.get('pnl_percent', 0))
        trade.max_unrealized_gain = float(data.get('max_unrealized_gain', 0))
        trade.max_unrealized_loss = float(data.get('max_unrealized_loss', 0))
        
        if data.get('trade_duration_seconds') and str(data['trade_duration_seconds']).strip() != 'nan':
            trade.trade_duration = timedelta(seconds=float(data['trade_duration_seconds']))
            
        return trade


class SignalProcessor:
    """
    Processes trading signals and manages trade lifecycle.
    """
    def __init__(self, open_trades_csv_path: str = "data/open_trades.csv", closed_trades_csv_path: str = "data/closed_trades.csv"):
        """Initialize the SignalProcessor with CSV file paths"""
        self.open_trades_csv_path = open_trades_csv_path
        self.closed_trades_csv_path = closed_trades_csv_path
        self.open_trades: Dict[str, Trade] = {}
        self.closed_trades: List[Trade] = []
        self.email_manager = EmailManager()
        self.et_tz = pytz.timezone('US/Eastern')
        
        # Signal batching for grouped emails
        self.signal_batch = {
            'buy': [],  # List of (trade, timestamp_minute) tuples
            'sell': []  # List of (trade, timestamp_minute) tuples
        }
        self.last_batch_time = None
        self.batch_timeout_seconds = 60  # Send batch after 60 seconds if no new signals
        
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Load existing trades from CSV files
        self._load_trades_from_csv()
        
        print(f"📊 Loaded {len(self.open_trades)} open trades and {len(self.closed_trades)} closed trades")
        
    def _load_trades_from_csv(self):
        """Load open trades from CSV file"""
        if os.path.exists(self.open_trades_csv_path):
            try:
                df = pd.read_csv(self.open_trades_csv_path)
                print(f"📂 Loading {len(df)} trades from {self.open_trades_csv_path}")
                
                # Remove duplicates based on symbol and timeframe (keep the latest entry)
                df = df.drop_duplicates(subset=['symbol', 'timeframe'], keep='last')
                print(f"📂 After removing duplicates: {len(df)} unique trades")
                
                for _, row in df.iterrows():
                    trade_data = row.to_dict()
                    trade = Trade.from_dict(trade_data)
                    key = self._get_trade_key(trade.symbol, trade.timeframe)
                    self.open_trades[key] = trade
                    print(f"📊 Loaded open trade: {trade.symbol} {trade.timeframe}")
                        
                print(f"✅ Loaded {len(self.open_trades)} open trades")
                
                # Save the deduplicated data back to CSV
                if len(df) > 0:
                    df.to_csv(self.open_trades_csv_path, index=False)
                    print(f"💾 Saved deduplicated trades back to CSV")
                
            except Exception as e:
                print(f"❌ Error loading trades from CSV: {e}")

    def _get_trade_key(self, symbol: str, timeframe: str) -> str:
        """Get the key for storing trades"""
        return f"{symbol}_{timeframe}"
            
    def process_historical_signals(self, symbol: str, timeframe: str, df: pd.DataFrame, index_of_first_new_row: int) -> List[Trade]:
        """
        Process signals for historical data (DataFrame).
        
        Args:
            symbol: Stock symbol
            timeframe: Timeframe
            df: DataFrame with OHLCV data and indicators
            
        Returns:
            List[Trade]: List of trades generated from historical data
        """
        symbol = symbol
        print(f"📊 Processing historical signals for {symbol} {timeframe}")
        
        # Check if we have data to process
        if index_of_first_new_row >= len(df):
            return []
        
        # Show the rows we're processing
        rows_to_process = list(df.iterrows())[index_of_first_new_row:]
        
        trades_generated = []

        for index, row in rows_to_process:
            try:
                # Use process_latest_signal with save_to_csv=False and is_historical=True for efficiency
                trade = self.process_latest_signal(symbol, timeframe, row, is_historical=True)
                if trade:
                    trades_generated.append(trade)
            except Exception as e:
                print(f"❌ Error processing latest signals for {symbol} {timeframe} at row {index}: {e}")
                continue
                
        # Save all open trades to csv   
        self._save_trades_batch(is_open=True)
        # Save all closed trades to csv
        self._save_trades_batch(is_open=False)
        
        return trades_generated

    def process_latest_signal(self, symbol: str, timeframe: str, row: pd.Series, is_historical: bool = False) -> Optional[Trade]:
        """
        Process signal for latest incoming data (single row).
        
        Args:
            symbol: Stock symbol
            timeframe: Timeframe
            row: DataFrame row with OHLCV data and indicators
            save_to_csv: Whether to save trade changes to CSV (default: True)
            is_historical: Whether this is historical data processing (default: False)
            
        Returns:
            Optional[Trade]: New trade if opened, None otherwise
        """
        try:
            # Parse datetime
            if 'datetime' in row:
                dt_str = str(row['datetime'])  # Convert to string to handle Series/ndarray
                if 'EST' in dt_str or 'EDT' in dt_str:
                    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S %Z')
                else:
                    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                    dt = self.et_tz.localize(dt)
            else:
                dt = datetime.now(self.et_tz)
                
            current_price = row['close']
            key = self._get_trade_key(symbol, timeframe)

            # Debug: Indicator values (only for streaming, not bootstrap)
            if not is_historical:
                try:
                    ema_col = row['ema'] if 'ema' in row else None
                    vwma_col = row['vwma'] if 'vwma' in row else None
                    roc_col = row['roc'] if 'roc' in row else None
                    macd_line_col = row['macd_line'] if 'macd_line' in row else None
                    macd_signal_col = row['macd_signal'] if 'macd_signal' in row else None
                    print(f"   EMA: {ema_col}, VWMA: {vwma_col}, ROC: {roc_col}, MACD Line: {macd_line_col}, MACD Signal: {macd_signal_col}")
                except Exception as e:
                    print(f"   [Debug] Could not print indicator values: {e}")

            # Check for buy signal
            buy_signal = self._check_buy_signal(row, debug=not is_historical)
            if buy_signal:
                if not is_historical:
                    print(f"   [Debug] Buy signal check: {buy_signal}")
                if key not in self.open_trades:
                    # Open new trade
                    trade = Trade(symbol, timeframe, dt, float(current_price))
                    self.open_trades[key] = trade
                    if not is_historical:
                        print(f"🟢 BUY: Opened trade for {symbol} {timeframe} at {current_price:.2f}")
                        self._send_buy_email(trade)  # Send buy email notification
                        self._save_new_trade(trade)  # Efficiently save new trade
                    return trade

            # Check for sell signal on existing open trade
            if key in self.open_trades:
                trade = self.open_trades[key]
                sell_signal = self._check_sell_signal(row, float(current_price), trade.entry_price, debug=not is_historical)
                if not is_historical:
                    print(f"   [Debug] Sell signal check: {sell_signal}")
                if sell_signal:
                    # Close trade
                    trade.close_trade(dt, float(current_price))
                    del self.open_trades[key]
                    self.closed_trades.append(trade)
                    # Only print and send email for real-time signals, not historical
                    if not is_historical:
                        print(f"🔴 SELL: Closed trade for {symbol} {timeframe} at {current_price:.2f}, P&L: {trade.pnl:.2f} ({trade.pnl_percent:.2f}%)")
                        self._send_sell_email(trade)  # Send sell email notification
                        self._save_closed_trade(trade)
                else:
                    # Update unrealized P&L and max gain/loss for every row
                    trade.update_unrealized_pnl(float(current_price))
                    if not is_historical:
                        print(f"   [Debug] No sell signal. Updated unrealized P&L for open trade.")
            else:
                if not is_historical:
                    print(f"   [Debug] No open trade for {symbol} {timeframe}.")
        except Exception as e:
            print(f"❌ Error processing latest signal for {symbol} {timeframe}: {e}")
        return None
        
    def _save_trades_batch(self, is_open: bool = True):
        """Save multiple trades efficiently (for batch operations)"""
        if is_open:
            trades = self.open_trades.values()
            trades_csv_path = self.open_trades_csv_path
        else:
            trades = self.closed_trades
            trades_csv_path = self.closed_trades_csv_path

        try:
            # Convert trades to dictionaries
            trade_data = [trade.to_dict() for trade in trades]
            df = pd.DataFrame(trade_data)
            
            if len(df) > 0:
                # Sort by entry_time for open trades, exit_time for closed trades
                if is_open:
                    df = df.sort_values(by=['entry_time'], ascending=[False])
                else:
                    df = df.sort_values(by=['exit_time'], ascending=[False])
                
                # Always overwrite the file to prevent duplicates
                df.to_csv(trades_csv_path, index=False)
                print(f"💾 Saved {len(df)} trades to {trades_csv_path}")
            else:
                # If no trades, create empty file with headers
                empty_df = pd.DataFrame(columns=pd.Index(['symbol', 'timeframe', 'entry_time', 'entry_price', 'exit_time', 'exit_price', 'pnl', 'pnl_percent', 'max_unrealized_gain', 'max_unrealized_loss', 'trade_duration_seconds']))
                empty_df.to_csv(trades_csv_path, index=False)
                print(f"💾 Created empty trades file at {trades_csv_path}")
            
        except Exception as e:
            print(f"❌ Error saving trades batch to CSV: {e}")


    def _save_closed_trade(self, trade: Trade):
        """Save a closed trade to CSV"""
        try:
            # Create DataFrame for the closed trade
            trade_df = pd.DataFrame([trade.to_dict()])
            
            # Append to existing CSV or create new one
            if os.path.exists(self.closed_trades_csv_path):
                trade_df.to_csv(self.closed_trades_csv_path, mode='a', header=False, index=False)
            else:
                trade_df.to_csv(self.closed_trades_csv_path, index=False)
            print(f"💾 Saved closed trade to {self.closed_trades_csv_path}")
            
        except Exception as e:
            print(f"❌ Error saving closed trade to CSV: {e}")

    def _check_buy_signal(self, row: pd.Series, debug: bool = False) -> bool:
        """
        Check if buy signal conditions are met.
        
        Buy Signal requires all three conditions:
        1. EMA > VWMA
        2. ROC > 0
        3. MACD Line > MACD Signal Line
        
        Args:
            row: DataFrame row with indicator values
            debug: Whether to show debug output (default: False)
            
        Returns:
            bool: True if buy signal conditions are met
        """
        try:
            if debug:
                print(f"🔍 [DEBUG] _check_buy_signal called with debug={debug}")
                print(f"🔍 [DEBUG] Row keys: {list(row.keys())}")
            
            # Get indicator values (assuming standard column names)
            ema_col = 'ema' if 'ema' in row else None
            vwma_col = 'vwma' if 'vwma' in row else None 
            roc_col = 'roc' if 'roc' in row else None
            macd_line_col = 'macd_line' if 'macd_line' in row else None
            macd_signal_col = 'macd_signal' if 'macd_signal' in row else None
            
            # ENHANCED DEBUG: Check if all required indicators are present
            if debug:
                print(f"   [Buy Signal Debug] Checking indicator presence:")
                print(f"     EMA columns found: {ema_col}")
                print(f"     VWMA columns found: {vwma_col}")
                print(f"     ROC columns found: {roc_col}")
                print(f"     MACD Line columns found: {macd_line_col}")
                print(f"     MACD Signal columns found: {macd_signal_col}")
            
            # Check if all required indicators are present
            if any(x is None or len(x) == 0 for x in [ema_col, vwma_col, roc_col, macd_line_col, macd_signal_col]):
                if debug:
                    missing_indicators = []
                    if ema_col is None or len(ema_col) == 0: missing_indicators.append("EMA") 
                    if vwma_col is None or len(vwma_col) == 0: missing_indicators.append("VWMA")
                    if roc_col is None or len(roc_col) == 0: missing_indicators.append("ROC")
                    if macd_line_col is None or len(macd_line_col) == 0: missing_indicators.append("MACD Line")
                    if macd_signal_col is None or len(macd_signal_col) == 0: missing_indicators.append("MACD Signal")
                    print(f"   [Buy Signal Debug] Missing indicators: {missing_indicators}")
                return False
                
            # Get values
            ema = row[ema_col]
            vwma = row[vwma_col]
            roc = row[roc_col]
            macd_line = row[macd_line_col]
            macd_signal = row[macd_signal_col]
            
            print(f"🔍 [DEBUG] Raw indicator values:")
            print(f"   EMA: {ema} (type: {type(ema)})")
            print(f"   VWMA: {vwma} (type: {type(vwma)})")
            print(f"   ROC: {roc} (type: {type(roc)})")
            print(f"   MACD Line: {macd_line} (type: {type(macd_line)})")
            print(f"   MACD Signal: {macd_signal} (type: {type(macd_signal)})")
            
            # ENHANCED DEBUG: Check for NaN values
            if debug:
                print(f"   [Buy Signal Debug] Indicator values:")
                print(f"     EMA: {ema} (NaN: {pd.isna(ema) or str(ema) == 'nan'})")
                print(f"     VWMA: {vwma} (NaN: {pd.isna(vwma) or str(vwma) == 'nan'})")
                print(f"     ROC: {roc} (NaN: {pd.isna(roc) or str(roc) == 'nan'})")
                print(f"     MACD Line: {macd_line} (NaN: {pd.isna(macd_line) or str(macd_line) == 'nan'})")
                print(f"     MACD Signal: {macd_signal} (NaN: {pd.isna(macd_signal) or str(macd_signal) == 'nan'})")
            
            # Check if any indicators are NaN using vectorized operations when possible
            if any(pd.isna(x) for x in [ema, vwma, roc, macd_line, macd_signal]):
                if debug:
                    nan_indicators = []
                    if pd.isna(ema): nan_indicators.append("EMA")
                    if pd.isna(vwma): nan_indicators.append("VWMA")
                    if pd.isna(roc): nan_indicators.append("ROC")
                    if pd.isna(macd_line): nan_indicators.append("MACD Line")
                    if pd.isna(macd_signal): nan_indicators.append("MACD Signal")
                    print(f"   [Buy Signal Debug] NaN indicators: {nan_indicators}")
                return False
                
            # Ensure all values are numeric before comparison
            try:
                ema = float(ema) if ema is not None else None
                vwma = float(vwma) if vwma is not None else None
                roc = float(roc) if roc is not None else None
                macd_line = float(macd_line) if macd_line is not None else None
                macd_signal = float(macd_signal) if macd_signal is not None else None
                
                print(f"🔍 [DEBUG] Converted to float:")
                print(f"   EMA: {ema}")
                print(f"   VWMA: {vwma}")
                print(f"   ROC: {roc}")
                print(f"   MACD Line: {macd_line}")
                print(f"   MACD Signal: {macd_signal}")
                
                # Double-check for None values after conversion
                if any(x is None for x in [ema, vwma, roc, macd_line, macd_signal]):
                    if debug:
                        print(f"   [Buy Signal Debug] Non-numeric indicators after conversion - cannot check buy signal")
                    return False
                    
                # At this point, all values are guaranteed to be float
                assert ema is not None and vwma is not None and roc is not None and macd_line is not None and macd_signal is not None
                    
            except (ValueError, TypeError) as e:
                print(f"🔍 [DEBUG] Error converting to float: {e}")
                if debug:
                    print(f"   [Buy Signal Debug] Non-numeric indicators - cannot check buy signal")
                return False
                
            # Check buy signal conditions (values are guaranteed to be float at this point)
            condition1 = ema > vwma
            condition2 = roc > 0
            condition3 = macd_line > macd_signal
            
            # ENHANCED DEBUG: Show condition results
            if debug:
                print(f"   [Buy Signal Debug] Condition results:")
                print(f"     Condition 1 (EMA > VWMA): {ema} > {vwma} = {condition1}")
                print(f"     Condition 2 (ROC > 0): {roc} > 0 = {condition2}")
                print(f"     Condition 3 (MACD Line > MACD Signal): {macd_line} > {macd_signal} = {condition3}")
            
            result = condition1 and condition2 and condition3
            if debug:
                print(f"🔍 [DEBUG] Buy signal result: {result}")
                print(f"   [Buy Signal Debug] Final result: {result}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error checking buy signal: {e}")
            return False
            
    def _check_sell_signal(self, row: pd.Series, current_price: Optional[float] = None, entry_price: Optional[float] = None, debug: bool = False) -> bool:
        """
        Check if sell signal conditions are met.
        
        Sell Signal requires either:
        1. Two or more of the buy conditions to fail, OR
        2. 5% stop loss triggered (unrealized loss >= 5%)
        
        Args:
            row: DataFrame row with indicator values
            current_price: Current price for stop loss calculation
            entry_price: Entry price for stop loss calculation
            debug: Whether to show debug output (default: False)
            
        Returns:
            bool: True if sell signal conditions are met
        """
        try:
            # Check for 5% stop loss first
            if current_price and entry_price:
                unrealized_loss_percent = ((current_price - entry_price) / entry_price) * 100
                if debug:
                    print(f"   [Sell Signal Debug] Stop loss check: {unrealized_loss_percent:.2f}% loss")
                if unrealized_loss_percent <= -5.0:  # 5% stop loss triggered
                    print(f"🛑 Stop loss triggered: {unrealized_loss_percent:.2f}% loss")
                    return True
            
            # Get indicator values directly from row
            ema = row.get('ema')
            vwma = row.get('vwma')
            roc = row.get('roc')
            macd_line = row.get('macd_line')
            macd_signal = row.get('macd_signal')
            
            # Check if all required indicators are present
            if any(x is None for x in [ema, vwma, roc, macd_line, macd_signal]):
                if debug:
                    print(f"   [Sell Signal Debug] Missing indicators - cannot check sell signal")
                return False
                
            # Check for NaN values using multiple methods
            def is_nan_value(value):
                # Handle both scalar and pandas Series values
                if isinstance(value, pd.Series):
                    return value.isna().any()
                return pd.isna(value) or str(value).lower() == 'nan' or (isinstance(value, float) and math.isnan(value))
            
            # Check if any indicators are NaN using vectorized operations when possible
            if any(is_nan_value(x) for x in [ema, vwma, roc, macd_line, macd_signal]):
                if debug:
                    print(f"   [Sell Signal Debug] NaN indicators - cannot check sell signal")
                return False
                
            # Ensure all values are numeric before comparison
            try:
                ema = float(ema) if ema is not None else None
                vwma = float(vwma) if vwma is not None else None
                roc = float(roc) if roc is not None else None
                macd_line = float(macd_line) if macd_line is not None else None
                macd_signal = float(macd_signal) if macd_signal is not None else None
                
                # Double-check for None values after conversion
                if any(x is None for x in [ema, vwma, roc, macd_line, macd_signal]):
                    if debug:
                        print(f"   [Sell Signal Debug] Non-numeric indicators after conversion - cannot check sell signal")
                    return False
                    
                # At this point, all values are guaranteed to be float
                assert ema is not None and vwma is not None and roc is not None and macd_line is not None and macd_signal is not None
                    
            except (ValueError, TypeError):
                if debug:
                    print(f"   [Sell Signal Debug] Non-numeric indicators - cannot check sell signal")
                return False
                
            # Check conditions (values are guaranteed to be float at this point)
            condition1 = ema > vwma
            condition2 = roc > 0
            condition3 = macd_line > macd_signal
            
            # Count failed conditions
            failed_conditions = sum([not condition1, not condition2, not condition3])
            
            # ENHANCED DEBUG: Show condition results
            if debug:
                print(f"   [Sell Signal Debug] Condition results:")
                print(f"     Condition 1 (EMA > VWMA): {ema} > {vwma} = {condition1}")
                print(f"     Condition 2 (ROC > 0): {roc} > 0 = {condition2}")
                print(f"     Condition 3 (MACD Line > MACD Signal): {macd_line} > {macd_signal} = {condition3}")
                print(f"     Failed conditions: {failed_conditions}")
            
            # Sell signal: two or more conditions failed
            result = failed_conditions >= 2
            if debug:
                print(f"   [Sell Signal Debug] Final result: {result}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error checking sell signal: {e}")
            return False

    
        
    ########################################################
    # Streaming functions
    ########################################################
        
    def get_trade_summary(self) -> dict:
        """Get summary statistics of all trades"""
        if not self.closed_trades:
            return {
                'total_trades': 0,
                'open_trades': 0,
                'closed_trades': 0,
                'total_pnl': 0.0,
                'avg_pnl': 0.0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0.0
            }
            
        closed_trades = self.closed_trades
        open_trades = self.open_trades.values()
        
        total_pnl = sum(t.pnl for t in closed_trades)
        avg_pnl = total_pnl / len(closed_trades) if closed_trades else 0.0
        winning_trades = len([t for t in closed_trades if t.pnl > 0])
        losing_trades = len([t for t in closed_trades if t.pnl < 0])
        win_rate = (winning_trades / len(closed_trades) * 100) if closed_trades else 0
        
        return {
            'total_trades': len(self.closed_trades) + len(self.open_trades),
            'open_trades': len(open_trades),
            'closed_trades': len(closed_trades),
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate
        }
        
    def get_trade_summary_by_symbol_timeframe(self, symbol: str, timeframe: str) -> dict:
        """
        Get trade summary for a specific symbol and timeframe combination.
        
        Args:
            symbol: Trading symbol (e.g., 'SPY')
            timeframe: Timeframe (e.g., '5m')
            
        Returns:
            Dictionary with trade statistics for the specific symbol-timeframe
        """
        symbol = symbol
        trade_key = f"{symbol}_{timeframe}"
        
        # Filter trades for this specific symbol-timeframe
        filtered_trades = [trade for trade in self.closed_trades 
                          if trade.symbol == symbol and trade.timeframe == timeframe]
        
        # Get closed trades for calculations
        closed_trades = filtered_trades
        
        if not closed_trades:
            # Check if there's an open trade
            open_trade = self.open_trades.get(trade_key)
            return {
                'symbol': symbol,
                'timeframe': timeframe,
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0.0,
                'total_pnl': 0.0,
                'avg_pnl': 0.0,
                'avg_win': 0.0,
                'avg_loss': 0.0,
                'max_win': 0.0,
                'max_loss': 0.0,
                'avg_trade_duration_hours': 0.0,
                'open_trades_count': 1 if open_trade else 0,
                'open_trades_pnl': open_trade.pnl if open_trade else 0.0,
                'current_unrealized_gain': open_trade.max_unrealized_gain if open_trade else 0.0,
                'current_unrealized_loss': open_trade.max_unrealized_loss if open_trade else 0.0
            }
        
        # Calculate statistics for this symbol-timeframe
        total_trades = len(closed_trades)
        winning_trades = len([t for t in closed_trades if t.pnl > 0])
        losing_trades = len([t for t in closed_trades if t.pnl <= 0])
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        total_pnl = sum(trade.pnl for trade in closed_trades)
        avg_pnl = total_pnl / total_trades if total_trades > 0 else 0.0
        
        wins = [t.pnl for t in closed_trades if t.pnl > 0]
        losses = [t.pnl for t in closed_trades if t.pnl <= 0]
        
        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        max_win = max(wins) if wins else 0
        max_loss = min(losses) if losses else 0
        
        # Calculate average trade duration
        durations = [trade.trade_duration.total_seconds() / 3600 
                    for trade in closed_trades if trade.trade_duration]
        avg_duration_hours = sum(durations) / len(durations) if durations else 0
        
        # Get open trade info
        open_trade = self.open_trades.get(trade_key)
        
        return {
            'symbol': symbol,
            'timeframe': timeframe,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'max_win': max_win,
            'max_loss': max_loss,
            'avg_trade_duration_hours': avg_duration_hours,
            'open_trades_count': 1 if open_trade else 0,
            'open_trades_pnl': open_trade.pnl if open_trade else 0.0,
            'current_unrealized_gain': open_trade.max_unrealized_gain if open_trade else 0.0,
            'current_unrealized_loss': open_trade.max_unrealized_loss if open_trade else 0.0
        }
        
    def email_trade_summary(self, subject: Optional[str], include_open_trades: bool = True) -> bool:
        """
        Send a comprehensive trade summary email
        
        Args:
            subject: Custom email subject (optional)
            include_open_trades: Whether to include current open trades in the email
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        try:
            # Get trade summary
            summary = self.get_trade_summary()
            
            # Get open trades if requested
            open_trades_list = []
            if include_open_trades:
                open_trades_list = self.open_trades.values()
            
            # Create subject
            if subject:
                pnl_emoji = "📈" if summary['avg_pnl'] >= 0 else "📉"
                subject = f"{pnl_emoji} Trade Summary - {summary['closed_trades']} Closed, {summary['open_trades']} Open"
            
            # Build email body
            body = f"""📊 TRADE SUMMARY REPORT

📈 Performance Overview:
- Total Trades: {summary['total_trades']}
- Closed Trades: {summary['closed_trades']}
- Open Trades: {summary['open_trades']}
- Average P&L: ${summary['avg_pnl']:.2f}
- Win Rate: {summary['win_rate']:.1f}%
- Winning Trades: {summary['winning_trades']}
- Losing Trades: {summary['losing_trades']}

"""
            
            # Add closed trades breakdown if any exist
            if summary['closed_trades'] > 0:
                body += "📋 Closed Trades Breakdown:\n"
                closed_trades = self.closed_trades
                
                # Sort by exit time (most recent first) with timezone handling
                et_tz = pytz.timezone('US/Eastern')
                def get_exit_time_for_sorting(trade):
                    exit_time = trade.exit_time if trade.exit_time else datetime.min.replace(tzinfo=et_tz)
                    if exit_time.tzinfo is None:
                        # If timezone-naive, assume Eastern Time
                        exit_time = et_tz.localize(exit_time)
                    return exit_time
                
                closed_trades.sort(key=get_exit_time_for_sorting, reverse=True)
                
                # Show last 10 closed trades
                for i, trade in enumerate(closed_trades[:10], 1):
                    pnl_emoji = "📈" if trade.pnl >= 0 else "📉"
                    duration = str(trade.trade_duration) if trade.trade_duration else "Unknown"
                    exit_time = trade.exit_time.strftime('%Y-%m-%d %H:%M:%S %Z') if trade.exit_time else "Unknown"
                    
                    body += f"""
{i}. {trade.symbol} {trade.timeframe}:
   Entry: ${trade.entry_price:.2f} | Exit: ${trade.exit_price:.2f}
   P&L: {pnl_emoji} ${trade.pnl:.2f} ({trade.pnl_percent:+.2f}%)
   Duration: {duration} | Exit: {exit_time}
   Max Gain: ${trade.max_unrealized_gain:.2f} | Max Loss: ${trade.max_unrealized_loss:.2f}
"""
                
                if len(closed_trades) > 10:
                    body += f"\n... and {len(closed_trades) - 10} more closed trades\n"
            
            # Add open trades if requested and any exist
            if include_open_trades and open_trades_list:
                body += f"\n📈 Current Open Trades ({len(open_trades_list)}):\n"
                
                for trade in open_trades_list:
                    if isinstance(trade, str):
                        continue
                    pnl_emoji = "📈" if trade.pnl >= 0 else "📉"
                    entry_time = trade.entry_time.strftime('%Y-%m-%d %H:%M:%S %Z')
                    
                    body += f"""
🔹 {trade.symbol} {trade.timeframe}:
   Entry: ${trade.entry_price:.2f} | Current P&L: {pnl_emoji} ${trade.pnl:.2f} ({trade.pnl_percent:+.2f}%)
   Entry Time: {entry_time}
   Max Gain: ${trade.max_unrealized_gain:.2f} | Max Loss: ${trade.max_unrealized_loss:.2f}
"""
            elif include_open_trades:
                body += "\n📈 Current Open Trades: None\n"
            
            # Add trading strategy information
            body += f"""

🎯 Trading Strategy:
- Entry: EMA > VWMA AND ROC > 0 AND MACD Line > MACD Signal
- Exit: 2+ conditions fail OR 5% stop loss
- Risk Management: 5% stop loss on all positions

📊 System Status:
- Signal Processor: Active
- Email Notifications: Enabled
- Trade Persistence: CSV Storage
- Risk Management: Stop Loss Active

⚠️ Disclaimer: This is automated analysis, not financial advice.
"""
            # Send email with closed trades CSV attachment
            if subject is None:
                subject = "Trade Summary"  # Provide default subject if None
            
            # Attach closed trades CSV file if it exists
            attachment_path = None
            if os.path.exists(self.closed_trades_csv_path):
                attachment_path = self.closed_trades_csv_path
                body += f"""

📎 ATTACHMENT:
• Closed Trades CSV: {os.path.basename(self.closed_trades_csv_path)}
  Complete detailed trade history attached for your records.
"""
            
            return self.email_manager._send_email(subject, body, attachment_path)
            
        except Exception as e:
            print(f"❌ Error sending trade summary email: {e}")
            return False
            
    def email_daily_summary(self) -> bool:
        """
        Send a daily trade summary email
        
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        today = datetime.now(self.et_tz).strftime('%Y-%m-%d')
        subject = f"📊 Daily Trade Summary - {today}"
        
        return self.email_trade_summary(subject=subject, include_open_trades=True)
        
    def email_weekly_summary(self) -> bool:
        """
        Send a weekly trade summary email
        
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        today = datetime.now(self.et_tz).strftime('%Y-%m-%d')
        subject = f"📈 Weekly Trade Summary - Week ending {today}"
        
        return self.email_trade_summary(subject=subject, include_open_trades=True)

    def _send_buy_email(self, trade: Trade):
        """Send email notification for buy signal (uses batching system)"""
        # Add to batch instead of sending immediately
        self._add_to_batch('buy', trade)
            
    def _send_sell_email(self, trade: Trade):
        """Send email notification for sell signal (uses batching system)"""
        # Add to batch instead of sending immediately  
        self._add_to_batch('sell', trade)

    def _add_to_batch(self, action: str, trade: Trade):
        """Add a trade to the signal batch for grouped email sending"""
        timestamp = trade.entry_time if action == 'buy' else (trade.exit_time or datetime.now(self.et_tz))
        minute_key = self._get_minute_key(timestamp)
        self.signal_batch[action].append((trade, minute_key))
        self.last_batch_time = datetime.now()
        
        # Check if we should send the batch immediately (multiple signals in same minute)
        self._check_and_send_batch(action, minute_key)

    def _send_individual_buy_email(self, trade: Trade):
        """Send individual buy email (fallback for single signals)"""
        try:
            signal_details = {
                'price': trade.entry_price,
                'timestamp': trade.entry_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'conditions_met': 3,  # All 3 conditions met for buy
                'condition_summary': "EMA > VWMA AND ROC > 0 AND MACD Line > MACD Signal"
            }
            
            self.email_manager.send_signal_alert(
                action='BUY',
                symbol=trade.symbol,
                timeframe=int(trade.timeframe.replace('m', '')),
                signal_details=signal_details
            )
            
        except Exception as e:
            print(f"❌ Error sending individual buy email: {e}")
            
    def _send_individual_sell_email(self, trade: Trade):
        """Send individual sell email (fallback for single signals)"""
        try:
            # Calculate trade duration in seconds
            trade_duration_seconds = trade.trade_duration.total_seconds() if trade.trade_duration else 0
            
            signal_details = {
                'price': trade.exit_price,
                'entry_price': trade.entry_price,
                'entry_time': trade.entry_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'sell_time': (trade.exit_time or datetime.now(self.et_tz)).strftime('%Y-%m-%d %H:%M:%S %Z'),
                'realized_pnl_percent': trade.pnl_percent,
                'hold_duration': str(trade.trade_duration) if trade.trade_duration else 'Unknown',
                'conditions_met': 1,  # 2+ conditions failed for sell
                'condition_summary': "EMA > VWMA AND ROC > 0 AND MACD Line > MACD Signal (2+ failed)",
                'pnl': trade.pnl,
                'pnl_percent': trade.pnl_percent,
                'max_unrealized_gain': trade.max_unrealized_gain,
                'max_unrealized_loss': trade.max_unrealized_loss,
                'trade_duration_seconds': trade_duration_seconds
            }
            
            self.email_manager.send_signal_alert(
                action='SELL',
                symbol=trade.symbol,
                timeframe=int(trade.timeframe.replace('m', '')),
                signal_details=signal_details
            )
            
        except Exception as e:
            print(f"❌ Error sending individual sell email: {e}")

    def _check_and_send_batch(self, action: str, current_minute_key: str):
        """Check if we should send a batch of signals and send if criteria are met"""
        # Count signals in the current minute
        current_minute_signals = [
            (trade, minute_key) for trade, minute_key in self.signal_batch[action] 
            if minute_key == current_minute_key
        ]
        
        # Send immediately if we have multiple signals in the same minute
        if len(current_minute_signals) >= 2:
            self._send_batched_signals(action, current_minute_key)
        # Or if it's been more than batch_timeout_seconds since last signal
        elif (self.last_batch_time and 
              (datetime.now() - self.last_batch_time).total_seconds() > self.batch_timeout_seconds):
            self._send_all_pending_batches()
    
    def _send_batched_signals(self, action: str, minute_key: str):
        """Send a batch of signals for a specific minute"""
        # Get all signals for this minute
        minute_signals = [
            (trade, mk) for trade, mk in self.signal_batch[action] 
            if mk == minute_key
        ]
        
        if not minute_signals:
            return
            
        trades = [trade for trade, _ in minute_signals]
        
        # Group by timeframe for better organization
        timeframe_groups = {}
        for trade in trades:
            tf = trade.timeframe
            if tf not in timeframe_groups:
                timeframe_groups[tf] = []
            timeframe_groups[tf].append(trade)
        
        # Create subject with symbols
        symbols = [trade.symbol for trade in trades]
        unique_symbols = list(dict.fromkeys(symbols))  # Preserve order, remove duplicates
        subject = f"{action.upper()} {', '.join(unique_symbols)}"
        
        # Create email body
        body = f"""🚨 {action.upper()} SIGNALS - {minute_key}
{'=' * 50}

📊 MULTIPLE {action.upper()} SIGNALS DETECTED:
"""
        
        total_value = 0
        for timeframe, tf_trades in timeframe_groups.items():
            body += f"\n📈 {timeframe} TIMEFRAME:\n"
            for trade in tf_trades:
                price = trade.entry_price if action == 'buy' else trade.exit_price
                total_value += price
                
                if action == 'buy':
                    body += f"• {trade.symbol} @ ${price:.2f}\n"
                else:
                    pnl_indicator = "📈" if trade.pnl > 0 else "📉"
                    body += f"• {trade.symbol} @ ${price:.2f} | {pnl_indicator} P&L: ${trade.pnl:.2f} ({trade.pnl_percent:.2f}%)\n"
        
        if action == 'buy':
            body += f"\n💰 Total Entry Value: ${total_value:.2f}"
            body += f"\n🎯 All positions opened simultaneously"
        else:
            total_pnl = sum(trade.pnl for trade in trades)
            total_pnl_percent = sum(trade.pnl_percent for trade in trades) / len(trades)
            body += f"\n💰 Total Exit Value: ${total_value:.2f}"
            body += f"\n📊 Combined P&L: ${total_pnl:.2f} ({total_pnl_percent:.2f}% avg)"
        
        body += f"\n\n📧 Batch sent at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        body += f"\n🤖 Automated Trading System"
        
        # Send the batched email
        try:
            success = self.email_manager._send_email(subject, body)
            if success:
                print(f"✅ Sent batched {action} email for {len(trades)} trades: {', '.join(unique_symbols)}")
            else:
                print(f"❌ Failed to send batched {action} email")
        except Exception as e:
            print(f"❌ Error sending batched {action} email: {e}")
        
        # Remove sent signals from batch
        self.signal_batch[action] = [
            (trade, mk) for trade, mk in self.signal_batch[action] 
            if mk != minute_key
        ]
    
    def _send_all_pending_batches(self):
        """Send all pending batches (called on timeout or shutdown)"""
        # Group remaining signals by minute and send
        for action in ['buy', 'sell']:
            minute_groups = {}
            for trade, minute_key in self.signal_batch[action]:
                if minute_key not in minute_groups:
                    minute_groups[minute_key] = []
                minute_groups[minute_key].append(trade)
            
            for minute_key, trades in minute_groups.items():
                if len(trades) == 1:
                    # Send individual email for single signals
                    trade = trades[0]
                    if action == 'buy':
                        self._send_individual_buy_email(trade)
                    else:
                        self._send_individual_sell_email(trade)
                else:
                    # Send batched email for multiple signals
                    self._send_batched_signals(action, minute_key)
        
        # Clear all batches
        self.signal_batch = {'buy': [], 'sell': []}

    def has_processed_signals(self, symbol: str, timeframe: str) -> bool:
        """
        Check if signals have already been processed for this symbol-timeframe combination.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            
        Returns:
            bool: True if signals have been processed (trades exist), False otherwise
        """
        symbol = symbol
        return any(trade.symbol == symbol and trade.timeframe == timeframe 
                  for trade in self.closed_trades)

    def drop_closed_trades_and_save_only_open(self):
        """Drop all closed trades from CSV and keep only open trades."""
        if os.path.exists(self.closed_trades_csv_path):
            try:
                df = pd.read_csv(self.closed_trades_csv_path)
                if 'exit_time' in df.columns:
                    open_trades = df[df['exit_time'] == None]
                    open_trades.to_csv(self.open_trades_csv_path, index=False)
                    print(f"🧹 Dropped closed trades, kept {len(open_trades)} open trades in {self.open_trades_csv_path}")
            except Exception as e:
                print(f"❌ Error dropping closed trades: {e}")

    def send_trades_csv_after_4pm(self):
        """Send the trades.csv file via email after 4pm ET."""
        now = datetime.now(self.et_tz)
        if now.hour >= 16:
            subject = f"Trade Log for {now.strftime('%Y-%m-%d')}"
            body = "Attached is the trade log for today."
            self.email_manager.send_trade_log_email(self.open_trades_csv_path, subject=subject, body=body)

    def _save_new_trade(self, trade: Trade):
        """Save a single new trade to CSV by appending"""
        try:
            # Create DataFrame for the new trade
            trade_df = pd.DataFrame([trade.to_dict()])
            
            # Append to existing CSV or create new one
            if os.path.exists(self.open_trades_csv_path):
                # Append to existing file
                trade_df.to_csv(self.open_trades_csv_path, mode='a', header=False, index=False)
                print(f"💾 Appended new trade for {trade.symbol} {trade.timeframe}")
            else:
                # Create new file
                trade_df.to_csv(self.open_trades_csv_path, index=False)
                print(f"💾 Created new trades file with trade for {trade.symbol} {trade.timeframe}")
                
        except Exception as e:
            print(f"❌ Error saving new trade to CSV: {e}")

    def _get_minute_key(self, timestamp: datetime) -> str:
        """Get minute-level key for batching signals (YYYY-MM-DD HH:MM)"""
        return timestamp.strftime('%Y-%m-%d %H:%M')