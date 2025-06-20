"""
Signal Processor Module
Handles trade tracking, signal processing, and trade lifecycle management.

The SignalProcessor tracks open trades using a mapping of (symbol, timeframe) to trade objects,
processes buy/sell signals based on technical indicators, and manages trade persistence with
comprehensive email notifications and risk management.

CORE FEATURES:
- Trade lifecycle management with CSV persistence
- Real-time signal processing for buy/sell decisions
- Email notifications for all trade actions
- 5% stop loss risk management
- Comprehensive trade performance tracking
- Historical and real-time data processing

SIGNAL LOGIC:
Buy Signal (ALL 3 conditions must be met):
1. EMA > VWMA
2. ROC > 0  
3. MACD Line > MACD Signal Line

Sell Signal (either condition):
1. 2+ technical conditions fail, OR
2. 5% stop loss triggered (unrealized loss >= 5%)

TRADE TRACKING:
- Complete trade lifecycle: entry → monitoring → exit
- Real-time P&L updates and max gain/loss tracking
- Trade duration and performance metrics
- CSV persistence with efficient batch/append operations
- State recovery across program restarts

EMAIL NOTIFICATIONS:
- Buy signals: symbol, timeframe, entry_time, entry_price
- Sell signals: complete trade details including P&L, max gain/loss, duration
- Trade summaries: performance reports with win rates and statistics
- Daily/weekly summary emails with comprehensive analysis
- Stop loss alerts with detailed loss information

RISK MANAGEMENT:
- 5% automatic stop loss on all positions
- Real-time unrealized P&L monitoring
- Max gain/loss tracking during trade lifetime
- Position size management (one trade per symbol/timeframe)

PERFORMANCE ANALYSIS:
- Win rate calculations
- Total P&L tracking
- Trade duration analysis
- Max unrealized gain/loss per trade
- Historical performance summaries

MAIN FUNCTIONS:
- process_historical_signals(): Bulk processing of historical data
- process_latest_signal(): Real-time single-row processing
- email_trade_summary(): Comprehensive performance reports
- email_daily_summary(): Daily trade summaries
- email_weekly_summary(): Weekly performance reports

USAGE EXAMPLE:
    signal_processor = SignalProcessor()
    
    # Process historical data
    trades = signal_processor.process_historical_signals("SPY", "5m", df)
    
    # Process real-time data
    new_trade = signal_processor.process_latest_signal("SPY", "5m", latest_row)
    
    # Send performance summary
    signal_processor.email_trade_summary()
    
    # Get trade statistics
    summary = signal_processor.get_trade_summary()
    print(f"Win Rate: {summary['win_rate']:.1f}%")

FEATURES:
- Pure in-memory processing for optimal performance
- Automatic state persistence and recovery
- Smart CSV operations (batch/append/update)
- Comprehensive email notifications
- Robust risk management with stop losses
- Complete trade performance tracking
- Timezone-aware datetime operations
- Error handling and logging
- Scalable for multiple symbols and timeframes
"""

import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import Optional, List, Dict, Tuple
import os
from email_manager import EmailManager
import math


class Trade:
    """
    Represents a single trade with entry/exit details and performance metrics.
    """
    def __init__(self, symbol: str, timeframe: str, entry_time: datetime, entry_price: float):
        self.symbol = symbol.upper()
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
        self.is_open = True
        
    def update_unrealized_pnl(self, current_price: float, current_time: datetime = None):
        """Update unrealized P&L and track max gain/loss"""
        if not self.is_open:
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
        self.is_open = False
        
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
            'is_open': self.is_open
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
        if not data['is_open'] and data['exit_time'] and str(data['exit_time']).strip() != 'nan':
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
            
        trade.is_open = bool(data.get('is_open', True))
        
        return trade


class SignalProcessor:
    """
    Processes trading signals and manages trade lifecycle.
    """
    def __init__(self, trades_csv_path: str = "data/trades.csv"):
        """
        Initialize the SignalProcessor.
        
        Args:
            trades_csv_path: Path to CSV file for storing trades
        """
        self.trades_csv_path = trades_csv_path
        # Mapping of (symbol, timeframe) to Trade object for open trades
        self.open_trades: Dict[Tuple[str, str], Trade] = {}
        # List of all trades (open and closed)
        self.all_trades: List[Trade] = []
        
        # Counter for periodic saving of open trades (to persist max unrealized values)
        self.update_counter = 0
        self.save_open_trades_interval = 50  # Save open trades every 50 updates
        
        # Initialize email manager
        self.email_manager = EmailManager()
        
        # Load existing trades from CSV
        self._load_trades_from_csv()
        
        # Eastern Time zone for datetime operations
        self.et_tz = pytz.timezone('US/Eastern')
        
    def _get_trade_key(self, symbol: str, timeframe: str) -> Tuple[str, str]:
        """Get the key for storing trades"""
        return (symbol.upper(), timeframe)
        
    def _load_trades_from_csv(self):
        """Load existing trades from CSV file"""
        if os.path.exists(self.trades_csv_path):
            try:
                df = pd.read_csv(self.trades_csv_path)
                print(f"📂 Loading {len(df)} trades from {self.trades_csv_path}")
                
                for _, row in df.iterrows():
                    trade_data = row.to_dict()
                    trade = Trade.from_dict(trade_data)
                    self.all_trades.append(trade)
                    
                    # Add to open trades if still open
                    if trade.is_open:
                        key = self._get_trade_key(trade.symbol, trade.timeframe)
                        self.open_trades[key] = trade
                        print(f"📊 Loaded open trade: {trade.symbol} {trade.timeframe}")
                        
                print(f"✅ Loaded {len(self.open_trades)} open trades and {len(self.all_trades)} total trades")
                
            except Exception as e:
                print(f"❌ Error loading trades from CSV: {e}")
                
    def _save_trades_to_csv(self):
        """Save all trades to CSV file efficiently"""
        try:
            # Convert all trades to dictionaries
            trade_data = [trade.to_dict() for trade in self.all_trades]
            df = pd.DataFrame(trade_data)
            
            # Sort by is_open (open trades first), then by entry_time descending
            if 'is_open' in df.columns:
                df = df.sort_values(by=['is_open', 'entry_time'], ascending=[False, False])
                
            # Save to CSV
            df.to_csv(self.trades_csv_path, index=False)
            print(f"💾 Saved {len(df)} trades to {self.trades_csv_path}")
            
        except Exception as e:
            print(f"❌ Error saving trades to CSV: {e}")
            
    def _save_new_trade(self, trade: Trade):
        """Save a single new trade to CSV by appending"""
        try:
            # Create DataFrame for the new trade
            trade_df = pd.DataFrame([trade.to_dict()])
            
            # Append to existing CSV or create new one
            if os.path.exists(self.trades_csv_path):
                # Append to existing file
                trade_df.to_csv(self.trades_csv_path, mode='a', header=False, index=False)
                print(f"💾 Appended new trade for {trade.symbol} {trade.timeframe}")
            else:
                # Create new file
                trade_df.to_csv(self.trades_csv_path, index=False)
                print(f"💾 Created new trades file with trade for {trade.symbol} {trade.timeframe}")
                
        except Exception as e:
            print(f"❌ Error saving new trade to CSV: {e}")
            
    def _update_trade_in_csv(self, trade: Trade):
        """Update an existing trade in CSV"""
        try:
            if not os.path.exists(self.trades_csv_path):
                return
                
            # Read existing trades
            df = pd.read_csv(self.trades_csv_path)
            
            # Find the trade to update (by symbol, timeframe, and entry_time)
            mask = (
                (df['symbol'] == trade.symbol) & 
                (df['timeframe'] == trade.timeframe) & 
                (df['entry_time'] == trade.entry_time.strftime('%Y-%m-%d %H:%M:%S %Z'))
            )
            
            if mask.any():
                # Update the trade data
                trade_dict = trade.to_dict()
                for key, value in trade_dict.items():
                    df.loc[mask, key] = value
                    
                # Save updated DataFrame
                df.to_csv(self.trades_csv_path, index=False)
                print(f"💾 Updated trade for {trade.symbol} {trade.timeframe}")
            else:
                print(f"⚠️  Trade not found in CSV for update: {trade.symbol} {trade.timeframe}")
                
        except Exception as e:
            print(f"❌ Error updating trade in CSV: {e}")
            
    def _save_trades_batch(self, trades: List[Trade] = None):
        """Save multiple trades efficiently (for batch operations)"""
        if not trades:
            trades = self.all_trades
            
        try:
            # Convert trades to dictionaries
            trade_data = [trade.to_dict() for trade in trades]
            df = pd.DataFrame(trade_data)
            
            # Sort by is_open (open trades first), then by entry_time descending
            if 'is_open' in df.columns:
                df = df.sort_values(by=['is_open', 'entry_time'], ascending=[False, False])
                
            # Save to CSV
            df.to_csv(self.trades_csv_path, index=False)
            print(f"💾 Saved {len(df)} trades to {self.trades_csv_path}")
            
        except Exception as e:
            print(f"❌ Error saving trades batch to CSV: {e}")
            
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
            # Get indicator values (assuming standard column names)
            ema_col = [col for col in row.index if col.startswith('ema_')]
            vwma_col = [col for col in row.index if col.startswith('vwma_')]
            roc_col = [col for col in row.index if col.startswith('roc_')]
            macd_line_col = [col for col in row.index if col.startswith('macd_line_')]
            macd_signal_col = [col for col in row.index if col.startswith('macd_signal_')]
            
            # ENHANCED DEBUG: Check if all required indicators are present
            if debug:
                print(f"   [Buy Signal Debug] Checking indicator presence:")
                print(f"     EMA columns found: {ema_col}")
                print(f"     VWMA columns found: {vwma_col}")
                print(f"     ROC columns found: {roc_col}")
                print(f"     MACD Line columns found: {macd_line_col}")
                print(f"     MACD Signal columns found: {macd_signal_col}")
            
            # Check if all required indicators are present
            if not (ema_col and vwma_col and roc_col and macd_line_col and macd_signal_col):
                if debug:
                    missing_indicators = []
                    if not ema_col: missing_indicators.append("EMA")
                    if not vwma_col: missing_indicators.append("VWMA")
                    if not roc_col: missing_indicators.append("ROC")
                    if not macd_line_col: missing_indicators.append("MACD Line")
                    if not macd_signal_col: missing_indicators.append("MACD Signal")
                    print(f"   [Buy Signal Debug] Missing indicators: {missing_indicators}")
                return False
                
            # Get values
            ema = row[ema_col[0]]
            vwma = row[vwma_col[0]]
            roc = row[roc_col[0]]
            macd_line = row[macd_line_col[0]]
            macd_signal = row[macd_signal_col[0]]
            
            # ENHANCED DEBUG: Check for NaN values
            if debug:
                print(f"   [Buy Signal Debug] Indicator values:")
                print(f"     EMA: {ema} (NaN: {pd.isna(ema) or str(ema) == 'nan'})")
                print(f"     VWMA: {vwma} (NaN: {pd.isna(vwma) or str(vwma) == 'nan'})")
                print(f"     ROC: {roc} (NaN: {pd.isna(roc) or str(roc) == 'nan'})")
                print(f"     MACD Line: {macd_line} (NaN: {pd.isna(macd_line) or str(macd_line) == 'nan'})")
                print(f"     MACD Signal: {macd_signal} (NaN: {pd.isna(macd_signal) or str(macd_signal) == 'nan'})")
            
            # Check for NaN values using multiple methods
            def is_nan_value(value):
                return pd.isna(value) or str(value).lower() == 'nan' or (isinstance(value, float) and math.isnan(value))
            
            if is_nan_value(ema) or is_nan_value(vwma) or is_nan_value(roc) or is_nan_value(macd_line) or is_nan_value(macd_signal):
                if debug:
                    nan_indicators = []
                    if is_nan_value(ema): nan_indicators.append("EMA")
                    if is_nan_value(vwma): nan_indicators.append("VWMA")
                    if is_nan_value(roc): nan_indicators.append("ROC")
                    if is_nan_value(macd_line): nan_indicators.append("MACD Line")
                    if is_nan_value(macd_signal): nan_indicators.append("MACD Signal")
                    print(f"   [Buy Signal Debug] NaN indicators: {nan_indicators}")
                return False
                
            # Check buy signal conditions
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
                print(f"   [Buy Signal Debug] Final result: {result}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error checking buy signal: {e}")
            return False
            
    def _check_sell_signal(self, row: pd.Series, current_price: float = None, entry_price: float = None, debug: bool = False) -> bool:
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
            
            # Get indicator values (assuming standard column names)
            ema_col = [col for col in row.index if col.startswith('ema_')]
            vwma_col = [col for col in row.index if col.startswith('vwma_')]
            roc_col = [col for col in row.index if col.startswith('roc_')]
            macd_line_col = [col for col in row.index if col.startswith('macd_line_')]
            macd_signal_col = [col for col in row.index if col.startswith('macd_signal_')]
            
            # Check if all required indicators are present
            if not (ema_col and vwma_col and roc_col and macd_line_col and macd_signal_col):
                if debug:
                    print(f"   [Sell Signal Debug] Missing indicators - cannot check sell signal")
                return False
                
            # Get values
            ema = row[ema_col[0]]
            vwma = row[vwma_col[0]]
            roc = row[roc_col[0]]
            macd_line = row[macd_line_col[0]]
            macd_signal = row[macd_signal_col[0]]
            
            # Check for NaN values using multiple methods
            def is_nan_value(value):
                return pd.isna(value) or str(value).lower() == 'nan' or (isinstance(value, float) and math.isnan(value))
            
            if is_nan_value(ema) or is_nan_value(vwma) or is_nan_value(roc) or is_nan_value(macd_line) or is_nan_value(macd_signal):
                if debug:
                    print(f"   [Sell Signal Debug] NaN indicators - cannot check sell signal")
                return False
                
            # Check conditions
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
            
    def process_latest_signal(self, symbol: str, timeframe: str, row: pd.Series, save_to_csv: bool = True, is_historical: bool = False) -> Optional[Trade]:
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
        symbol = symbol.upper()
        try:
            # Debug: Entry point (only for streaming, not bootstrap)
            if not is_historical:
                print(f"\n🔍 [SignalProcessor] process_latest_signal called for {symbol} {timeframe}")
                if 'close' in row:
                    print(f"   Close price: {row['close']}")
                if 'datetime' in row:
                    print(f"   Datetime: {row['datetime']}")
                
                # NEW DEBUG: Print all column names to see what's available
                print(f"   [Debug] All columns in row: {list(row.index)}")
            
            # Parse datetime
            if 'datetime' in row:
                dt_str = row['datetime']
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
                    ema_col = [col for col in row.index if col.startswith('ema_')]
                    vwma_col = [col for col in row.index if col.startswith('vwma_')]
                    roc_col = [col for col in row.index if col.startswith('roc_')]
                    macd_line_col = [col for col in row.index if col.startswith('macd_line_')]
                    macd_signal_col = [col for col in row.index if col.startswith('macd_signal_')]
                    if ema_col and vwma_col and roc_col and macd_line_col and macd_signal_col:
                        print(f"   EMA: {row[ema_col[0]]}, VWMA: {row[vwma_col[0]]}, ROC: {row[roc_col[0]]}, MACD Line: {row[macd_line_col[0]]}, MACD Signal: {row[macd_signal_col[0]]}")
                except Exception as e:
                    print(f"   [Debug] Could not print indicator values: {e}")
            # Check for buy signal
            buy_signal = self._check_buy_signal(row, debug=not is_historical)
            if not is_historical:
                print(f"   [Debug] Buy signal check: {buy_signal}")
            if buy_signal:
                if key not in self.open_trades:
                    # Open new trade
                    trade = Trade(symbol, timeframe, dt, current_price)
                    self.open_trades[key] = trade
                    self.all_trades.append(trade)
                    if save_to_csv:
                        self._save_new_trade(trade)  # Efficiently save new trade
                    # Only print and send email for real-time signals, not historical
                    if not is_historical:
                        print(f"🟢 BUY: Opened trade for {symbol} {timeframe} at {current_price:.2f}")
                        self._send_buy_email(trade)  # Send buy email notification
                    return trade
            # Check for sell signal on existing open trade
            if key in self.open_trades:
                trade = self.open_trades[key]
                sell_signal = self._check_sell_signal(row, current_price, trade.entry_price, debug=not is_historical)
                if not is_historical:
                    print(f"   [Debug] Sell signal check: {sell_signal}")
                if sell_signal:
                    # Close trade
                    trade.close_trade(dt, current_price)
                    del self.open_trades[key]
                    if save_to_csv:
                        self._update_trade_in_csv(trade)  # Update existing trade
                    # Only print and send email for real-time signals, not historical
                    if not is_historical:
                        print(f"🔴 SELL: Closed trade for {symbol} {timeframe} at {current_price:.2f}, P&L: {trade.pnl:.2f} ({trade.pnl_percent:.2f}%)")
                        self._send_sell_email(trade)  # Send sell email notification
                else:
                    # Update unrealized P&L and max gain/loss for every row
                    trade.update_unrealized_pnl(current_price, dt)
                    if not is_historical:
                        print(f"   [Debug] No sell signal. Updated unrealized P&L for open trade.")
            else:
                if not is_historical:
                    print(f"   [Debug] No open trade for {symbol} {timeframe}.")
            # Update max unrealized gain/loss for any existing open trade (even if no signals triggered)
            if key in self.open_trades:
                trade = self.open_trades[key]
                trade.update_unrealized_pnl(current_price, dt)
            # Periodically save open trades to persist max unrealized gain/loss values
            # Only for real-time processing (not historical) to avoid excessive writes
            if not is_historical and self.open_trades:
                self.update_counter += 1
                if self.update_counter >= self.save_open_trades_interval:
                    self._save_open_trades_to_csv()
                    self.update_counter = 0  # Reset counter
        except Exception as e:
            print(f"❌ Error processing latest signal for {symbol} {timeframe}: {e}")
        return None
        
    def process_historical_signals(self, symbol: str, timeframe: str, df: pd.DataFrame) -> List[Trade]:
        """
        Process signals for historical data (DataFrame).
        
        Args:
            symbol: Stock symbol
            timeframe: Timeframe
            df: DataFrame with OHLCV data and indicators
            
        Returns:
            List[Trade]: List of trades generated from historical data
        """
        symbol = symbol.upper()
        print(f"📊 Processing historical signals for {symbol} {timeframe}")
        
        trades = []
        
        for index, row in df.iterrows():
            try:
                # Use process_latest_signal with save_to_csv=False and is_historical=True for efficiency
                new_trade = self.process_latest_signal(symbol, timeframe, row, save_to_csv=False, is_historical=True)
                if new_trade:
                    trades.append(new_trade)
                        
            except Exception as e:
                print(f"❌ Error processing row {index}: {e}")
                continue
                
        # Save all trades to CSV efficiently (batch save for historical processing)
        if trades:
            self._save_trades_batch()
        
        # Save open trades to persist max unrealized gain/loss values
        if self.open_trades:
            self._save_open_trades_to_csv()
            print(f"💾 Persisted max unrealized values for {len(self.open_trades)} open trades")
        
        print(f"✅ Processed {len(trades)} new trades from historical data")
        return trades
        
    def get_open_trades(self) -> Dict[Tuple[str, str], Trade]:
        """Get all currently open trades"""
        return self.open_trades.copy()
        
    def get_all_trades(self) -> List[Trade]:
        """Get all trades (open and closed)"""
        return self.all_trades.copy()
        
    def get_trade_summary(self) -> dict:
        """Get summary statistics of all trades"""
        if not self.all_trades:
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
            
        closed_trades = [t for t in self.all_trades if not t.is_open]
        open_trades = [t for t in self.all_trades if t.is_open]
        
        total_pnl = sum(t.pnl for t in closed_trades)
        avg_pnl = total_pnl / len(closed_trades) if closed_trades else 0.0
        winning_trades = len([t for t in closed_trades if t.pnl > 0])
        losing_trades = len([t for t in closed_trades if t.pnl < 0])
        win_rate = (winning_trades / len(closed_trades) * 100) if closed_trades else 0
        
        return {
            'total_trades': len(self.all_trades),
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
        symbol = symbol.upper()
        trade_key = (symbol, timeframe)
        
        # Filter trades for this specific symbol-timeframe
        all_trades = self.get_all_trades()
        filtered_trades = [trade for trade in all_trades 
                          if trade.symbol == symbol and trade.timeframe == timeframe]
        
        # Get closed trades for calculations
        closed_trades = [trade for trade in filtered_trades if not trade.is_open]
        
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
        
    def email_trade_summary(self, subject: str = None, include_open_trades: bool = True) -> bool:
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
                open_trades_list = self.get_open_trades()
            
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
                closed_trades = [t for t in self.all_trades if not t.is_open]
                
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
                
                for key, trade in open_trades_list.items():
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
            
            # Send email
            return self.email_manager._send_email(subject, body)
            
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
        """Send email notification for buy signal"""
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
            print(f"❌ Error sending buy email: {e}")
            
    def _send_sell_email(self, trade: Trade):
        """Send email notification for sell signal"""
        try:
            # Calculate trade duration in seconds
            trade_duration_seconds = trade.trade_duration.total_seconds() if trade.trade_duration else 0
            
            signal_details = {
                'price': trade.exit_price,
                'entry_price': trade.entry_price,
                'entry_time': trade.entry_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'sell_time': trade.exit_time.strftime('%Y-%m-%d %H:%M:%S %Z') if trade.exit_time else 'Unknown',
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
            print(f"❌ Error sending sell email: {e}")

    def _save_open_trades_to_csv(self):
        """Save current open trades to CSV to persist max unrealized gain/loss"""
        try:
            if not self.open_trades:
                return
                
            if not os.path.exists(self.trades_csv_path):
                return
                
            # Read existing trades
            df = pd.read_csv(self.trades_csv_path)
            
            # Update each open trade in the CSV
            for trade in list(self.open_trades.values()):
                mask = (
                    (df['symbol'] == trade.symbol) & 
                    (df['timeframe'] == trade.timeframe) & 
                    (df['entry_time'] == trade.entry_time.strftime('%Y-%m-%d %H:%M:%S %Z'))
                )
                
                if mask.any():
                    # Update the trade data with current max unrealized values
                    trade_dict = trade.to_dict()
                    for key, value in trade_dict.items():
                        df.loc[mask, key] = value
            
            # Sort by is_open (open trades first), then by entry_time descending
            if 'is_open' in df.columns:
                df = df.sort_values(by=['is_open', 'entry_time'], ascending=[False, False])
                
            # Save updated DataFrame
            df.to_csv(self.trades_csv_path, index=False)
            print(f"💾 Updated {len(self.open_trades)} open trades with max unrealized values")
                
        except Exception as e:
            print(f"❌ Error saving open trades to CSV: {e}")

    def email_trade_summary_by_symbol_timeframe(self, symbol: str, timeframe: str, 
                                              subject: str = None, include_open_trades: bool = True) -> bool:
        """
        Send a comprehensive trade summary email for a specific symbol and timeframe.
        
        Args:
            symbol: Trading symbol (e.g., 'SPY')
            timeframe: Timeframe (e.g., '5m')
            subject: Custom email subject (optional)
            include_open_trades: Whether to include current open trades in the email
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        try:
            symbol = symbol.upper()
            
            # Get trade summary for this specific symbol-timeframe
            summary = self.get_trade_summary_by_symbol_timeframe(symbol, timeframe)
            
            if not subject:
                subject = f"Trade Summary: {symbol} {timeframe} - {summary['total_trades']} Trades, {summary['win_rate']:.1f}% Win Rate"
            
            # Build email body
            body = f"""
📊 TRADE SUMMARY REPORT: {symbol} {timeframe}
{'=' * 50}

📈 PERFORMANCE METRICS:
• Total Trades: {summary['total_trades']}
• Winning Trades: {summary['winning_trades']}
• Losing Trades: {summary['losing_trades']}
• Win Rate: {summary['win_rate']:.1f}%

💰 PROFIT & LOSS:
• Total P&L: ${summary['total_pnl']:.2f}
• Average P&L: ${summary['avg_pnl']:.2f}
• Average Win: ${summary['avg_win']:.2f}
• Average Loss: ${summary['avg_loss']:.2f}
• Best Trade: ${summary['max_win']:.2f}
• Worst Trade: ${summary['max_loss']:.2f}

⏱️ TRADE DURATION:
• Average Duration: {summary['avg_trade_duration_hours']:.2f} hours

"""
            
            # Add open trades section if requested and trades exist
            if include_open_trades and summary['open_trades_count'] > 0:
                body += f"""
🔄 CURRENT OPEN TRADES: {summary['open_trades_count']}
• Current P&L: ${summary['open_trades_pnl']:.2f} ({(summary['open_trades_pnl']/100)*100:.2f}%)
• Max Unrealized Gain: ${summary['current_unrealized_gain']:.2f}
• Max Unrealized Loss: ${summary['current_unrealized_loss']:.2f}

"""
            
            # Add detailed trade history if there are trades
            if summary['total_trades'] > 0:
                body += f"""
📋 DETAILED TRADE HISTORY:
{'=' * 30}

"""
                # Get all trades for this symbol-timeframe
                all_trades = self.get_all_trades()
                symbol_trades = [trade for trade in all_trades 
                               if trade.symbol == symbol and trade.timeframe == timeframe]
                
                # Sort by entry time (ensure timezone consistency)
                et_tz = pytz.timezone('US/Eastern')
                def get_entry_time_for_sorting(trade):
                    entry_time = trade.entry_time
                    if entry_time.tzinfo is None:
                        # If timezone-naive, assume Eastern Time
                        entry_time = et_tz.localize(entry_time)
                    return entry_time
                
                symbol_trades.sort(key=get_entry_time_for_sorting)
                
                for i, trade in enumerate(symbol_trades[-10:], 1):  # Show last 10 trades
                    status = "🔄 OPEN" if trade.is_open else "✅ CLOSED"
                    entry_time = trade.entry_time.strftime('%Y-%m-%d %H:%M')
                    
                    if trade.is_open:
                        body += f"{i:2d}. {status} | Entry: {entry_time} @ ${trade.entry_price:.2f} | Current P&L: ${trade.pnl:.2f} ({trade.pnl_percent:.2f}%)\n"
                    else:
                        exit_time = trade.exit_time.strftime('%Y-%m-%d %H:%M')
                        duration_hours = trade.trade_duration.total_seconds() / 3600 if trade.trade_duration else 0
                        profit_indicator = "📈" if trade.pnl > 0 else "📉"
                        body += f"{i:2d}. {status} | {entry_time} → {exit_time} | ${trade.entry_price:.2f} → ${trade.exit_price:.2f} | {profit_indicator} ${trade.pnl:.2f} ({trade.pnl_percent:.2f}%) | {duration_hours:.1f}h\n"
                
                if len(symbol_trades) > 10:
                    body += f"\n... and {len(symbol_trades) - 10} more trades\n"
            
            body += f"""

📧 Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🤖 Automated Trading System
"""
            
            # Send email
            email_manager = EmailManager()
            success = email_manager._send_email(subject, body)
            
            if success:
                print(f"✅ Trade summary email sent for {symbol} {timeframe}")
            else:
                print(f"❌ Failed to send trade summary email for {symbol} {timeframe}")
                
            return success
            
        except Exception as e:
            print(f"❌ Failed to send trade summary email for {symbol} {timeframe}: {str(e)}")
            return False

    def has_processed_signals(self, symbol: str, timeframe: str) -> bool:
        """
        Check if signals have already been processed for this symbol-timeframe combination.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            
        Returns:
            bool: True if signals have been processed (trades exist), False otherwise
        """
        symbol = symbol.upper()
        return any(trade.symbol == symbol and trade.timeframe == timeframe 
                  for trade in self.all_trades)

    def drop_closed_trades_and_save_only_open(self):
        """Drop all closed trades from CSV and keep only open trades."""
        if os.path.exists(self.trades_csv_path):
            try:
                df = pd.read_csv(self.trades_csv_path)
                if 'is_open' in df.columns:
                    open_trades = df[df['is_open'] == True]
                    open_trades.to_csv(self.trades_csv_path, index=False)
                    print(f"🧹 Dropped closed trades, kept {len(open_trades)} open trades in {self.trades_csv_path}")
            except Exception as e:
                print(f"❌ Error dropping closed trades: {e}")

    def send_trades_csv_after_4pm(self):
        """Send the trades.csv file via email after 4pm ET."""
        now = datetime.now(self.et_tz)
        if now.hour >= 16:
            subject = f"Trade Log for {now.strftime('%Y-%m-%d')}"
            body = "Attached is the trade log for today."
            self.email_manager.send_trade_log_email(self.trades_csv_path, subject=subject, body=body)

if __name__ == "__main__":
    # Example usage
    signal_processor = SignalProcessor()
    
    # Example of processing historical data
    print("📊 Signal Processor initialized")
    
    # Get summary
    summary = signal_processor.get_trade_summary()
    print(f"📈 Trade Summary: {summary}")
    
    # Get open trades
    open_trades = signal_processor.get_open_trades()
    print(f"📊 Open Trades: {len(open_trades)}")
    for key, trade in open_trades.items():
        print(f"  {trade.symbol} {trade.timeframe}: Entry {trade.entry_price:.2f}, P&L {trade.pnl:.2f} ({trade.pnl_percent:.2f}%)")
