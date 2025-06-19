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
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List
import os
import pytz
from email_manager.email_manager import EmailManager


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
        
    def update_unrealized_pnl(self, current_price: float, current_time: datetime):
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
        
        # Parse entry time
        entry_time = datetime.strptime(data['entry_time'], '%Y-%m-%d %H:%M:%S %Z')
        if entry_time.tzinfo is None:
            entry_time = et_tz.localize(entry_time)
            
        # Create trade
        trade = cls(data['symbol'], data['timeframe'], entry_time, data['entry_price'])
        
        # Set other attributes if trade is closed
        if not data['is_open'] and data['exit_time']:
            exit_time = datetime.strptime(data['exit_time'], '%Y-%m-%d %H:%M:%S %Z')
            if exit_time.tzinfo is None:
                exit_time = et_tz.localize(exit_time)
            trade.close_trade(exit_time, data['exit_price'])
        
        trade.pnl = data['pnl']
        trade.pnl_percent = data['pnl_percent']
        trade.max_unrealized_gain = data['max_unrealized_gain']
        trade.max_unrealized_loss = data['max_unrealized_loss']
        
        if data['trade_duration_seconds']:
            trade.trade_duration = timedelta(seconds=data['trade_duration_seconds'])
            
        trade.is_open = data['is_open']
        
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
        if trades is None:
            trades = self.all_trades
            
        try:
            # Convert trades to dictionaries
            trade_data = [trade.to_dict() for trade in trades]
            df = pd.DataFrame(trade_data)
            
            # Save to CSV
            df.to_csv(self.trades_csv_path, index=False)
            print(f"💾 Saved {len(df)} trades to {self.trades_csv_path}")
            
        except Exception as e:
            print(f"❌ Error saving trades batch to CSV: {e}")
            
    def _check_buy_signal(self, row: pd.Series) -> bool:
        """
        Check if buy signal conditions are met.
        
        Buy Signal requires all three conditions:
        1. EMA > VWMA
        2. ROC > 0
        3. MACD Line > MACD Signal Line
        
        Args:
            row: DataFrame row with indicator values
            
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
            
            # Check if all required indicators are present
            if not (ema_col and vwma_col and roc_col and macd_line_col and macd_signal_col):
                return False
                
            # Get values
            ema = row[ema_col[0]]
            vwma = row[vwma_col[0]]
            roc = row[roc_col[0]]
            macd_line = row[macd_line_col[0]]
            macd_signal = row[macd_signal_col[0]]
            
            # Check for NaN values
            if pd.isna(ema) or pd.isna(vwma) or pd.isna(roc) or pd.isna(macd_line) or pd.isna(macd_signal):
                return False
                
            # Check buy signal conditions
            condition1 = ema > vwma
            condition2 = roc > 0
            condition3 = macd_line > macd_signal
            
            return condition1 and condition2 and condition3
            
        except Exception as e:
            print(f"❌ Error checking buy signal: {e}")
            return False
            
    def _check_sell_signal(self, row: pd.Series, current_price: float = None, entry_price: float = None) -> bool:
        """
        Check if sell signal conditions are met.
        
        Sell Signal requires either:
        1. Two or more of the buy conditions to fail, OR
        2. 5% stop loss triggered (unrealized loss >= 5%)
        
        Args:
            row: DataFrame row with indicator values
            current_price: Current price for stop loss calculation
            entry_price: Entry price for stop loss calculation
            
        Returns:
            bool: True if sell signal conditions are met
        """
        try:
            # Check for 5% stop loss first
            if current_price is not None and entry_price is not None:
                unrealized_loss_percent = ((current_price - entry_price) / entry_price) * 100
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
                return False
                
            # Get values
            ema = row[ema_col[0]]
            vwma = row[vwma_col[0]]
            roc = row[roc_col[0]]
            macd_line = row[macd_line_col[0]]
            macd_signal = row[macd_signal_col[0]]
            
            # Check for NaN values
            if pd.isna(ema) or pd.isna(vwma) or pd.isna(roc) or pd.isna(macd_line) or pd.isna(macd_signal):
                return False
                
            # Check conditions
            condition1 = ema > vwma
            condition2 = roc > 0
            condition3 = macd_line > macd_signal
            
            # Count failed conditions
            failed_conditions = sum([not condition1, not condition2, not condition3])
            
            # Sell signal: two or more conditions failed
            return failed_conditions >= 2
            
        except Exception as e:
            print(f"❌ Error checking sell signal: {e}")
            return False
            
    def process_latest_signal(self, symbol: str, timeframe: str, row: pd.Series, save_to_csv: bool = True) -> Optional[Trade]:
        """
        Process signal for latest incoming data (single row).
        
        Args:
            symbol: Stock symbol
            timeframe: Timeframe
            row: DataFrame row with OHLCV data and indicators
            save_to_csv: Whether to save trade changes to CSV (default: True)
            
        Returns:
            Optional[Trade]: New trade if opened, None otherwise
        """
        symbol = symbol.upper()
        
        try:
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
            
            # Check for buy signal
            if self._check_buy_signal(row):
                if key not in self.open_trades:
                    # Open new trade
                    trade = Trade(symbol, timeframe, dt, current_price)
                    self.open_trades[key] = trade
                    self.all_trades.append(trade)
                    if save_to_csv:
                        self._save_new_trade(trade)  # Efficiently save new trade
                    print(f"🟢 BUY: Opened trade for {symbol} {timeframe} at {current_price:.2f}")
                    self._send_buy_email(trade)  # Send buy email notification
                    return trade
                    
            # Check for sell signal on existing open trade
            if key in self.open_trades:
                trade = self.open_trades[key]
                if self._check_sell_signal(row, current_price, trade.entry_price):
                    # Close trade
                    trade.close_trade(dt, current_price)
                    del self.open_trades[key]
                    if save_to_csv:
                        self._update_trade_in_csv(trade)  # Update existing trade
                    print(f"🔴 SELL: Closed trade for {symbol} {timeframe} at {current_price:.2f}, P&L: {trade.pnl:.2f} ({trade.pnl_percent:.2f}%)")
                    self._send_sell_email(trade)  # Send sell email notification
                else:
                    # Update unrealized P&L and max gain/loss for every row
                    trade.update_unrealized_pnl(current_price, dt)
                    # Note: We don't save unrealized P&L updates to CSV to avoid excessive writes
                    
            # Update max unrealized gain/loss for any existing open trade (even if no signals triggered)
            if key in self.open_trades:
                trade = self.open_trades[key]
                trade.update_unrealized_pnl(current_price, dt)
                    
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
                # Use process_latest_signal with save_to_csv=False for efficiency
                new_trade = self.process_latest_signal(symbol, timeframe, row, save_to_csv=False)
                if new_trade:
                    trades.append(new_trade)
                        
            except Exception as e:
                print(f"❌ Error processing row {index}: {e}")
                continue
                
        # Save all trades to CSV efficiently (batch save for historical processing)
        if trades:
            self._save_trades_batch()
        
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
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0.0
            }
            
        closed_trades = [t for t in self.all_trades if not t.is_open]
        open_trades = [t for t in self.all_trades if t.is_open]
        
        total_pnl = sum(t.pnl for t in closed_trades)
        winning_trades = len([t for t in closed_trades if t.pnl > 0])
        losing_trades = len([t for t in closed_trades if t.pnl < 0])
        win_rate = (winning_trades / len(closed_trades) * 100) if closed_trades else 0
        
        return {
            'total_trades': len(self.all_trades),
            'open_trades': len(open_trades),
            'closed_trades': len(closed_trades),
            'total_pnl': total_pnl,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate
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
            if subject is None:
                pnl_emoji = "📈" if summary['total_pnl'] >= 0 else "📉"
                subject = f"{pnl_emoji} Trade Summary - {summary['closed_trades']} Closed, {summary['open_trades']} Open"
            
            # Build email body
            body = f"""📊 TRADE SUMMARY REPORT

📈 Performance Overview:
- Total Trades: {summary['total_trades']}
- Closed Trades: {summary['closed_trades']}
- Open Trades: {summary['open_trades']}
- Total P&L: ${summary['total_pnl']:.2f}
- Win Rate: {summary['win_rate']:.1f}%
- Winning Trades: {summary['winning_trades']}
- Losing Trades: {summary['losing_trades']}

"""
            
            # Add closed trades breakdown if any exist
            if summary['closed_trades'] > 0:
                body += "📋 Closed Trades Breakdown:\n"
                closed_trades = [t for t in self.all_trades if not t.is_open]
                
                # Sort by exit time (most recent first)
                closed_trades.sort(key=lambda x: x.exit_time if x.exit_time else datetime.min, reverse=True)
                
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
