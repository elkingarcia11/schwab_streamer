"""
This file contains the SchwabStreamerClient class, which is used to connect to the Schwab Streaming API and stream data to the aggregator.

It also contains the parse_chart_data function, which is used to parse the CHART_EQUITY data into a dictionary.

It also contains the parse_option_streaming_data function, which is used to parse the LEVELONE_OPTIONS data into a dictionary.

It also contains the get_user_preferences function, which is used to get the user preferences from the Schwab API.
"""
from schwab_auth import SchwabAuth
from typing import List
import json
import time
import threading
import httpx
import websocket
import requests
from data_manager import MarketDataFetcher
from indicator_generator import IndicatorGenerator
from backtest_generator import BacktestStrategy
import pandas as pd
import os
import time

class SchwabStreamerClient:
    """Main client for Schwab Streaming API - OHLCV Data Collection"""
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.auth = SchwabAuth()
        self.ws = None
        self.running = False
        self.connected = False
        self.request_id = 1
        self.subscriptions = {}
        self.message_handlers = {}
        self.streamer_info = None  # Store streamer info for subscription requests
        self.tracked_symbols = self.load_symbols_from_file()
        self.timeframes = ["5m","10m","15m"]
        
        # Initialize the new modules
        self.market_data_fetcher = MarketDataFetcher()
        self.indicator_generator = IndicatorGenerator()
        self.backtest_strategy = BacktestStrategy()
        
        # Trade tracking
        self.open_trades = {}  # symbol -> {signal, entry_time, entry_price, candle_data}
        self.closed_trades = []  # List of completed trades
        self.daily_email_sent = False  # Track if 4pm email was sent today
        
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Get user preferences and store SchwabClientCustomerId
        self.user_preferences = self.get_user_preferences()
        if not self.user_preferences or 'streamerInfo' not in self.user_preferences:
            raise Exception("Could not get user preferences")
        
        # Store streamer info
        self.streamer_info = self.user_preferences['streamerInfo'][0]
        self.schwab_client_customer_id = self.streamer_info.get('schwabClientCustomerId')
        if not self.schwab_client_customer_id:
            raise Exception("Could not get SchwabClientCustomerId from user preferences")
        
        # Actual CHART_EQUITY field mappings based on observed raw data
        # Fields 2-6 are OHLCV (Open, High, Low, Close, Volume) and field 7 is timestamp
        self.chart_equity_fields = {
            0: 'key',        # Symbol (ticker symbol in upper case)
            1: 'sequence',   # Sequence - Identifies the candle minute
            2: 'open',       # Open Price - Opening price for the minute
            3: 'high',       # High Price - Highest price for the minute  
            4: 'low',        # Low Price - Chart's lowest price for the minute
            5: 'close',      # Close Price - Closing price for the minute
            6: 'volume',     # Volume - Total volume for the minute
            7: 'time',       # Chart Time - Milliseconds since Epoch
            8: 'chart_day'   # Chart Day
        }
    
    def load_symbols_from_file(self, filename: str = 'symbols_to_stream.txt') -> List[str]:
        """Load symbols to stream from text file"""
        try:
            with open(filename, 'r') as f:
                content = f.read().strip()
                symbols = [symbol.strip() for symbol in content.split(',') if symbol.strip()]
                return symbols
        except FileNotFoundError:
            print(f"❌ File {filename} not found")
            return []
        except Exception as e:
            print(f"❌ Error reading symbols file: {e}")
            return []
    
    def get_user_preferences(self):
        """Get user preferences using the SchwabAuth token"""
        try:
            # Get fresh token
            access_token = self.auth.get_access_token()
            if not access_token:
                raise Exception("Failed to get valid access token")
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }
            
            url = "https://api.schwabapi.com/trader/v1/userPreference"
            
            with httpx.Client() as client:
                response = client.get(url, headers=headers)
                
            if response.status_code != 200:
                raise Exception(f"User preferences request failed: {response.status_code} - {response.text}")
            
            return response.json()
            
        except Exception as e:
            if self.debug:
                print(f"❌ Error getting user preferences: {e}")
            raise

    def parse_chart_data(self, content_item: dict) -> dict:
        """Parse CHART_EQUITY message content using field mappings"""
        try:
            # Extract the fields array from the content
            fields = content_item.get('fields', [])
            if not fields or len(fields) < 9:  # We expect at least 9 fields
                return None

            # Map the fields according to the Schwab API specification
            parsed = {
                'symbol': fields[0],          # key - Ticker symbol
                'sequence': fields[1],        # sequence - Identifies the candle minute
                'open': float(fields[2]),     # Open Price
                'high': float(fields[3]),     # High Price
                'low': float(fields[4]),      # Low Price
                'close': float(fields[5]),    # Close Price
                'volume': float(fields[6]),   # Volume
                'time': int(fields[7]),       # Chart Time (milliseconds since Epoch)
                'chart_day': int(fields[8])   # Chart Day
            }

            return parsed

        except Exception as e:
            print(f"❌ Error parsing CHART_EQUITY data: {e}")
            return None

    def process_new_candle(self, symbol: str, candle: dict) -> None:
        """
        Process a new 1m candle with immediate trade processing and email notifications
        """
        try:
            # Convert timestamp to datetime for comparison
            candle_time = pd.Timestamp(candle['timestamp'], unit='ms', tz='UTC')
            current_time = pd.Timestamp.now(tz='UTC')
            
            # Skip incomplete candles (from current minute)
            if candle_time.floor('min') >= current_time.floor('min'):
                if self.debug:
                    print(f"Skipping incomplete candle for {symbol} at {candle_time}")
                return

            print(f"📊 Processing 1m candle for {symbol} at {candle_time.strftime('%Y-%m-%d %H:%M:%S')} - OHLCV: ${candle['open']:.2f}/${candle['high']:.2f}/${candle['low']:.2f}/${candle['close']:.2f} Vol:{candle['volume']:,}")

            # Step 1: Save the candle data to CSV
            self.save_candle_to_csv(symbol, candle)
            
            # Step 2: Generate indicators using 1m configuration
            csv_file = f"data/{symbol}_1m.csv"
            
            if os.path.exists(csv_file):
                # Check how many data points we have
                df = pd.read_csv(csv_file)
                data_points = len(df)
                print(f"🔍 {symbol}: {data_points} data points available for indicator calculation")
                
                if data_points < 40:  # Need at least 40 points for MACD with slow EMA 39
                    print(f"⚠️  {symbol}: Need at least 40 data points for indicators, currently have {data_points}")
                    return
                
                # Generate indicators with 1m specific periods
                indicator_gen = IndicatorGenerator(
                    ema_periods=[5],
                    vwma_periods=[16],
                    roc_periods=[6],
                    fast_emas=[15],
                    slow_emas=[39],
                    signal_emas=[11]
                )
                
                print(f"📊 Generating indicators for {symbol}...")
                indicator_gen.generate_indicators(symbol, "1m")
                print(f"✅ Indicators generated for {symbol}")
                
                # Step 3: Immediately process trades using BacktestStrategy  
                backtest = BacktestStrategy(
                    ema_periods=[5],
                    vwma_periods=[16], 
                    roc_periods=[6],
                    fast_emas=[15],
                    slow_emas=[39],
                    signal_emas=[11]
                )
                
                # Process the latest signal and track trades
                latest_signal = self.analyze_latest_signal(symbol, "1m")
                
                if latest_signal:
                    print(f"🎯 Trade signal detected for {symbol}: {latest_signal}")
                    
                    # Track the trade
                    self.track_trade(symbol, latest_signal, candle)
                    
                    # Send email notification with trade context
                    trade_context = None
                    if latest_signal == "SELL" and symbol in self.open_trades:
                        # Get the open trade info for SELL emails
                        trade_context = self.open_trades[symbol]
                    self.send_trade_email(symbol, latest_signal, candle, trade_context)
                
                if self.debug:
                    print(f"✅ Completed processing for {symbol} 1m candle")

        except Exception as e:
            print(f"❌ Error processing new candle for {symbol}: {str(e)}")

    def analyze_latest_signal(self, symbol: str, timeframe: str) -> str:
        """
        Analyze the latest candle for trade signals
        Returns signal type: 'BUY', 'SELL', or None
        """
        try:
            csv_file = f"data/{symbol}_1m.csv"
            if not os.path.exists(csv_file):
                return None
            
            df = pd.read_csv(csv_file)
            if len(df) < 2:  # Need at least 2 rows to compare
                return None
            
            # Get latest and previous rows
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            
            # Check if required indicators exist
            ema_col = f"ema_5"
            vwma_col = f"vwma_16"
            roc_col = f"roc_6"
            macd_line_col = f"macd_line_15_39"
            macd_signal_col = f"macd_signal_15_39_11"
            
            # Check if all required columns exist
            required_cols = [ema_col, vwma_col, roc_col, macd_line_col, macd_signal_col]
            if not all(col in df.columns for col in required_cols):
                return None
            
            # Check for valid values
            if any(pd.isna(latest[col]) for col in required_cols):
                return None
            
            # BUY Signal: All 3 conditions must be met
            ema_condition = latest[ema_col] > latest[vwma_col]
            roc_condition = latest[roc_col] > 0
            macd_condition = latest[macd_line_col] > latest[macd_signal_col]
            
            # Check previous conditions
            prev_ema_condition = previous[ema_col] > previous[vwma_col] if not pd.isna(previous[ema_col]) else False
            prev_roc_condition = previous[roc_col] > 0 if not pd.isna(previous[roc_col]) else False
            prev_macd_condition = previous[macd_line_col] > previous[macd_signal_col] if not pd.isna(previous[macd_line_col]) else False
            
            current_buy_signal = ema_condition and roc_condition and macd_condition
            previous_buy_signal = prev_ema_condition and prev_roc_condition and prev_macd_condition
            
            # NEW BUY signal (all 3 conditions met, wasn't active before)
            if current_buy_signal and not previous_buy_signal:
                return "BUY"
            
            # NEW SELL signal (any condition fails when all were previously met)
            if previous_buy_signal and not current_buy_signal:
                # Determine which condition(s) failed
                failed_conditions = []
                if not ema_condition and prev_ema_condition:
                    failed_conditions.append("EMA≤VWMA")
                if not roc_condition and prev_roc_condition:
                    failed_conditions.append("ROC≤0")
                if not macd_condition and prev_macd_condition:
                    failed_conditions.append("MACD≤Signal")
                
                if failed_conditions:
                    print(f"🔴 SELL signal triggered for {symbol}: {', '.join(failed_conditions)} failed")
                
                return "SELL"
            
            return None
            
        except Exception as e:
            print(f"❌ Error analyzing signal for {symbol}: {e}")
            return None

    def track_trade(self, symbol: str, signal: str, candle: dict) -> None:
        """
        Track open and closed trades - only 1 trade per symbol at a time
        """
        try:
            timestamp = pd.Timestamp(candle['timestamp'], unit='ms', tz='US/Eastern')
            
            if signal == "BUY":
                # Can only open a trade if there's no existing open trade for this symbol
                if symbol in self.open_trades:
                    print(f"⚠️  BUY signal for {symbol} but trade already open - ignoring")
                    return
                
                # Open new trade
                self.open_trades[symbol] = {
                    'signal': 'BUY',
                    'entry_time': timestamp,
                    'entry_price': candle['close'],
                    'entry_candle': candle.copy()
                }
                
                print(f"📈 Opened BUY trade for {symbol} at ${candle['close']:.2f}")
                
            elif signal == "SELL":
                # Can only close a trade if there's an existing open trade for this symbol
                if symbol not in self.open_trades:
                    print(f"⚠️  SELL signal for {symbol} but no open trade to close - ignoring")
                    return
                
                # Close existing trade
                self.close_trade(symbol, candle, "SELL")
                    
        except Exception as e:
            print(f"❌ Error tracking trade for {symbol}: {e}")

    def close_trade(self, symbol: str, exit_candle: dict, reason: str) -> None:
        """
        Close an open trade and record it
        """
        try:
            if symbol not in self.open_trades:
                return
                
            open_trade = self.open_trades[symbol]
            exit_timestamp = pd.Timestamp(exit_candle['timestamp'], unit='ms', tz='US/Eastern')
            
            # Calculate trade performance
            entry_price = open_trade['entry_price']
            exit_price = exit_candle['close']
            pnl = exit_price - entry_price
            pnl_percent = (pnl / entry_price) * 100
            
            # Duration
            duration = exit_timestamp - open_trade['entry_time']
            
            closed_trade = {
                'symbol': symbol,
                'entry_time': open_trade['entry_time'].strftime('%Y-%m-%d %H:%M:%S'),
                'exit_time': exit_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'entry_price': entry_price,
                'exit_price': exit_price,
                'pnl': pnl,
                'pnl_percent': pnl_percent,
                'duration_minutes': int(duration.total_seconds() / 60),
                'reason': reason,
                'entry_candle': open_trade['entry_candle'],
                'exit_candle': exit_candle
            }
            
            self.closed_trades.append(closed_trade)
            
            # Remove from open trades
            del self.open_trades[symbol]
            
            # Save to CSV
            self.save_trade_to_csv(closed_trade)
            
            print(f"📉 Closed trade for {symbol}: ${entry_price:.2f} -> ${exit_price:.2f} = {pnl_percent:.2f}% ({reason})")
            
        except Exception as e:
            print(f"❌ Error closing trade for {symbol}: {e}")

    def save_trade_to_csv(self, trade: dict) -> None:
        """
        Save closed trade to CSV file
        """
        try:
            csv_file = "data/closed_trades_1m.csv"
            
            # Create DataFrame for this trade
            trade_df = pd.DataFrame([{
                'symbol': trade['symbol'],
                'entry_time': trade['entry_time'],
                'exit_time': trade['exit_time'],
                'entry_price': trade['entry_price'],
                'exit_price': trade['exit_price'],
                'pnl': trade['pnl'],
                'pnl_percent': trade['pnl_percent'],
                'duration_minutes': trade['duration_minutes'],
                'reason': trade['reason']
            }])
            
            # Append to existing file or create new
            if os.path.exists(csv_file):
                trade_df.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                trade_df.to_csv(csv_file, index=False)
                
        except Exception as e:
            print(f"❌ Error saving trade to CSV: {e}")

    def send_trade_email(self, symbol: str, signal: str, candle: dict, trade_context: dict = None) -> None:
        """
        Send email notification for trade signal with improved formatting
        """
        try:
            from email_manager.email_manager import EmailManager
            
            email_manager = EmailManager()
            
            # Format email content
            timestamp = pd.Timestamp(candle['timestamp'], unit='ms', tz='US/Eastern').strftime('%Y-%m-%d %H:%M:%S ET')
            
            if signal == "BUY":
                # BUY Email - Show entry price
                subject = f"🟢 BUY Signal: {symbol} at ${candle['close']:.2f}"
                
                body = f"""
🚨 BUY SIGNAL DETECTED - {symbol}

📊 Trade Details:
- Symbol: {symbol}
- Action: BUY (Open Position)
- Entry Price: ${candle['close']:.2f}
- Timestamp: {timestamp}

📈 Current Candle:
- Open: ${candle['open']:.2f}
- High: ${candle['high']:.2f}
- Low: ${candle['low']:.2f}
- Close: ${candle['close']:.2f}
- Volume: {candle['volume']:,}

🎯 Signal Conditions (ALL met):
✅ EMA 5 > VWMA 16
✅ ROC 6 > 0
✅ MACD Line > MACD Signal

📋 Configuration:
- Timeframe: 1 minute
- EMA: 5 periods
- VWMA: 16 periods
- ROC: 6 periods
- MACD: Fast 15, Slow 39, Signal 11

🔄 Status: Position opened, monitoring for SELL signal
"""
            
            elif signal == "SELL":
                # SELL Email - Show entry price, exit price, and P&L
                if trade_context:
                    entry_price = trade_context['entry_price']
                    entry_time = trade_context['entry_time'].strftime('%Y-%m-%d %H:%M:%S ET')
                    exit_price = candle['close']
                    pnl = exit_price - entry_price
                    pnl_percent = (pnl / entry_price) * 100
                    
                    # Duration calculation
                    exit_time = pd.Timestamp(candle['timestamp'], unit='ms', tz='US/Eastern')
                    duration = exit_time - trade_context['entry_time']
                    duration_str = f"{int(duration.total_seconds() / 60)} minutes"
                    
                    pnl_emoji = "📈" if pnl >= 0 else "📉"
                    pnl_sign = "+" if pnl >= 0 else ""
                    
                    subject = f"🔴 SELL Signal: {symbol} {pnl_emoji} {pnl_sign}${pnl:.2f} ({pnl_percent:+.1f}%)"
                    
                    body = f"""
🚨 SELL SIGNAL DETECTED - {symbol}

📊 Trade Results:
- Symbol: {symbol}
- Action: SELL (Close Position)
- Entry Price: ${entry_price:.2f}
- Exit Price: ${exit_price:.2f}
- P&L: {pnl_emoji} {pnl_sign}${pnl:.2f} ({pnl_percent:+.1f}%)

⏰ Trade Timeline:
- Entry Time: {entry_time}
- Exit Time: {timestamp}
- Duration: {duration_str}

📈 Exit Candle:
- Open: ${candle['open']:.2f}
- High: ${candle['high']:.2f}
- Low: ${candle['low']:.2f}
- Close: ${candle['close']:.2f}
- Volume: {candle['volume']:,}

🎯 Signal Trigger:
❌ At least one condition failed (EMA≤VWMA OR ROC≤0 OR MACD≤Signal)

📋 Configuration:
- Timeframe: 1 minute
- Trade Logic: SELL when ANY condition fails

🔄 Status: Position closed, monitoring for new BUY signal
"""
                else:
                    # Fallback if no trade context
                    subject = f"🔴 SELL Signal: {symbol} at ${candle['close']:.2f}"
                    body = f"""
🚨 SELL SIGNAL DETECTED - {symbol}

📊 Trade Details:
- Symbol: {symbol}
- Action: SELL
- Price: ${candle['close']:.2f}
- Timestamp: {timestamp}

Note: No open position found to close.
"""
            
            success = email_manager._send_email(subject, body)
            if success:
                print(f"📧 Email sent for {symbol} {signal} signal")
            else:
                print(f"❌ Failed to send email for {symbol} {signal} signal")
            
        except Exception as e:
            print(f"❌ Error sending email for {symbol}: {e}")

    def send_daily_trades_email(self) -> None:
        """
        Send daily summary of closed trades at 4pm
        """
        try:
            from email_manager.email_manager import EmailManager
            
            csv_file = "data/closed_trades_1m.csv"
            current_date = pd.Timestamp.now(tz='US/Eastern').strftime('%Y-%m-%d')
            
            # Check if there are any trades today
            if not os.path.exists(csv_file):
                print("📧 No trades file found for daily email")
                return
            
            df = pd.read_csv(csv_file)
            if df.empty:
                print("📧 No trades found for daily email")
                return
            
            # Filter trades for today
            df['entry_date'] = pd.to_datetime(df['entry_time']).dt.date
            today_trades = df[df['entry_date'] == pd.to_datetime(current_date).date()]
            
            if today_trades.empty:
                print("📧 No trades for today, skipping daily email")
                return
            
            # Calculate summary statistics
            total_trades = len(today_trades)
            winning_trades = len(today_trades[today_trades['pnl'] > 0])
            losing_trades = len(today_trades[today_trades['pnl'] < 0])
            total_pnl = today_trades['pnl'].sum()
            avg_pnl_percent = today_trades['pnl_percent'].mean()
            win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
            
            # Create email content
            email_manager = EmailManager()
            
            subject = f"📊 Daily Trading Summary - {current_date}"
            
            # Create trades table
            trades_table = today_trades[['symbol', 'entry_time', 'exit_time', 'entry_price', 'exit_price', 'pnl', 'pnl_percent', 'duration_minutes', 'reason']].to_string(index=False, float_format='%.2f')
            
            body = f"""
Daily Trading Summary for {current_date}

PERFORMANCE OVERVIEW:
- Total Trades: {total_trades}
- Winning Trades: {winning_trades}
- Losing Trades: {losing_trades}
- Win Rate: {win_rate:.1f}%
- Total P&L: ${total_pnl:.2f}
- Average P&L: {avg_pnl_percent:.2f}%

DETAILED TRADES:
{trades_table}

OPEN TRADES:
{self.format_open_trades()}

Timeframe: 1m
Indicator Config: EMA 5, VWMA 16, ROC 6, MACD 15/39/11
Processing Mode: Real-time Streaming

This email was automatically sent at 4:00 PM ET.
"""
            
                         # Send email (note: CSV attachment functionality would need to be added to EmailManager)
            email_manager.send_email(subject, body)
            
            print(f"📧 Daily trades email sent: {total_trades} trades, ${total_pnl:.2f} P&L")
            
        except Exception as e:
            print(f"❌ Error sending daily trades email: {e}")

    def format_open_trades(self) -> str:
        """
        Format open trades for email display
        """
        if not self.open_trades:
            return "No open trades"
        
        lines = []
        for symbol, trade in self.open_trades.items():
            current_time = pd.Timestamp.now(tz='US/Eastern')
            duration = current_time - trade['entry_time']
            lines.append(f"- {symbol}: BUY at ${trade['entry_price']:.2f} ({trade['entry_time'].strftime('%H:%M:%S')}) - {int(duration.total_seconds()/60)}m ago")
        
        return "\n".join(lines)

    def backfill_todays_data(self) -> None:
        """
        Backfill today's historical 1m data for all tracked symbols using market hours
        """
        try:
            # Get today's date in ET timezone
            et_tz = pd.Timestamp.now(tz='US/Eastern')
            current_date = et_tz.strftime('%Y-%m-%d')
            
            # Calculate market hours timestamps (9:30 AM ET for start, use fixed end date from working curl)
            market_open = pd.Timestamp(f"{current_date} 09:30:00", tz='US/Eastern')
            
            # Convert to milliseconds epoch
            start_time_ms = int(market_open.timestamp() * 1000)
            end_time_ms = 1750233600000  # Fixed end date from working curl example
            
            print(f"🔄 Backfilling 1m data for {current_date} (Market Hours)...")
            print(f"   📅 Start: {market_open} ({start_time_ms})")
            print(f"   📅 End: Fixed timestamp ({end_time_ms})")
            
            # Load symbols to backfill
            symbols = self.tracked_symbols
            print(f"📊 Symbols to backfill: {', '.join(symbols)}")
            
            total_success = 0
            total_failed = 0
            
            for symbol in symbols:
                print(f"\n📈 Backfilling {symbol}...")
                
                success = self.backfill_symbol_market_hours(
                    symbol=symbol,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms
                )
                
                if success:
                    total_success += 1
                    
                    # Generate indicators for this symbol
                    print(f"🧮 Generating indicators for {symbol}...")
                    indicator_gen = IndicatorGenerator(
                        ema_periods=[5],
                        vwma_periods=[16],
                        roc_periods=[6],
                        fast_emas=[15],
                        slow_emas=[39],
                        signal_emas=[11]
                    )
                    
                    indicator_gen.generate_indicators(symbol, "1m")
                    
                    # Process historical trades for this symbol
                    print(f"🎯 Processing historical trades for {symbol}...")
                    self.process_historical_trades(symbol)
                    
                    print(f"✅ Backfill complete for {symbol}")
                else:
                    total_failed += 1
                    print(f"❌ Backfill failed for {symbol}")
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
            
            print(f"\n📋 Backfill Summary:")
            print(f"   ✅ Successful: {total_success}")
            print(f"   ❌ Failed: {total_failed}")
            print(f"   📊 Ready for live streaming!")
            
            # Send bootstrap completion email with current open positions
            self.send_bootstrap_completion_email()
            
        except Exception as e:
            print(f"❌ Error during backfill: {e}")

    def send_bootstrap_completion_email(self) -> None:
        """
        Send email notification when bootstrap is complete with current open positions
        """
        try:
            from email_manager.email_manager import EmailManager
            
            current_time = pd.Timestamp.now(tz='US/Eastern')
            
            # Get current open positions
            open_positions_text = self.format_open_trades()
            if not open_positions_text:
                open_positions_text = "No open positions"
            
            subject = f"🚀 Schwab Streamer Bootstrap Complete - {current_time.strftime('%Y-%m-%d %H:%M:%S ET')}"
            
            body = f"""
📊 SCHWAB STREAMER BOOTSTRAP COMPLETED

⏰ Time: {current_time.strftime('%Y-%m-%d %H:%M:%S ET')}

📈 Configuration:
   • Timeframe: 1m only
   • EMA: 5 periods
   • VWMA: 16 periods  
   • ROC: 6 periods
   • MACD: Fast 15, Slow 39, Signal 11
   • Trade Logic: BUY when ALL conditions met, SELL when ANY fails

📊 Current Open Positions:
{open_positions_text}

🔄 Status: Historical data loaded, indicators calculated
🎯 Ready: Live streaming and trade processing active
📧 Alerts: Will send email on every new trade signal

---
This is an automated message from Schwab Streamer.
            """.strip()
            
            email_manager = EmailManager()
            success = email_manager._send_email(subject, body)
            
            if success:
                print("📧 Bootstrap completion email sent successfully")
            else:
                print("❌ Failed to send bootstrap completion email")
                
        except Exception as e:
            print(f"❌ Error sending bootstrap completion email: {e}")

    def backfill_symbol_market_hours(self, symbol: str, start_time_ms: int, end_time_ms: int) -> bool:
        """
        Backfill a single symbol using direct API call with market hours timestamps
        """
        try:
            # Validate credentials first
            if not self.auth.validate_credentials():
                print("❌ Schwab credentials validation failed")
                return False

            headers = self.auth.get_auth_headers()
            if not headers:
                print("❌ No valid authentication headers available")
                return False
            
            # Modify headers to match curl example exactly
            headers = {
                'accept': 'application/json',
                'Authorization': headers['Authorization']
            }

            url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
            
            params = {
                'symbol': symbol.upper(),
                'periodType': 'day',
                'period': 1,
                'frequencyType': 'minute',
                'frequency': 1,
                'startDate': start_time_ms,
                'endDate': end_time_ms,
                'needExtendedHoursData': 'false',
                'needPreviousClose': 'false'
            }

            print(f"🔍 API URL: {url}")
            print(f"🔍 API Params: {params}")
            print(f"🔍 Headers: {headers}")
            
            # Construct full URL for debugging
            full_url = requests.Request('GET', url, params=params).prepare().url
            print(f"🔍 Full URL: {full_url}")

            response = requests.get(url, headers=headers, params=params, timeout=30)
            time.sleep(1)  # Rate limiting

            if response.status_code == 200:
                data = response.json()

                if 'candles' in data and data['candles']:
                    candles = data['candles']
                    print(f"✅ Retrieved {len(candles)} candles from Schwab API")
                    
                    # Convert candles to DataFrame with proper structure
                    df_data = []
                    for candle in candles:
                        df_data.append({
                            'timestamp': candle.get('datetime', 0),
                            'datetime': pd.Timestamp(candle.get('datetime', 0), unit='ms', tz='US/Eastern').strftime('%Y-%m-%d %H:%M:%S'),
                            'open': candle.get('open', 0),
                            'high': candle.get('high', 0),
                            'low': candle.get('low', 0),
                            'close': candle.get('close', 0),
                            'volume': candle.get('volume', 0)
                        })

                    new_df = pd.DataFrame(df_data)
                    new_df = new_df.sort_values('timestamp').drop_duplicates(subset=['timestamp'])

                    # Save to CSV file
                    csv_filename = f"data/{symbol}_1m.csv"
                    if os.path.exists(csv_filename):
                        print(f"📂 Found existing data file for {symbol}_1m")
                        existing_df = pd.read_csv(csv_filename)
                        existing_df['timestamp'] = pd.to_numeric(existing_df['timestamp'])

                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                        combined_df = combined_df.sort_values('timestamp').drop_duplicates(subset=['timestamp'])

                        print(f"📊 Combined data: {len(existing_df)} existing + {len(new_df)} new = {len(combined_df)} total records")
                        combined_df.to_csv(csv_filename, index=False)
                        print(f"💾 Updated {csv_filename} with combined data")
                    else:
                        new_df.to_csv(csv_filename, index=False)
                        print(f"💾 Created new file {csv_filename} with {len(new_df)} records")

                    return True
                else:
                    print("📊 No candle data found in API response")
                    return False
            else:
                print(f"❌ API request failed: {response.status_code}")
                if response.text:
                    print(f"Full Response: {response.text}")
                return False

        except Exception as e:
            print(f"❌ Error backfilling {symbol}: {e}")
            return False

    def process_historical_trades(self, symbol: str) -> None:
        """
        Process historical trade signals for a symbol after backfill
        """
        try:
            csv_file = f"data/{symbol}_1m.csv"
            if not os.path.exists(csv_file):
                return
            
            df = pd.read_csv(csv_file)
            if len(df) < 40:  # Still need minimum data
                return
            
            print(f"🔍 Analyzing historical signals for {symbol}...")
            
            # Check each row for signal changes (starting from row 2 to have previous data)
            signals_found = 0
            
            for i in range(1, len(df)):
                # Create mock candle for this historical point
                row = df.iloc[i]
                mock_candle = {
                    'timestamp': row['timestamp'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                }
                
                # Check for signal at this point by temporarily modifying the CSV to only include data up to this point
                temp_df = df.iloc[:i+1]
                temp_csv = f"data/{symbol}_1m_temp.csv"
                temp_df.to_csv(temp_csv, index=False)
                
                # Analyze signal at this point
                signal = self.analyze_historical_signal(symbol, temp_csv)
                
                if signal:
                    signals_found += 1
                    timestamp_str = pd.Timestamp(row['timestamp'], unit='ms', tz='US/Eastern').strftime('%H:%M:%S')
                    print(f"   📍 {signal} signal at {timestamp_str}: ${row['close']:.2f}")
                    
                    # Track the historical trade (but don't send emails for historical data)
                    self.track_historical_trade(symbol, signal, mock_candle)
                
                # Clean up temp file
                if os.path.exists(temp_csv):
                    os.remove(temp_csv)
            
            print(f"   🎯 Found {signals_found} historical signals for {symbol}")
            
        except Exception as e:
            print(f"❌ Error processing historical trades for {symbol}: {e}")

    def analyze_historical_signal(self, symbol: str, csv_file: str) -> str:
        """
        Analyze signal for historical data using a specific CSV file
        """
        try:
            if not os.path.exists(csv_file):
                return None
            
            df = pd.read_csv(csv_file)
            if len(df) < 2:
                return None
            
            # Get latest and previous rows
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            
            # Check if required indicators exist
            ema_col = f"ema_5"
            vwma_col = f"vwma_16"
            roc_col = f"roc_6"
            macd_line_col = f"macd_line_15_39"
            macd_signal_col = f"macd_signal_15_39_11"
            
            # Check if all required columns exist
            required_cols = [ema_col, vwma_col, roc_col, macd_line_col, macd_signal_col]
            if not all(col in df.columns for col in required_cols):
                return None
            
            # Check for valid values
            if any(pd.isna(latest[col]) or pd.isna(previous[col]) for col in required_cols):
                return None
            
            # Current and previous conditions
            ema_condition = latest[ema_col] > latest[vwma_col]
            roc_condition = latest[roc_col] > 0
            macd_condition = latest[macd_line_col] > latest[macd_signal_col]
            
            prev_ema_condition = previous[ema_col] > previous[vwma_col]
            prev_roc_condition = previous[roc_col] > 0
            prev_macd_condition = previous[macd_line_col] > previous[macd_signal_col]
            
            current_buy_signal = ema_condition and roc_condition and macd_condition
            previous_buy_signal = prev_ema_condition and prev_roc_condition and prev_macd_condition
            
            # NEW BUY signal
            if current_buy_signal and not previous_buy_signal:
                return "BUY"
            
            # NEW SELL signal
            if previous_buy_signal and not current_buy_signal:
                return "SELL"
            
            return None
            
        except Exception as e:
            return None

    def track_historical_trade(self, symbol: str, signal: str, candle: dict) -> None:
        """
        Track historical trades (same logic as live but no email notifications)
        """
        try:
            timestamp = pd.Timestamp(candle['timestamp'], unit='ms', tz='US/Eastern')
            
            if signal == "BUY":
                # Can only open a trade if there's no existing open trade for this symbol
                if symbol in self.open_trades:
                    return  # Skip if trade already open
                
                # Open new trade
                self.open_trades[symbol] = {
                    'signal': 'BUY',
                    'entry_time': timestamp,
                    'entry_price': candle['close'],
                    'entry_candle': candle.copy()
                }
                
            elif signal == "SELL":
                # Can only close a trade if there's an existing open trade for this symbol
                if symbol not in self.open_trades:
                    return  # Skip if no trade to close
                
                # Close existing trade (historical, so no email)
                self.close_historical_trade(symbol, candle, "SELL")
                    
        except Exception as e:
            print(f"❌ Error tracking historical trade for {symbol}: {e}")

    def close_historical_trade(self, symbol: str, exit_candle: dict, reason: str) -> None:
        """
        Close a historical trade (same as live but no email)
        """
        try:
            if symbol not in self.open_trades:
                return
                
            open_trade = self.open_trades[symbol]
            exit_timestamp = pd.Timestamp(exit_candle['timestamp'], unit='ms', tz='US/Eastern')
            
            # Calculate trade performance
            entry_price = open_trade['entry_price']
            exit_price = exit_candle['close']
            pnl = exit_price - entry_price
            pnl_percent = (pnl / entry_price) * 100
            
            # Duration
            duration = exit_timestamp - open_trade['entry_time']
            
            closed_trade = {
                'symbol': symbol,
                'entry_time': open_trade['entry_time'].strftime('%Y-%m-%d %H:%M:%S'),
                'exit_time': exit_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'entry_price': entry_price,
                'exit_price': exit_price,
                'pnl': pnl,
                'pnl_percent': pnl_percent,
                'duration_minutes': int(duration.total_seconds() / 60),
                'reason': reason,
                'entry_candle': open_trade['entry_candle'],
                'exit_candle': exit_candle
            }
            
            self.closed_trades.append(closed_trade)
            
            # Remove from open trades
            del self.open_trades[symbol]
            
            # Save to CSV
            self.save_trade_to_csv(closed_trade)
            
        except Exception as e:
            print(f"❌ Error closing historical trade for {symbol}: {e}")

    def save_candle_to_csv(self, symbol: str, candle: dict) -> None:
        """
        Save a single candle to CSV file in market_data_fetcher format
        """
        try:
            # Convert candle to DataFrame format expected by market_data_fetcher
            # Convert timestamp to Eastern Time for both timestamp and datetime columns
            dt_eastern = pd.to_datetime(candle['timestamp'], unit='ms', utc=True).tz_convert('US/Eastern')
            # Convert back to milliseconds timestamp in Eastern Time
            eastern_timestamp_ms = int(dt_eastern.timestamp() * 1000)
            
            candle_data = {
                'timestamp': eastern_timestamp_ms,
                'datetime': dt_eastern.strftime('%Y-%m-%d %H:%M:%S'),
                'open': candle['open'],
                'high': candle['high'],
                'low': candle['low'],
                'close': candle['close'],
                'volume': candle['volume']
            }
            
            new_df = pd.DataFrame([candle_data])
            
            # Save to 1-minute CSV file
            csv_filename = f"data/{symbol}_1m.csv"
            if os.path.exists(csv_filename):
                # Read existing data and append
                existing_df = pd.read_csv(csv_filename)
                existing_df['timestamp'] = pd.to_numeric(existing_df['timestamp'])
                
                # Check if this Eastern timestamp already exists
                if eastern_timestamp_ms not in existing_df['timestamp'].values:
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    combined_df = combined_df.sort_values('timestamp').drop_duplicates(subset=['timestamp'])
                    combined_df.to_csv(csv_filename, index=False)
                    if self.debug:
                        print(f"Appended candle to {csv_filename}")
                else:
                    if self.debug:
                        print(f"Candle already exists in {csv_filename}")
            else:
                # Create new file
                new_df.to_csv(csv_filename, index=False)
                if self.debug:
                    print(f"Created new file {csv_filename}")
                    
        except Exception as e:
            print(f"❌ Error saving candle to CSV for {symbol}: {str(e)}")

    def bootstrap_for_streaming(self) -> None:
        """
        Bootstrap the system for streaming by:
        1. Fetching historical data (5m,10m,15m from 2025-01-01, 1m today only)
        2. Calculating indicators with specific periods for each timeframe
        3. Processing historical signals to determine open trades at market open
        """
        try:
            print("🚀 Starting bootstrap process...")
            
            # Define timeframe-specific indicator periods
            timeframe_configs = {
                "5m": {
                    "ema_periods": [7],
                    "vwma_periods": [6], 
                    "roc_periods": [11],
                    "fast_emas": [21],
                    "slow_emas": [37],
                    "signal_emas": [15],
                    "start_date": "2025-01-01"
                },
                "10m": {
                    "ema_periods": [9],
                    "vwma_periods": [5],
                    "roc_periods": [10], 
                    "fast_emas": [16],
                    "slow_emas": [31],
                    "signal_emas": [10],
                    "start_date": "2025-01-01"
                },
                "15m": {
                    "ema_periods": [6],
                    "vwma_periods": [4],
                    "roc_periods": [7],
                    "fast_emas": [14], 
                    "slow_emas": [30],
                    "signal_emas": [10],
                    "start_date": "2025-01-01"
                },
                "1m": {
                    "ema_periods": [7],  # Default for 1m
                    "vwma_periods": [17],
                    "roc_periods": [8],
                    "fast_emas": [12],
                    "slow_emas": [26],
                    "signal_emas": [9],
                    "start_date": pd.Timestamp.now(tz='US/Eastern').strftime('%Y-%m-%d')  # Today only
                }
            }
            
            today = pd.Timestamp.now(tz='US/Eastern').strftime('%Y-%m-%d')
            
            # Step 1: Fetch historical data for each symbol and timeframe
            print("📡 Fetching historical data...")
            for symbol in self.tracked_symbols:
                print(f"\n📊 Processing {symbol}...")
                
                for timeframe, config in timeframe_configs.items():
                    timeframe_minutes = int(timeframe.replace('m', ''))
                    start_date = config["start_date"]
                    
                    print(f"  📈 Fetching {timeframe} data from {start_date} to {today}...")
                    
                    # Fetch data using MarketDataFetcher
                    success = self.market_data_fetcher.get_price_history_from_schwab(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=today,
                        interval_to_fetch=timeframe_minutes
                    )
                    
                    if not success:
                        print(f"❌ Failed to fetch {timeframe} data for {symbol}")
                        continue
                    
                    # Step 2: Generate indicators with timeframe-specific periods
                    print(f"  🔢 Generating indicators for {symbol} {timeframe}...")
                    indicator_gen = IndicatorGenerator(
                        ema_periods=config["ema_periods"],
                        vwma_periods=config["vwma_periods"],
                        roc_periods=config["roc_periods"],
                        fast_emas=config["fast_emas"],
                        slow_emas=config["slow_emas"],
                        signal_emas=config["signal_emas"]
                    )
                    
                    indicator_gen.generate_indicators(symbol, timeframe)
                    
                    # Step 3: Process signals using BacktestStrategy to determine current position
                    print(f"  📊 Processing signals for {symbol} {timeframe}...")
                    backtest = BacktestStrategy(
                        ema_periods=config["ema_periods"],
                        vwma_periods=config["vwma_periods"], 
                        roc_periods=config["roc_periods"],
                        fast_emas=config["fast_emas"],
                        slow_emas=config["slow_emas"],
                        signal_emas=config["signal_emas"]
                    )
                    
                    # Analyze the latest data to determine if we should have an open position
                    open_position = self.analyze_current_position(symbol, timeframe, config)
                    
                    if open_position:
                        print(f"  🟢 {symbol} {timeframe}: OPEN POSITION detected")
                    else:
                        print(f"  ⚪ {symbol} {timeframe}: No open position")
            
            print("\n✅ Bootstrap process completed!")
            print("📧 Sending bootstrap summary...")
            self.send_bootstrap_summary()
            
        except Exception as e:
            print(f"❌ Error during bootstrap: {str(e)}")

    def analyze_current_position(self, symbol: str, timeframe: str, config: dict) -> bool:
        """
        Analyze the latest data to determine if we should have an open position
        Returns True if position should be open, False otherwise
        """
        try:
            # Read the data file
            csv_file = f"data/{symbol}_{timeframe}.csv"
            if not os.path.exists(csv_file):
                print(f"❌ No data file found: {csv_file}")
                return False
            
            df = pd.read_csv(csv_file)
            if df.empty:
                print(f"❌ Empty data file: {csv_file}")
                return False
            
            # Get the latest row with complete indicators
            latest_row = df.iloc[-1]
            
            # Check if indicators exist (they should after running generate_indicators)
            ema_col = f"ema_{config['ema_periods'][0]}"
            vwma_col = f"vwma_{config['vwma_periods'][0]}"
            roc_col = f"roc_{config['roc_periods'][0]}"
            macd_line_col = f"macd_line_{config['fast_emas'][0]}_{config['slow_emas'][0]}"
            macd_signal_col = f"macd_signal_{config['fast_emas'][0]}_{config['slow_emas'][0]}_{config['signal_emas'][0]}"
            
            # Check if all required columns exist
            required_cols = [ema_col, vwma_col, roc_col, macd_line_col, macd_signal_col]
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                print(f"❌ Missing indicator columns in {csv_file}: {missing_cols}")
                return False
            
            # Check if indicators have valid values (not NaN)
            if pd.isna(latest_row[ema_col]) or pd.isna(latest_row[vwma_col]) or pd.isna(latest_row[roc_col]) or pd.isna(latest_row[macd_line_col]) or pd.isna(latest_row[macd_signal_col]):
                print(f"⚠️ Latest indicators contain NaN values for {symbol} {timeframe}")
                return False
            
            # Apply buy signal logic (same as BacktestStrategy)
            ema_condition = latest_row[ema_col] > latest_row[vwma_col]
            roc_condition = latest_row[roc_col] > 0
            macd_condition = latest_row[macd_line_col] > latest_row[macd_signal_col]
            
            # Buy signal: All 3 conditions must be met
            buy_signal = ema_condition and roc_condition and macd_condition
            
            if self.debug:
                print(f"    📊 {symbol} {timeframe} indicators:")
                print(f"      EMA({config['ema_periods'][0]}): {latest_row[ema_col]:.4f}")
                print(f"      VWMA({config['vwma_periods'][0]}): {latest_row[vwma_col]:.4f}")
                print(f"      ROC({config['roc_periods'][0]}): {latest_row[roc_col]:.4f}")
                print(f"      MACD Line: {latest_row[macd_line_col]:.4f}")
                print(f"      MACD Signal: {latest_row[macd_signal_col]:.4f}")
                print(f"      Conditions: EMA>{ema_condition} ROC>{roc_condition} MACD>{macd_condition}")
                print(f"      Buy Signal: {buy_signal}")
            
            return buy_signal
            
        except Exception as e:
            print(f"❌ Error analyzing position for {symbol} {timeframe}: {str(e)}")
            return False

    def send_bootstrap_summary(self) -> None:
        """
        Send a summary of the bootstrap process
        """
        try:
            total_symbols = len(self.tracked_symbols)
            total_timeframes = 4  # 1m, 5m, 10m, 15m
            total_combinations = total_symbols * total_timeframes
            
            print(f"\n📋 Bootstrap Summary:")
            print(f"  📊 Symbols processed: {total_symbols}")
            print(f"  ⏱️  Timeframes: 1m, 5m, 10m, 15m")
            print(f"  🔢 Total combinations: {total_combinations}")
            print(f"  📅 Historical data: 2025-01-01 to today (5m,10m,15m)")
            print(f"  📅 Today's data: {pd.Timestamp.now(tz='US/Eastern').strftime('%Y-%m-%d')} (1m)")
            print(f"  🎯 System ready for streaming!")
            
        except Exception as e:
            print(f"❌ Error sending bootstrap summary: {str(e)}")

    def start_with_bootstrap(self) -> None:
        """
        Start the streaming client with bootstrap process
        """
        try:
            print("🚀 Starting Schwab Streaming Client with Bootstrap...")
            
            # Step 1: Run bootstrap to prepare data and determine open positions
            self.bootstrap_for_streaming()
            
            # Step 2: Connect to streaming after bootstrap is complete
            print("\n🔗 Connecting to streaming...")
            self.connect()
            
        except Exception as e:
            print(f"❌ Error starting with bootstrap: {str(e)}")

    def on_message(self, ws, message):
        """Handle WebSocket messages"""
        try:
            current_time = pd.Timestamp.now(tz='US/Eastern')
            market_open = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = current_time.replace(hour=16, minute=1, second=0, microsecond=0)
            
            # Check for 4pm daily email (send once per day)
            four_pm = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
            if current_time >= four_pm and not self.daily_email_sent:
                self.send_daily_trades_email()
                self.daily_email_sent = True
            
            # Reset daily flag at midnight
            if current_time.hour == 0 and current_time.minute == 0 and self.daily_email_sent:
                self.daily_email_sent = False
            
            if current_time < market_open or current_time >= market_close:
                if self.debug:
                    print(f"🕐 Outside market hours ({current_time.strftime('%H:%M:%S ET')}), ignoring message")
                return
            
            data = json.loads(message)
            
            if self.debug:
                print(f"📨 Received message: {json.dumps(data, indent=2)}")
            
            # Handle different message types
            if isinstance(data, dict):
                if "response" in data:
                    # Login response
                    response_data = data["response"][0]
                    if response_data.get("command") == "LOGIN":
                        content = response_data.get("content", {})
                        code = content.get("code", -1)
                        if code == 0:
                            print("✅ WebSocket login successful")
                            msg = content.get("msg", "")
                            if "status=" in msg:
                                status = msg.split("status=")[1].split(";")[0] if ";" in msg else msg.split("status=")[1]
                                print(f"📊 Account status: {status}")
                            self.connected = True
                            # Subscribe to symbols after successful login
                            print("📊 Subscribing to symbols...")
                            self.subscribe_chart_data(self.tracked_symbols)
                        else:
                            print(f"❌ WebSocket login failed with code: {code}")
                            print(f"   Message: {content.get('msg', 'Unknown error')}")
                            self.connected = False
                
                elif "notify" in data:
                    # Heartbeat or other notifications
                    notify_data = data["notify"][0]
                    if notify_data.get("heartbeat"):
                        if self.debug:
                            print("💓 Heartbeat received")
                    elif notify_data.get("service") == "ADMIN":
                        content = notify_data.get("content", {})
                        if content.get("code") == 30:  # Empty subscription
                            print("⚠️ Empty subscription detected, resubscribing...")
                            self.subscribe_chart_data(self.tracked_symbols)
                
                elif "data" in data:
                    # Market data
                    for data_item in data["data"]:
                        service = data_item.get("service")
                        content = data_item.get("content", [])
                        
                        if service == "CHART_EQUITY" and content:
                            for candle_data in content:
                                # Parse the numeric key format
                                # 1: chart time
                                # 2: open price
                                # 3: high price
                                # 4: low price
                                # 5: close price
                                # 6: volume
                                # 7: timestamp
                                # 8: sequence number
                                formatted_candle = {
                                    'timestamp': int(candle_data.get('7', 0)),  # Chart Time (milliseconds since Epoch)
                                    'open': float(candle_data.get('2', 0)),     # Open Price
                                    'high': float(candle_data.get('3', 0)),     # High Price
                                    'low': float(candle_data.get('4', 0)),      # Low Price
                                    'close': float(candle_data.get('5', 0)),    # Close Price
                                    'volume': float(candle_data.get('6', 0))    # Volume
                                }
                                
                                symbol = candle_data.get('key')  # Symbol (ticker symbol in upper case)
                                if symbol in self.tracked_symbols:
                                    # Process new candle with the new workflow
                                    self.process_new_candle(symbol, formatted_candle)
                                    if self.debug:
                                        print(f"Processed new candle for {symbol}: {formatted_candle}")
                        
                        elif service == "LEVELONE_OPTIONS" and content:
                            for content_item in content:
                                print(f"\n🔍 PROCESSING LEVELONE_OPTIONS CONTENT:")
                                print(f"   📄 Raw JSON: {json.dumps(content_item, indent=4)}")
                                
                                # Show detailed field breakdown
                                print(f"   🗂️  Field Breakdown:")
                                option_fields = {
                                    'key': 'Option Symbol',
                                    '0': 'symbol',
                                    '1': 'description', 
                                    '2': 'bid_price',
                                    '3': 'ask_price',
                                    '4': 'last_price',
                                    '5': 'high_price',
                                    '6': 'low_price',
                                    '7': 'close_price',
                                    '8': 'total_volume',
                                    '9': 'open_interest',
                                    '10': 'volatility',
                                    '11': 'intrinsic_value',
                                    '12': 'expiration_year',
                                    '13': 'multiplier',
                                    '14': 'digits',
                                    '15': 'open_price',
                                    '16': 'bid_size',
                                    '17': 'ask_size',
                                    '18': 'last_size',
                                    '19': 'net_change',
                                    '20': 'strike_price',
                                    '21': 'contract_type',
                                    '22': 'underlying',
                                    '23': 'expiration_month',
                                    '24': 'deliverables',
                                    '25': 'time_value',
                                    '26': 'expiration_day',
                                    '27': 'days_to_expiration',
                                    '28': 'delta',
                                    '29': 'gamma',
                                    '30': 'theta',
                                    '31': 'vega',
                                    '32': 'rho',
                                    '33': 'security_status',
                                    '34': 'theoretical_value',
                                    '35': 'underlying_price',
                                    '36': 'uv_expiration_type',
                                    '37': 'mark_price',
                                    '38': 'quote_time',
                                    '39': 'trade_time',
                                    '40': 'exchange',
                                    '41': 'exchange_name',
                                    '42': 'last_trading_day',
                                    '43': 'settlement_type',
                                    '44': 'net_percent_change',
                                    '45': 'mark_price_net_change',
                                    '46': 'mark_price_percent_change',
                                    '47': 'implied_yield',
                                    '48': 'is_penny_pilot',
                                    '49': 'option_root',
                                    '50': 'week_52_high',
                                    '51': 'week_52_low',
                                    '52': 'indicative_ask_price',
                                    '53': 'indicative_bid_price',
                                    '54': 'indicative_quote_time',
                                    '55': 'exercise_type'
                                }
                                
                                for field_id, value in content_item.items():
                                    field_name = option_fields.get(field_id, f'unknown_field_{field_id}')
                                    print(f"      {field_id:>3} ({field_name:<20}): {value}")
                                
                                # Highlight key pricing fields
                                key_fields = ['key', '2', '3', '4', '20', '22', '35', '37']  # Symbol, Bid, Ask, Last, Strike, Underlying, Underlying Price, Mark
                                print(f"   🎯 Key Fields:")
                                for field_id in key_fields:
                                    if field_id in content_item:
                                        field_name = option_fields.get(field_id, field_id)
                                        value = content_item[field_id]
                                        print(f"      {field_name}: {value}")
                                
                                # Note: This section is for options processing, not chart equity
                        
        except Exception as e:
            print(f"❌ Message handling error: {str(e)}")
            print(f"   Raw message: {message}")
            raise

    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        print(f"❌ WebSocket error: {str(error)}")
        self.connected = False

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        print(f"WebSocket connection closed: {close_status_code} - {close_msg}")
        self.connected = False

    def on_open(self, ws):
        """Handle WebSocket connection open"""
        print("🔗 WebSocket connected, attempting login...")
        self.running = True
        self.login()  # Send login request immediately after connection

    def login(self):
        """Send login request to WebSocket"""
        try:
            # Get fresh token
            access_token = self.auth.get_access_token()
            if not access_token:
                raise Exception("Failed to get valid access token")

            # Prepare login request
            request = {
                "service": "ADMIN",
                "command": "LOGIN",
                "requestid": str(self.request_id),
                "SchwabClientCustomerId": self.schwab_client_customer_id,
                "SchwabClientCorrelId": self.streamer_info.get("schwabClientCorrelId", ""),
                "parameters": {
                    "Authorization": access_token,
                    "SchwabClientChannel": self.streamer_info.get("schwabClientChannel", ""),
                    "SchwabClientFunctionId": self.streamer_info.get("schwabClientFunctionId", "")
                }
            }

            if self.debug:
                print(f"📤 Sending login: {json.dumps(request, indent=2)}")

            # Send login request
            self.ws.send(json.dumps(request))
            self.request_id += 1

        except Exception as e:
            print(f"❌ Login error: {str(e)}")
            raise

    def connect(self):
        """Connect to WebSocket and start streaming"""
        try:
            # Get streamer info from user preferences
            if not self.streamer_info:
                raise Exception("No streamer info available")

            # Get WebSocket URL
            ws_url = self.streamer_info.get('streamerSocketUrl')
            if not ws_url:
                raise Exception("No WebSocket URL in streamer info")

            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )

            # Start WebSocket connection in a separate thread
            def run_ws():
                self.ws.run_forever()

            # Start the WebSocket thread
            ws_thread = threading.Thread(target=run_ws)
            ws_thread.daemon = True
            ws_thread.start()

            # Wait for connection and login
            timeout = 30  # Increased timeout to 30 seconds
            start_time = time.time()
            while not self.connected and time.time() - start_time < timeout:
                time.sleep(0.1)

            if not self.connected:
                raise Exception("Failed to connect and login to WebSocket within timeout")

            print("✅ Successfully connected and logged in to WebSocket")

            # Keep the main thread alive and handle simple reconnection
            while self.running:
                if not self.connected:
                    print("❌ Connection lost, attempting to reconnect...")
                    time.sleep(5)  # Wait 5 seconds before reconnecting
                    try:
                        self.connect()
                    except Exception as e:
                        print(f"❌ Reconnection failed: {e}")
                        time.sleep(10)  # Wait longer on failure
                else:
                    time.sleep(1)

        except Exception as e:
            print(f"❌ WebSocket error: {str(e)}")
            if self.ws:
                self.ws.close()
            raise

    def subscribe_chart_data(self, symbols: List[str]):
        """Subscribe to CHART_EQUITY data for the given symbols"""
        if not self.connected:
            print("❌ Not connected to WebSocket")
            return

        try:
            # Prepare the subscription request
            request = {
                "service": "CHART_EQUITY",
                "command": "SUBS",
                "requestid": self.request_id,
                "SchwabClientCustomerId": self.schwab_client_customer_id,
                "SchwabClientCorrelId": f"chart_{int(time.time() * 1000)}",
                "parameters": {
                    "keys": ",".join(symbols),
                    "fields": "0,1,2,3,4,5,6,7,8"  # All fields: key,sequence,open,high,low,close,volume,time,chart_day
                }
            }

            if self.debug:
                print(f"📤 Sending CHART_EQUITY subscription request: {json.dumps(request, indent=2)}")

            # Send the subscription request
            self.ws.send(json.dumps(request))
            self.request_id += 1

            # Store the subscription
            self.subscriptions["CHART_EQUITY"] = symbols

            print(f"✅ Subscribed to CHART_EQUITY data for: {', '.join(symbols)}")

        except Exception as e:
            print(f"❌ Error subscribing to CHART_EQUITY data: {e}")
            raise

    def subscribe_options_data(self, option_symbols: List[str]):
        """Subscribe to LEVELONE_OPTIONS data for real-time option prices"""
        if not self.connected:
            print("❌ Not connected to WebSocket")
            return False
        
        if not self.streamer_info:
            print("❌ No streamer info available")
            return False
        
        if not option_symbols:
            print("⚠️  No option symbols to subscribe to")
            return True
        
        try:
            keys = ",".join(option_symbols)
            # Request key fields: last_price, bid_price, ask_price, mark_price, underlying_price
            fields = "0,2,3,4,20,22,35,37"  # symbol, bid, ask, last, strike, underlying, underlying_price, mark
            
            request = {
                "service": "LEVELONE_OPTIONS",
                "command": "SUBS",
                "requestid": str(self.request_id),
                "SchwabClientCustomerId": self.streamer_info.get("schwabClientCustomerId", ""),
                "SchwabClientCorrelId": self.streamer_info.get("schwabClientCorrelId", ""),
                "parameters": {
                    "keys": keys,
                    "fields": fields
                }
            }
            
            if self.debug:
                print(f"📤 Options subscription: {json.dumps(request, indent=2)}")
            
            self.ws.send(json.dumps(request))
            self.subscriptions["LEVELONE_OPTIONS"] = option_symbols
            self.option_symbols.update(option_symbols)
            self.request_id += 1
            
            print(f"✅ Subscribed to LEVELONE_OPTIONS for: {option_symbols}")
            return True
            
        except Exception as e:
            print(f"❌ Options subscription failed: {e}")
            return False
    
    def add_option_symbol_subscription(self, option_symbol: str):
        """Add a single option symbol to the streaming subscription"""
        if not self.connected:
            if self.debug:
                print(f"⚠️  Not connected, cannot add option symbol: {option_symbol}")
            return False
        
        if option_symbol in self.option_symbols:
            if self.debug:
                print(f"📊 Option symbol already subscribed: {option_symbol}")
            return True
        
        try:
            # Use ADD command to add to existing subscription
            request = {
                "service": "LEVELONE_OPTIONS",
                "command": "ADD",
                "requestid": str(self.request_id),
                "SchwabClientCustomerId": self.streamer_info.get("schwabClientCustomerId", ""),
                "SchwabClientCorrelId": self.streamer_info.get("schwabClientCorrelId", ""),
                "parameters": {
                    "keys": option_symbol,
                    "fields": "0,2,3,4,20,22,35,37"  # symbol, bid, ask, last, strike, underlying, underlying_price, mark
                }
            }
            
            if self.debug:
                print(f"📤 Adding option symbol: {option_symbol}")
            
            self.ws.send(json.dumps(request))
            self.option_symbols.add(option_symbol)
            self.request_id += 1
            
            print(f"✅ Added option symbol to stream: {option_symbol}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to add option symbol: {e}")
            return False

    def remove_option_symbol_subscription(self, option_symbol: str):
        """Remove a single option symbol from the streaming subscription"""
        if not self.connected:
            if self.debug:
                print(f"⚠️  Not connected, cannot remove option symbol: {option_symbol}")
            return False
        
        if option_symbol not in self.option_symbols:
            if self.debug:
                print(f"📊 Option symbol not subscribed: {option_symbol}")
            return True  # Already not subscribed, consider it success
        
        try:
            # Use UNSUBS command to remove from existing subscription
            request = {
                "service": "LEVELONE_OPTIONS",
                "command": "UNSUBS",
                "requestid": str(self.request_id),
                "SchwabClientCustomerId": self.streamer_info.get("schwabClientCustomerId", ""),
                "SchwabClientCorrelId": self.streamer_info.get("schwabClientCorrelId", ""),
                "parameters": {
                    "keys": option_symbol
                }
            }
            
            if self.debug:
                print(f"📤 Removing option symbol: {option_symbol}")
            
            self.ws.send(json.dumps(request))
            self.option_symbols.discard(option_symbol)  # Remove from our tracking set
            self.request_id += 1
            
            print(f"✅ Removed option symbol from stream: {option_symbol}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to remove option symbol: {e}")
            return False

    def get_option_subscription_count(self):
        """Get the current number of option symbols being streamed"""
        return len(self.option_symbols)
    
    def get_subscribed_option_symbols(self):
        """Get the list of currently subscribed option symbols"""
        return list(self.option_symbols)

    def disconnect(self):
        """Disconnect from WebSocket API"""
        self.running = False
        if self.ws:
            self.ws.close()

        print("🔌 Disconnected from Schwab Streaming API")

    def get_upcoming_expirations(self, symbol: str, num_expirations: int = 6) -> List[str]:
        """
        Get upcoming option expiration dates for a symbol, including both weekly and monthly expirations.
        Returns the next 6 closest expiration dates.
        
        Args:
            symbol: The underlying symbol (e.g., 'AAPL')
            num_expirations: Number of upcoming expirations to return (default: 6)
            
        Returns:
            List of expiration dates in YYMMDD format
        """
        from datetime import datetime, timedelta
        import calendar
        
        # Get current date
        today = datetime.now()
        
        def get_friday(year, month, week_number):
            """Get the nth Friday of a given month"""
            cal = calendar.monthcalendar(year, month)
            fridays = [week[calendar.FRIDAY] for week in cal if week[calendar.FRIDAY] != 0]
            if week_number <= len(fridays):
                return datetime(year, month, fridays[week_number - 1])
            return None
        
        expirations = []
        current_date = today
        
        # Get next 6 Fridays (including both weekly and monthly expirations)
        while len(expirations) < num_expirations:
            # Move to next Friday
            days_until_friday = (4 - current_date.weekday()) % 7
            if days_until_friday == 0:
                days_until_friday = 7
            current_date += timedelta(days=days_until_friday)
            
            # Format as YYMMDD
            exp_str = current_date.strftime('%y%m%d')
            if exp_str not in expirations:
                expirations.append(exp_str)
            
            # Move to next day to continue the loop
            current_date += timedelta(days=1)
        
        return expirations

    def format_option_symbol(self, symbol: str, expiration: str, strike: float, option_type: str) -> str:
        """
        Format an option symbol according to Schwab's format:
        RRRRRRYYMMDDsWWWWWddd
        
        Args:
            symbol: The underlying symbol (e.g., 'AAPL')
            expiration: Expiration date in YYMMDD format
            strike: Strike price
            option_type: 'C' for call or 'P' for put
            
        Returns:
            Formatted option symbol
        """
        # Pad symbol to 6 characters
        symbol = symbol.ljust(6)
        
        # Format strike price (5 digits for whole number, 3 for decimal)
        strike_str = f"{int(strike):05d}{int((strike % 1) * 1000):03d}"
        
        # Combine all parts
        return f"{symbol}{expiration}{option_type}{strike_str}"

    def get_option_chain(self, symbol: str, expiration: str = None) -> List[str]:
        """
        Get a list of option symbols for a given expiration date.
        If no expiration is provided, uses the next monthly expiration.
        
        Args:
            symbol: The underlying symbol (e.g., 'AAPL')
            expiration: Optional expiration date in YYMMDD format
            
        Returns:
            List of option symbols
        """
        if not expiration:
            # Get next monthly expiration
            expiration = self.get_upcoming_expirations(symbol, 1)[0]
        
        # Common strike price intervals
        strike_intervals = [5, 10, 20, 50, 100]
        
        # Get current price (you'll need to implement this)
        current_price = 100  # Placeholder - implement actual price fetch
        
        # Generate strikes around current price
        strikes = []
        for interval in strike_intervals:
            # Add strikes below current price
            strike = current_price - (current_price % interval)
            while strike > current_price * 0.5:  # Go down to 50% of current price
                strikes.append(strike)
                strike -= interval
            
            # Add strikes above current price
            strike = current_price + (interval - (current_price % interval))
            while strike < current_price * 1.5:  # Go up to 150% of current price
                strikes.append(strike)
                strike += interval
        
        # Remove duplicates and sort
        strikes = sorted(list(set(strikes)))
        
        # Generate option symbols
        option_symbols = []
        for strike in strikes:
            # Add call
            option_symbols.append(self.format_option_symbol(symbol, expiration, strike, 'C'))
            # Add put
            option_symbols.append(self.format_option_symbol(symbol, expiration, strike, 'P'))
        
        return option_symbols

    def subscribe_option_chain(self, symbol: str, expiration: str = None):
        """
        Subscribe to an entire option chain for a given symbol and expiration.
        
        Args:
            symbol: The underlying symbol (e.g., 'AAPL')
            expiration: Optional expiration date in YYMMDD format
        """
        option_symbols = self.get_option_chain(symbol, expiration)
        print(f"📊 Subscribing to {len(option_symbols)} options for {symbol}")
        self.subscribe_options_data(option_symbols)

    def fetch_expiration_day(self, symbol: str, at_least: int) -> str:
        """
        Fetches the expiration chain for a given symbol and returns the first expiration date
        with days to expiration greater than or equal to the specified minimum.
        """
        url = f"https://api.schwabapi.com/marketdata/v1/expirationchain?symbol={symbol}"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for expiration in data.get("expirations", []):
                if expiration.get("daysToExpiration", 0) >= at_least:
                    return expiration.get("expirationDate")
        return None

if __name__ == "__main__":
    # Create client with debug mode enabled  
    client = SchwabStreamerClient(debug=True)
    
    # Override timeframes to only use 1m with specific indicator periods
    client.timeframes = ["1m"]  # Only 1m
    
    # Set up 1m indicator configuration
    client.indicator_config_1m = {
        "ema_periods": [5],
        "vwma_periods": [16], 
        "roc_periods": [6],
        "fast_emas": [15],
        "slow_emas": [39],
        "signal_emas": [11]
    }
    
    try:
        # Step 1: Backfill today's historical data first
        print("🚀 Starting Schwab Streamer in STREAMING-ONLY mode...")
        print("📊 Configuration:")
        print(f"   Timeframe: 1m only")
        print(f"   EMA: {client.indicator_config_1m['ema_periods']}")
        print(f"   VWMA: {client.indicator_config_1m['vwma_periods']}")
        print(f"   ROC: {client.indicator_config_1m['roc_periods']}")
        print(f"   MACD: Fast {client.indicator_config_1m['fast_emas']}, Slow {client.indicator_config_1m['slow_emas']}, Signal {client.indicator_config_1m['signal_emas']}")
        print("   Processing: Trade after every candle")
        print("   Email: On every trade signal")
        
        print("\n📈 Step 1: Backfilling today's historical 1m data...")
        client.backfill_todays_data()
        
        print("\n🔗 Step 2: Connecting to streaming...")
        client.connect()
        
        # Keep the script running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        client.disconnect()
    except Exception as e:
        print(f"❌ Error: {e}")
        client.disconnect()