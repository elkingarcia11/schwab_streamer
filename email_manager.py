"""
Email Notifier Module
Handles email notifications for position changes, including LONG and SHORT positions
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Optional
import time

class EmailManager:
    def __init__(self):
        self.enabled = False
        self.sender = None
        self.password = None
        self.recipients = []
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.last_open_positions_email = 0  # Track last email timestamp
        
        self._load_credentials()
    
    def send_trade_log_email(self, trade_log_csv_path, subject="Trade Log Summary", body=None):
        if not self.enabled:
            print("EmailManager is disabled.")
            return False

        if not self.recipients:
            print("No email recipients configured.")
            return False

        if body is None:
            body = f"Attached is the trade log summary. Generated on {time.strftime('%Y-%m-%d %H:%M:%S')}."

        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = ", ".join(self.recipients)
            msg['Subject'] = subject
            msg.set_content(body)

            # Attach the CSV file
            with open(trade_log_csv_path, 'rb') as f:
                file_data = f.read()
                file_name = trade_log_csv_path.split('/')[-1]

            msg.add_attachment(file_data, maintype='text', subtype='csv', filename=file_name)

            # Connect and send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as smtp:
                smtp.starttls()
                smtp.login(self.sender, self.password)
                smtp.send_message(msg)

            self.last_open_positions_email = time.time()
            print(f"Trade log email sent to {self.recipients}")
            return True

        except Exception as e:
            print(f"Failed to send trade log email: {e}")
            return False
        
    def _load_credentials(self):
        """Load email credentials from environment file"""
        try:
            env_file = "email_credentials.env"
            if not os.path.exists(env_file):
                print("📧 Email credentials file not found, email notifications disabled")
                return
            
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        if key == "EMAIL_ALERTS_ENABLED":
                            self.enabled = value.lower() == 'true'
                        elif key == "SENDER_EMAIL":
                            self.sender = value
                        elif key == "SENDER_PASSWORD":
                            self.password = value
                        elif key == "TO_EMAILS":
                            self.recipients = [email.strip() for email in value.split(',')]
                        elif key == "SMTP_SERVER":
                            self.smtp_server = value
                        elif key == "SMTP_PORT":
                            self.smtp_port = int(value)
            
            if self.enabled:
                print(f"📧 Email notifications enabled for {len(self.recipients)} recipients")
            else:
                print("📧 Email notifications disabled in configuration")
                
        except Exception as e:
            print(f"❌ Error loading email credentials: {e}")
            self.enabled = False
    
    def test_configuration(self) -> bool:
        """
        Test email configuration
        
        Returns:
            True if configuration is valid, False otherwise
        """
        if not self.enabled:
            print("📧 Email notifications are disabled")
            return False
        
        if not all([self.sender, self.password, self.recipients]):
            print("❌ Email configuration incomplete")
            return False
        
        try:
            # Test SMTP connection
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender, self.password)
            server.quit()
            print("✅ Email configuration test successful")
            return True
        except Exception as e:
            print(f"❌ Email configuration test failed: {e}")
            return False
    
    def send_position_notification(self, symbol: str, period: str, position_type: str, action: str, 
                                 signal_details: Dict, pnl_info: Optional[Dict], positions: Dict) -> bool:
        """
        Send email notification for position changes (LONG or SHORT)
        
        Args:
            symbol: Stock symbol
            period: Time period
            position_type: 'LONG' or 'SHORT'
            action: 'OPEN' or 'CLOSE'
            signal_details: Dictionary with signal information
            pnl_info: P&L information (for CLOSE actions)
            positions: Current position status
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            return True  # Not an error if disabled
        
        try:
            # Extract signal details
            price = signal_details.get('price', 'Unknown')
            conditions_met = signal_details.get('conditions_met', 0)
            condition_summary = signal_details.get('condition_summary', 'Unknown')
            timestamp = signal_details.get('timestamp', 'Unknown')
            
            # Create subject with position type
            if action == 'OPEN':
                subject_emoji = "🟢"
                action_text = f"OPEN {position_type}"
                price_info = f"${price:.4f}" if isinstance(price, (int, float)) else str(price)
            else:  # CLOSE
                if pnl_info and pnl_info.get('pnl_dollar', 0) >= 0:
                    subject_emoji = "📈"
                    pnl_dollar = pnl_info['pnl_dollar']
                    price_info = f"${price:.4f} 📈${pnl_dollar:.4f}"
                else:
                    subject_emoji = "📉"
                    pnl_dollar = pnl_info['pnl_dollar'] if pnl_info else 0
                    price_info = f"${price:.4f} 📉${abs(pnl_dollar):.4f}"
                action_text = f"CLOSE {position_type}"
            
            subject = f"{subject_emoji} {symbol} {period} - {action_text} at {price_info}"
            
            # Create email body with position type information
            body = f"""🚨 {symbol} {period} - {action_text} POSITION at ${price:.4f}

Position Change Details:
- Symbol: {symbol}
- Timeframe: {period}
- Position Type: {position_type}
- Action: {action_text}
- Time: {timestamp}
- Price: ${price:.4f}
- Conditions Met: {conditions_met}/3

Technical Conditions:
{condition_summary}
"""
            
            # Add P&L analysis for CLOSE actions
            if action == 'CLOSE' and pnl_info:
                opening_price = pnl_info.get('opening_price', 0)
                closing_price = pnl_info.get('closing_price', 0)
                pnl_dollar = pnl_info.get('pnl_dollar', 0)
                pnl_percent = pnl_info.get('pnl_percent', 0)
                total_pnl = pnl_info.get('total_pnl', 0)
                
                pnl_emoji = "📈" if pnl_dollar >= 0 else "📉"
                
                body += f"""
P&L Analysis ({position_type} Position):
- Opening Price: ${opening_price:.4f}
- Closing Price: ${closing_price:.4f}
- Profit/Loss: {pnl_emoji} ${pnl_dollar:.4f} ({pnl_percent:+.2f}%)
- Total P&L ({period} {position_type}): ${total_pnl:.4f}
"""
            
            # Add current position status
            body += f"""
Current Positions Status:
- 1m: {positions.get('1m', 'Unknown')}
- 5m: {positions.get('5m', 'Unknown')}  
- 15m: {positions.get('15m', 'Unknown')}

Position Types: L=LONG, S=SHORT
Trading Logic: Open when ALL 3 conditions met, Close when ≤1 condition remains
"""
            
            # Send email
            return self._send_email(subject, body)
            
        except Exception as e:
            print(f"❌ Error creating position notification: {e}")
            return False
    
    def _send_email(self, subject: str, body: str) -> bool:
        """
        Send email with given subject and body
        
        Args:
            subject: Email subject
            body: Email body
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = ', '.join(self.recipients)
            msg['Subject'] = subject
            
            # Add body
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender, self.password)
            text = msg.as_string()
            server.sendmail(self.sender, self.recipients, text)
            server.quit()
            
            print(f"📧 Email sent successfully to {len(self.recipients)} recipients")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send email: {e}")
            return False
    
    def send_test_email(self) -> bool:
        """
        Send a test email to verify configuration
        
        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            print("📧 Email notifications are disabled")
            return False
        
        subject = "🧪 Schwab Market Data - Email Test"
        body = """This is a test email from the Schwab Market Data Coordinator.

If you receive this email, your email notification configuration is working correctly.

System Features:
- LONG positions (Regular price data)
- SHORT positions (Inverse price data)  
- Multi-timeframe tracking (1m, 5m, 15m)
- Real-time P&L calculations
- Technical indicator signals (EMA, VWMA, MACD, ROC)

Position Types:
- LONG: Based on regular OHLC price data
- SHORT: Based on inverse (1/price) OHLC data
- Same 3-condition logic applied to both types

You will receive notifications when:
- LONG positions open/close based on regular data conditions
- SHORT positions open/close based on inverse data conditions
- P&L analysis for all closed positions

Happy trading! 📈📉
"""
        
        return self._send_email(subject, body) 
    
    def send_signal_alert(self, action: str, symbol: str, timeframe: int, signal_details: Dict) -> bool:
        """
        Send email notification for trading signals with options support
        
        Args:
            action: 'BUY' or 'SELL'
            symbol: Stock symbol
            timeframe: Timeframe in minutes
            signal_details: Dictionary with signal information including options data
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            return True  # Not an error if disabled
        
        try:
            # Extract signal details
            price = signal_details.get('price', 'Unknown')
            underlying_price = signal_details.get('underlying_price', price)
            is_option = signal_details.get('is_option', False)
            option_symbol = signal_details.get('option_symbol')
            option_type = signal_details.get('option_type', 'CALL')  # CALL, PUT, or EQUITY
            strike_price = signal_details.get('strike_price')
            expiry_date = signal_details.get('expiry_date')
            conditions_met = signal_details.get('conditions_met', 0)
            condition_summary = signal_details.get('condition_summary', 'Unknown')
            buy_time = signal_details.get('buy_time')
            sell_time = signal_details.get('sell_time')
            entry_time = signal_details.get('entry_time')
            if entry_time and isinstance(entry_time, str):
                entry_time = entry_time.split('-')[0]
            else:
                entry_time = None
            
            # Create subject based on action and position type
            if action == 'BUY':
                subject_emoji = "🟢"
                action_text = "BUY"
                if is_option:
                    strike_str = f"${strike_price:.2f}" if strike_price and strike_price > 0 else "TBD"
                    option_name = "Call" if option_type == 'CALL' else "Put"
                    price_info = f"${price:.2f} {option_name} (Strike: {strike_str})"
                    display_symbol = option_symbol or f"{symbol} {option_type}"
                else:
                    price_info = f"${price:.2f}"
                    display_symbol = symbol
            else:  # SELL
                # Check if it was profitable
                entry_price = signal_details.get('entry_price', price)
                realized_pnl = signal_details.get('realized_pnl_percent', 0)
                
                if realized_pnl >= 0:
                    subject_emoji = "📈"
                    pnl_text = f"+{realized_pnl:.1f}%"
                else:
                    subject_emoji = "📉"
                    pnl_text = f"{realized_pnl:.1f}%"
                
                action_text = "SELL"
                if is_option:
                    option_name = "Call" if option_type == 'CALL' else "Put"
                    price_info = f"${price:.2f} {option_name} {pnl_text}"
                    display_symbol = option_symbol or f"{symbol} {option_type}"
                else:
                    price_info = f"${price:.2f} {pnl_text}"
                    display_symbol = symbol
            
            subject = f"{subject_emoji} {display_symbol} {timeframe}min - {action_text} at {price_info}"
            
            # Create detailed email body
            if is_option:
                # Options trading email
                strike_str = f"${strike_price:.2f}" if strike_price and strike_price > 0 else "TBD"
                option_name = "Call" if option_type == 'CALL' else "Put"
                strategy_data = "Regular Data" if option_type == 'CALL' else "Inverse Data (1/OHLC)"
                position_type = f"LONG {option_type}"
                
                body = f"""🚨 {action_text} {option_name} OPTIONS SIGNAL - {symbol} {timeframe}min

📊 Options Details:
- Position Type: {position_type} ({strategy_data})
- Underlying: {symbol} @ ${underlying_price:.2f}
- Option Symbol: {option_symbol or 'Generated'}
- Option Type: {option_name}
- Strike Price: {strike_str}
- Expiry Date: {expiry_date or 'TBD'}
- Option Price: ${price:.2f}
- Action: {action_text}
- Transaction Time: {buy_time or sell_time or 'Unknown'}

📈 Signal Analysis:
- Timeframe: {timeframe} minutes
- Data Source: {strategy_data}
- Conditions Met: {conditions_met}/3
- Signal triggered on {"regular" if option_type == 'CALL' else "inverse"} price analysis

Technical Conditions:
{condition_summary}
"""

                # Add P&L info for SELL signals
                if action == 'SELL':
                    entry_price = signal_details.get('entry_price', 0)
                    underlying_entry_price = signal_details.get('underlying_entry_price', 0)
                    realized_pnl_percent = signal_details.get('realized_pnl_percent', 0)
                    hold_duration = signal_details.get('hold_duration', 'Unknown')
                    
                    pnl_dollar = (price - entry_price) * 100  # Assuming 100 contracts
                    pnl_emoji = "📈" if realized_pnl_percent >= 0 else "📉"
                    
                    body += f"""
💰 Trade Results:
- Entry Option Price: ${entry_price:.2f}
- Exit Option Price: ${price:.2f}
- Entry Time: {entry_time or buy_time or 'Unknown'}
- Exit Time: {sell_time or 'Unknown'}
- Underlying Entry: ${underlying_entry_price:.2f}
- Underlying Exit: ${underlying_price:.2f}
- Options P&L: {pnl_emoji} ${pnl_dollar:.2f} ({realized_pnl_percent:+.1f}%)
- Hold Duration: {hold_duration}
"""
            else:
                # Equity trading email (fallback)
                body = f"""🚨 {action_text} EQUITY SIGNAL - {symbol} {timeframe}min

📊 Trade Details:
- Symbol: {symbol}
- Price: ${price:.2f}
- Action: {action_text}
- Transaction Time: {buy_time or sell_time or 'Unknown'}
- Timeframe: {timeframe} minutes
- Conditions Met: {conditions_met}/3

Technical Conditions:
{condition_summary}
"""

                # Add P&L info for SELL signals
                if action == 'SELL':
                    entry_price = signal_details.get('entry_price', 0)
                    realized_pnl_percent = signal_details.get('realized_pnl_percent', 0)
                    hold_duration = signal_details.get('hold_duration', 'Unknown')
                    
                    pnl_dollar = (price - entry_price) * 100  # Assuming 100 shares
                    pnl_emoji = "📈" if realized_pnl_percent >= 0 else "📉"
                    
                    body += f"""
💰 Trade Results:
- Entry Price: ${entry_price:.2f}
- Exit Price: ${price:.2f}
- Entry Time: {entry_time or buy_time or 'Unknown'}
- Exit Time: {sell_time or 'Unknown'}
- P&L: {pnl_emoji} ${pnl_dollar:.2f} ({realized_pnl_percent:+.1f}%)
- Hold Duration: {hold_duration}
"""

            # Add disclaimer
            body += f"""

📋 Trading Strategy:
- Entry: EMA(7) > VWMA(17) AND ROC(8) > 0 AND MACD Line > MACD Signal
- Exit: 2+ conditions fail OR 5% stop loss
- Risk Management: Position sizing and stop losses active

⚠️ Disclaimer: This is automated analysis, not financial advice.
"""
            
            # Send email
            return self._send_email(subject, body)
            
        except Exception as e:
            print(f"❌ Error creating signal alert: {e}")
            import traceback
            traceback.print_exc()
            return False

    def send_bootstrap_summary_email(self, symbols: list, total_processed: int, 
                                   open_positions_count: int, transaction_count: int) -> bool:
        """
        Send bootstrap completion summary email
        
        Args:
            symbols: List of symbols processed
            total_processed: Total timeframe combinations processed  
            open_positions_count: Number of open positions
            transaction_count: Total number of transactions
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            return True  # Not an error if disabled
        
        try:
            subject = "🚀 Schwab Trading System - Bootstrap Complete"
            
            symbols_str = ", ".join(symbols)
            
            body = f"""🎉 BOOTSTRAP INITIALIZATION COMPLETE

The Schwab dual-strategy trading system has successfully loaded historical data and is ready for live trading!

📊 Bootstrap Summary:
- Symbols Processed: {len(symbols)} ({symbols_str})
- Timeframe Combinations: {total_processed}
- Date Range: January 1, 2025 - June 9, 2025
- Data Source: Schwab API (15min intervals → aggregated)

💼 Position Status:
- Total Transactions Processed: {transaction_count}
- Currently Open Positions: {open_positions_count}

🎯 Dual Strategy Configuration:
- Regular Strategy → CALL Options (Normal OHLC data)
- Inverse Strategy → PUT Options (1/OHLC data with H/L swap)
- Timeframes: 15min, 30min, 1h, 4h

📁 Data Organization:
- File Structure: data/timeframe/symbol.csv & symbol_inverse.csv
- All technical indicators pre-calculated
- Transaction history tracked

🚀 System Status: READY FOR LIVE STREAMING
- Real-time data processing enabled
- Dual strategy signals active
- Email notifications configured
- Options streaming ready (CALL & PUT)

The system will now monitor live market data and execute the dual trading strategy automatically. You'll receive email alerts for:
- CALL option signals (Regular strategy)  
- PUT option signals (Inverse strategy)
- Position opens/closes with P&L analysis

Happy trading! 📈📉
"""
            
            return self._send_email(subject, body)
            
        except Exception as e:
            print(f"❌ Error creating bootstrap summary email: {e}")
            return False

    def send_open_positions_email(self, open_positions: list) -> bool:
        """
        Send email with current open positions details
        
        Args:
            open_positions: List of open position dictionaries
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            return True  # Not an error if disabled
        
        if not open_positions:
            return True  # No positions to report
        
        try:
            # Check if we've sent an email in the last minute
            current_time = time.time()
            if current_time - self.last_open_positions_email < 60:  # 60 seconds = 1 minute
                return True  # Skip sending if less than a minute has passed
            
            subject = f"📈 Open Positions Report - {len(open_positions)} Active"
            
            body = f"""📋 CURRENT OPEN POSITIONS REPORT

You currently have {len(open_positions)} active positions:

"""
            
            # List all positions in a single list
            for i, position in enumerate(open_positions, 1):
                symbol = position.get('symbol', 'Unknown')
                timeframe = position.get('timeframe', 'Unknown')
                position_type = position.get('position_type', 'Unknown')
                entry_price = position.get('entry_price', 0)
                current_price = position.get('current_price', 0)
                entry_time = position.get('entry_time', 'Unknown')
                
                # Calculate unrealized P&L
                if entry_price and current_price:
                    pnl_percent = ((current_price - entry_price) / entry_price) * 100
                    pnl_dollar = (current_price - entry_price) * 100  # Assuming 100 shares
                    pnl_emoji = "📈" if pnl_percent >= 0 else "📉"
                    pnl_text = f"{pnl_emoji} ${pnl_dollar:.2f} ({pnl_percent:+.1f}%)"
                else:
                    pnl_text = "Calculating..."
                
                body += f"""🔹 Position #{i}:
   Symbol: {symbol}
   Timeframe: {timeframe}
   Type: {position_type}
   Entry Price: ${entry_price:.2f}
   Current Price: ${current_price:.2f}
   Unrealized P&L: {pnl_text}
   Entry Time: {entry_time} 

"""
            
            body += f"""💡 Position Management:
- All positions will automatically close when exit conditions are met
- Exit triggers: ≤1 technical condition remaining OR 5% stop loss
- You'll receive email notifications for all position closes with final P&L

📊 Strategy Breakdown:
- LONG positions: Based on regular price analysis
- SHORT positions: Based on inverse (1/price) analysis
- Same technical indicators applied to both strategies

⚠️ Monitor your positions and ensure you have sufficient account balance.

This is an automated report from your trading system.
"""
            
            # Update last email timestamp
            self.last_open_positions_email = current_time
            
            return self._send_email(subject, body)
            
        except Exception as e:
            print(f"❌ Error creating open positions email: {e}")
            return False

    def send_bootstrap_summary_with_trades(self, symbols: list, total_processed: int, 
                                         open_positions_count: int, transaction_count: int,
                                         trade_summaries: dict) -> bool:
        """
        Send bootstrap completion summary email with trade breakdown by timeframe
        
        Args:
            symbols: List of symbols processed
            total_processed: Total timeframe combinations processed  
            open_positions_count: Number of open positions
            transaction_count: Total number of transactions
            trade_summaries: Dict with trade stats per timeframe
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            return True  # Not an error if disabled
        
        try:
            subject = "🚀 Schwab Trading System - Bootstrap Complete with Trade Summary"
            
            symbols_str = ", ".join(symbols)
            
            # Build trade summary section
            trade_summary_text = "\n📊 Trade Performance by Timeframe:\n"
            total_call_trades = 0
            total_put_trades = 0
            total_call_wins = 0
            total_put_wins = 0
            
            for timeframe, summary in trade_summaries.items():
                call_trades = summary['call_trades']
                put_trades = summary['put_trades']
                call_wins = summary['call_wins']
                put_wins = summary['put_wins']
                total_call_pnl = summary['total_call_pnl']
                total_put_pnl = summary['total_put_pnl']
                
                total_call_trades += call_trades
                total_put_trades += put_trades
                total_call_wins += call_wins
                total_put_wins += put_wins
                
                call_winrate = (call_wins / call_trades * 100) if call_trades > 0 else 0
                put_winrate = (put_wins / put_trades * 100) if put_trades > 0 else 0
                avg_call_pnl = (total_call_pnl / call_trades) if call_trades > 0 else 0
                avg_put_pnl = (total_put_pnl / put_trades) if put_trades > 0 else 0
                
                trade_summary_text += f"""
🕐 {timeframe}:
   📈 CALL Options: {call_trades} trades | {call_winrate:.1f}% win rate | Avg P&L: {avg_call_pnl:+.1f}%
   📉 PUT Options: {put_trades} trades | {put_winrate:.1f}% win rate | Avg P&L: {avg_put_pnl:+.1f}%"""
            
            # Overall stats
            overall_call_winrate = (total_call_wins / total_call_trades * 100) if total_call_trades > 0 else 0
            overall_put_winrate = (total_put_wins / total_put_trades * 100) if total_put_trades > 0 else 0
            
            body = f"""🎉 BOOTSTRAP INITIALIZATION COMPLETE WITH TRADE ANALYSIS

The Schwab dual-strategy trading system has successfully loaded historical data and generated trading signals!

📊 Bootstrap Summary:
- Symbols Processed: {len(symbols)} ({symbols_str})
- Timeframe Combinations: {total_processed}
- Date Range: January 1, 2025 - June 9, 2025
- Data Source: Schwab API (15min intervals → aggregated)

💼 Trading Results:
- Total Transactions Generated: {transaction_count}
- Currently Open Positions: {open_positions_count}
- CALL Options: {total_call_trades} trades ({overall_call_winrate:.1f}% win rate)
- PUT Options: {total_put_trades} trades ({overall_put_winrate:.1f}% win rate)
{trade_summary_text}

🎯 Dual Strategy Performance:
- Regular Strategy (CALL Options): Analyzed normal OHLC price data
- Inverse Strategy (PUT Options): Analyzed inverse (1/OHLC) price data with H/L swap
- Both strategies used identical technical indicators and entry/exit rules

📁 Data Organization:
- File Structure: data/timeframe/symbol.csv & symbol_inverse.csv
- All technical indicators pre-calculated
- Transaction history tracked in CSV format

🚀 System Status: READY FOR LIVE STREAMING
- Real-time data processing enabled
- Dual strategy signals active
- Email notifications configured for individual trades
- Options streaming ready (CALL & PUT)

📧 Next Steps:
- Start live streaming with main.py
- System will monitor real-time data and execute trades
- You'll receive individual trade alerts during live operation
- Position management automated with stop-loss and exit rules

The bootstrap analysis shows your dual-strategy system is generating {transaction_count} historical trading signals. Monitor performance and adjust parameters as needed.

Happy trading! 📈📉
"""
            
            return self._send_email(subject, body)
            
        except Exception as e:
            print(f"❌ Error creating bootstrap trade summary email: {e}")
            return False 