
import json
import time
import threading
import httpx
import websocket
import os
import time
import pandas as pd
from typing import List
from data_manager import DataManager
from schwab_auth import SchwabAuth
from file_utils import get_symbols_from_file, get_timeframes_from_file

class SchwabStreamerClient:
    """Main client for Schwab Streaming API - OHLCV Data Collection"""
    
    def __init__(self, debug: bool = False, symbols_filepath: str = 'symbols.txt', timeframes_filepath: str = 'timeframe.txt'):
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        self.debug = debug
        self.auth = SchwabAuth()
        # Read equity symbols from file path, timeframe from file path
        self.equity_symbols = get_symbols_from_file(symbols_filepath)
        self.timeframes = get_timeframes_from_file(timeframes_filepath)
        self.data_manager = DataManager(self.auth, self.equity_symbols, self.timeframes)

        
        # Candle aggregation buffers for higher timeframes
        self.candle_buffers = self.initialize_candle_buffer(self.equity_symbols, self.timeframes) # Store 1m candles for aggregation

        self.daily_email_sent = False  # Track if 4pm email was sent today
        self.last_email_date = None    # Track the date when last email was sent
        
        self.ws = None
        self.running = False
        self.connected = False
        self.request_id = 1
        self.subscriptions = {}
        self.message_handlers = {}
        self.streamer_info = None  # Store streamer info for subscription requests
        
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

    def disconnect(self):
        """Disconnect from WebSocket API"""
        self.running = False
        if self.ws:
            self.ws.close()

        print("🔌 Disconnected from Schwab Streaming API")
    
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

            # Map the fields using the chart_equity_fields mapping
            parsed = {}
            for i, field_value in enumerate(fields):
                if i in self.chart_equity_fields:
                    field_name = self.chart_equity_fields[i]
                    
                    # Convert to appropriate data type based on field
                    if field_name in ['open', 'high', 'low', 'close', 'volume']:
                        parsed[field_name] = float(field_value)
                    elif field_name in ['time', 'chart_day', 'sequence']:
                        parsed[field_name] = int(field_value)
                    else:
                        parsed[field_name] = field_value

            return parsed

        except Exception as e:
            print(f"❌ Error parsing CHART_EQUITY data: {e}")
            return None

    def on_message(self, _, message):
        """Handle WebSocket messages"""
        try:

            # Current time in Eastern Time
            current_time = pd.Timestamp.now(tz='US/Eastern')
            # Market open and close times in Eastern Time
            market_open = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = current_time.replace(hour=16, minute=1, second=0, microsecond=0)
            
            # Check for 4pm daily email (send once per day)
            four_pm = current_time.replace(hour=16, minute=0, second=0, microsecond=0)

            # Send daily email if it's 4pm and we haven't sent it yet
            if current_time >= four_pm and not self.daily_email_sent:
                self.send_daily_trades_email()
                self.daily_email_sent = True
                self.last_email_date = current_time.date()  # Store the date when email was sent
                # Disconnect the stream after sending email
                print("🔌 Disconnecting stream after daily email...")
                self.disconnect()
                return
            
            # Ignore streaming data before 9:30 AM or after market close
            if current_time < market_open:
                if self.debug:
                    print(f"🕐 Before market open ({current_time.strftime('%H:%M:%S ET')} < 09:30:00 ET), ignoring streaming data")
                return
            elif current_time >= market_close:
                if self.debug:
                    print(f"🕐 After market close ({current_time.strftime('%H:%M:%S ET')} >= 16:01:00 ET), ignoring streaming data")
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
                            print("📊 Subscribing to symbols...")
                            self.subscribe_chart_data(self.equity_symbols)
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
                            self.subscribe_chart_data(self.equity_symbols)
                
                elif "data" in data:
                    # Market data
                    for data_item in data["data"]:
                        service = data_item.get("service")
                        content = data_item.get("content", [])
                        
                        if service == "CHART_EQUITY" and content:
                            for candle_data in content:
                                # Use parse_chart_data to parse the candle data
                                parsed_candle = self.parse_chart_data(candle_data)
                                if parsed_candle:
                                    # Format for processing
                                    formatted_candle = {
                                        'timestamp': parsed_candle.get('time', 0),  # Chart Time (milliseconds since Epoch)
                                        'open': parsed_candle.get('open', 0),       # Open Price
                                        'high': parsed_candle.get('high', 0),       # High Price
                                        'low': parsed_candle.get('low', 0),         # Low Price
                                        'close': parsed_candle.get('close', 0),     # Close Price
                                        'volume': parsed_candle.get('volume', 0)    # Volume
                                    }
                                    
                                    symbol = parsed_candle.get('key')  # Symbol (ticker symbol in upper case)
                                    if symbol in self.equity_symbols:
                                        # Process new candle with the new workflow
                                        self.process_new_candle(symbol, '1m', formatted_candle)
                                        if self.debug:
                                            print(f"Processed new candle for {symbol}: {formatted_candle}")
                else:
                    # Handle unexpected message types
                    if self.debug:
                        print(f"⚠️ Unexpected message type received: {list(data.keys())}")
                    else:
                        print(f"⚠️ Unexpected message type received with keys: {list(data.keys())}")
        except Exception as e:
            print(f"❌ Message handling error: {str(e)}")
            print(f"   Raw message: {message}")
            raise

    def on_error(self, _, error):
        """Handle WebSocket errors"""
        print(f"❌ WebSocket error: {str(error)}")
        self.connected = False

    def on_close(self, _, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        print(f"WebSocket connection closed: {close_status_code} - {close_msg}")
        self.connected = False

    def on_open(self, _):
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

    def process_new_candle(self, symbol: str, timeframe: str, candle_data: dict):
        """
        Process new candle data from streaming API
        """
        try:
            # Convert timestamp from milliseconds to datetime
            timestamp_ms = candle_data['timestamp']
            timestamp_dt = pd.Timestamp(timestamp_ms, unit='ms', tz='US/Eastern')
            
            # Create new row in the format expected by DataManager (matching CSV structure)
            new_row = pd.Series({
                'timestamp': timestamp_ms,  # Keep original milliseconds format
                'datetime': timestamp_dt.strftime('%Y-%m-%d %H:%M:%S %Z'),  # Formatted datetime string
                'open': candle_data['open'],
                'high': candle_data['high'],
                'low': candle_data['low'],
                'close': candle_data['close'],
                'volume': candle_data['volume']
            })
            
            # Add to candle buffer for aggregation to higher timeframes
            self.add_to_candle_buffer(symbol, timeframe, candle_data)

            # Fetch the DataFrame for the symbol and timeframe
            df = self.data_manager.fetchDF(symbol, timeframe)
            
            # Check if this candle already exists (avoid duplicates)
            if not df.empty and len(df) > 0:
                last_timestamp = df.iloc[-1]['timestamp']
                # Check if the new row is within 30 seconds of the last row
                if abs(new_row['timestamp'] - last_timestamp) < 30000:  # Within 30 seconds (30000 ms), likely duplicate
                    if self.debug:
                        print(f"🔄 Duplicate candle detected for {symbol}, skipping")
                    return
        
            # Add new row to DataFrame
            df.loc[len(df)] = new_row
            
            self.data_manager.indicator_generator.calculate_real_time_indicators(symbol, timeframe, df)
            
            # Save to CSV efficiently (append only the new row)
            if len(df) > 0:
                latest_row_with_indicators = df.iloc[-1]
                self.data_manager.append_row_to_csv(latest_row_with_indicators, symbol, timeframe)
            # Process signals for the new row only (real-time processing)
            if len(df) > 0:
                latest_row = df.iloc[-1]
                # Use data manager's process_latest_signal which handles both original and inverse symbols
                self.data_manager.process_latest_signal(symbol, timeframe, latest_row)
                if self.debug:
                    print(f"📊 Processed signal for {symbol} {timeframe}: Close=${latest_row['close']:.2f}")
            # Print confirmation (only in debug mode to avoid spam)
            if self.debug:
                print(f"✅ Processed new candle: {symbol} {timeframe} @ {timestamp_dt.strftime('%H:%M:%S')} Close=${candle_data['close']:.2f}")

        except Exception as e:
            print(f"❌ Error processing new candle for {symbol}: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()

    def initialize_candle_buffer(self, symbol: str):
        """
        Initialize candle buffer with only incomplete candles that haven't been aggregated.
        """
        try:
            df_1m = self.data_manager.fetchDF(symbol, '1m')
            
            if df_1m is None or len(df_1m) == 0:
                print(f"⚠️ No 1m data found for {symbol}, initializing empty buffer")
                self.candle_buffers[symbol] = {}
                for timeframe in self.timeframes[1:]:
                    self.candle_buffers[symbol][timeframe] = []
                return
            
            # Initialize buffer for each timeframe
            for timeframe in self.timeframes[1:]:
                timeframe_minutes = int(timeframe.replace('m', ''))
                
                # Calculate incomplete candles: remaining 1m candles that don't make a complete higher timeframe
                incomplete_count = len(df_1m) % timeframe_minutes
                
                if incomplete_count > 0:
                    # Get incomplete candles
                    incomplete_candles = df_1m.tail(incomplete_count)
                    
                    buffer_candles = []
                    for _, row in incomplete_candles.iterrows():
                        candle = {
                            'timestamp': row['timestamp'],
                            'open': row['open'],
                            'high': row['high'],
                            'low': row['low'],
                            'close': row['close'],
                            'volume': row['volume']
                        }
                        buffer_candles.append(candle)
                    
                    if symbol not in self.candle_buffers:
                        self.candle_buffers[symbol] = {}
                    self.candle_buffers[symbol][timeframe] = buffer_candles
                    
                    if self.debug:
                        print(f"📊 {symbol} {timeframe}: {incomplete_count} incomplete candles in buffer")
                else:
                    # No incomplete candles
                    if symbol not in self.candle_buffers:
                        self.candle_buffers[symbol] = {}
                    self.candle_buffers[symbol][timeframe] = []
                    
                    if self.debug:
                        print(f"📊 {symbol} {timeframe}: no incomplete candles")
            
            print(f"✅ Initialized incomplete candle buffers for {symbol}")
            
        except Exception as e:
            print(f"❌ Error initializing candle buffer for {symbol}: {e}")
            if symbol not in self.candle_buffers:
                self.candle_buffers[symbol] = {}
            for timeframe in self.timeframes[1:]:
                self.candle_buffers[symbol][timeframe] = []

    def add_to_candle_buffer(self, symbol: str, candle_data: dict):
        """
        Add new candle to buffer and check if aggregation is needed.
        """
        # Initialize buffer if needed
        if symbol not in self.candle_buffers:
            self.initialize_candle_buffer(symbol)
        
        # Add to all timeframe buffers
        for timeframe in self.timeframes[1:]:
            if timeframe not in self.candle_buffers[symbol]:
                self.candle_buffers[symbol][timeframe] = []
            
            buffer = self.candle_buffers[symbol][timeframe]
            buffer.append(candle_data)
            
            # Check if we should aggregate
            timeframe_minutes = int(timeframe.replace('m', ''))
            if len(buffer) == timeframe_minutes:
                # Buffer is full - aggregate and clear
                aggregated_candle = self.aggregate_candles(symbol, timeframe)
                if aggregated_candle:
                    self.process_new_candle(symbol, timeframe, aggregated_candle)
                
                # Clear buffer after aggregation
                self.candle_buffers[symbol][timeframe] = []
                
                if self.debug:
                    print(f"✅ Aggregated {timeframe} candle for {symbol}, buffer cleared")


    def aggregate_candles(self, symbol: str, timeframe: str) -> dict:
        """Aggregate 1m candles into higher timeframe candle"""
        if symbol not in self.candle_buffers or not self.candle_buffers[symbol]:
            return None
        
        # Get last timeframe candles
        timeframe_minutes = int(timeframe.replace('m', ''))

        df_1m = self.data_manager.fetchDF(symbol, '1m')
        relevant_candles = df_1m.tail(timeframe_minutes)
        
        if not relevant_candles:
            return None

        # Aggregate OHLCV data
        aggregated_candle = {
            'timestamp': relevant_candles['timestamp'].iloc[-1],  # Use end time as the candle timestamp
            'open': relevant_candles['open'].iloc[0],  # First candle's open
            'high': relevant_candles['high'].max(),  # Highest high
            'low': relevant_candles['low'].min(),    # Lowest low
            'close': relevant_candles['close'].iloc[-1],  # Last candle's close
            'volume': relevant_candles['volume'].sum()  # Total volume
        }
        
        if self.debug:
            print(f"📊 Aggregated {len(relevant_candles)} candles for {symbol} {timeframe}")
            print(f"   Result: O={aggregated_candle['open']:.2f} H={aggregated_candle['high']:.2f} L={aggregated_candle['low']:.2f} C={aggregated_candle['close']:.2f} V={aggregated_candle['volume']}")
        
        return aggregated_candle

    def send_daily_trades_email(self):
        """Send daily trades email and disconnect the stream"""
        try:
            print("📧 Sending daily trades email...")
            success = self.data_manager.email_daily_summary()
            
            if success:
                print("✅ Daily trades email sent successfully")
            else:
                print("❌ Failed to send daily trades email")
            
            # Disconnect the stream after sending email
            print("🔌 Disconnecting stream after daily email...")
            self.disconnect()
            
        except Exception as e:
            print(f"❌ Error sending daily trades email: {e}")
            # Still disconnect even if email fails
            self.disconnect()


if __name__ == "__main__":
    # Create client (debug mode will be enabled after bootstrap)
    client = SchwabStreamerClient()
    try:
        # Run bootstrap first to ensure all historical data is processed
        print("🚀 Running bootstrap to process historical data...")
        client.data_manager.bootstrap()
        print("✅ Bootstrap completed, now starting streaming...")
        
        # Enable debug mode for streaming to see real-time processing
        client.debug = True
        print("🔍 Debug mode enabled for streaming")
        
        # Now connect to streaming after bootstrap is complete
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