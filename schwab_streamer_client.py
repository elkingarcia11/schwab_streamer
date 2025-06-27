"""
Main client for Schwab Streaming API - OHLCV Data Collection
    - Connects to WebSocket API
    - Subscribes to CHART_EQUITY data
    - Parses incoming data
    - Stores the data on the 1-minute DataFrame
    - Calculates indicators on the 1-minute DataFrame
    - Aggregates the 1-minute DataFrame to higher timeframes
    - Processes all new data to generate signals
    - Sends daily email with trades
    - Disconnects after sending daily email
"""

import json
import time
import threading
import httpx
import websocket
import os
import time
import traceback
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import List, Optional
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
        # Loads data from csvs into DataManager DataFrames
        # Loads indicators from csvs into IndicatorGenerator's last_values
        self.data_manager = DataManager(self.auth, self.equity_symbols, self.timeframes)

        
        # Candle aggregation buffers for higher timeframes
        self.candle_buffers = {}

        self.daily_email_sent = False  # Track if 4pm email was sent today
        self.last_email_date = None    # Track the date when last email was sent
        
        self.ws: Optional[websocket.WebSocketApp] = None
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
            0: 'key',        # Symbol (ticker symbol)
            1: 'sequence',   # Sequence - Identifies the candle minute
            2: 'open',       # Open Price - Opening price for the minute
            3: 'high',       # High Price - Highest price for the minute  
            4: 'low',        # Low Price - Chart's lowest price for the minute
            5: 'close',      # Close Price - Closing price for the minute
            6: 'volume',     # Volume - Total volume for the minute
            7: 'time',       # Chart Time - Milliseconds since Epoch
            8: 'chart_day'   # Chart Day
        }
        
        # Market hours (Eastern Time)
        self.et_tz = pytz.timezone('US/Eastern')
        self.market_open = datetime.strptime('09:30', '%H:%M').time()
    
    def wait_for_market_open(self):
        """
        Check if it's before 9:30 AM ET and wait until market opens if needed.
        Returns True if market is open or will be open today, False if it's weekend.
        """
        now_et = datetime.now(self.et_tz)
        current_time = now_et.time()
        current_date = now_et.date()
        
        # Check if it's weekend
        if current_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            print(f"📅 Weekend detected ({current_date.strftime('%A')}), market is closed")
            return False
        
        # Check if it's before market hours
        if current_time < self.market_open:
            # Calculate time until market open
            market_open_dt = datetime.combine(current_date, self.market_open)
            market_open_dt = self.et_tz.localize(market_open_dt)
            
            wait_seconds = (market_open_dt - now_et).total_seconds()
            
            if wait_seconds > 0:
                wait_hours = int(wait_seconds // 3600)
                wait_minutes = int((wait_seconds % 3600) // 60)
                wait_secs = int(wait_seconds % 60)
                
                print(f"⏰ Before market hours. Current time: {now_et.strftime('%H:%M:%S %Z')}")
                print(f"📈 Market opens at: {market_open_dt.strftime('%H:%M:%S %Z')}")
                print(f"⏳ Waiting {wait_hours:02d}:{wait_minutes:02d}:{wait_secs:02d} until market open...")
                
                # Wait with progress indicator
                start_time = time.time()
                while time.time() - start_time < wait_seconds:
                    remaining = wait_seconds - (time.time() - start_time)
                    remaining_hours = int(remaining // 3600)
                    remaining_minutes = int((remaining % 3600) // 60)
                    remaining_secs = int(remaining % 60)
                    
                    print(f"\r⏳ Waiting: {remaining_hours:02d}:{remaining_minutes:02d}:{remaining_secs:02d} remaining...", end='', flush=True)
                    time.sleep(1)
                
                print(f"\n🎉 Market is now open! Starting streaming...")
            else:
                print(f"✅ Market is already open (current time: {now_et.strftime('%H:%M:%S %Z')})")
        else:
            print(f"✅ Market is already open (current time: {now_et.strftime('%H:%M:%S %Z')})")
        
        return True
    
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
            if self.ws:
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
                if self.ws:
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
            print(f"🔍 Parsing content item: {content_item}")
            parsed = {}

            for field_key, field_value in content_item.items():
                print(f"  Processing field: {field_key} = {field_value}")
                if field_key.isdigit():
                    field_index = int(field_key)
                    if field_index in self.chart_equity_fields:
                        field_name = self.chart_equity_fields[field_index]
                        print(f"    Mapped to: {field_name}")

                        # Convert to appropriate data type based on field
                        if field_name in ['open', 'high', 'low', 'close', 'volume']:
                            parsed[field_name] = float(field_value)
                        elif field_name in ['time', 'chart_day', 'sequence']:
                            parsed[field_name] = int(field_value)
                        else:
                            parsed[field_name] = field_value
                    else:
                        print(f"    Field index {field_index} not in chart_equity_fields mapping")
                elif field_key in ['key', 'seq']:
                    print(f"    Processing non-numeric key: {field_key}")
                    if field_key == 'seq':
                        parsed['sequence'] = int(field_value)
                    else:
                        parsed[field_key] = field_value
                else:
                    print(f"    Skipping unknown field: {field_key}")

            print(f"✅ Final parsed result: {parsed}")
            return parsed

        except Exception as e:
            print(f"❌ Error parsing CHART_EQUITY data: {e}")
            traceback.print_exc()
            return {}

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
                    print(f"Data: {data}")
                    # Market data
                    for data_item in data["data"]:
                        print(f"Data item: {data_item}")
                        service = data_item.get("service")
                        content = data_item.get("content", [])
                        
                        if service == "CHART_EQUITY" and content:
                            print(f"Processing {len(content)} candles for CHART_EQUITY")
                            for candle_data in content:
                                print(f"Raw candle data: {candle_data}")
                                # Use parse_chart_data to parse the candle data
                                parsed_candle = self.parse_chart_data(candle_data)
                                print(f"Parsed candle result: {parsed_candle}")
                                if parsed_candle:
                                    print(f"Parsed candle: {parsed_candle}")
                                    # Format for processing
                                    formatted_candle = {
                                        'timestamp': parsed_candle.get('time', 0),  # Chart Time (milliseconds since Epoch)
                                        'open': parsed_candle.get('open', 0),       # Open Price
                                        'high': parsed_candle.get('high', 0),       # High Price
                                        'low': parsed_candle.get('low', 0),         # Low Price
                                        'close': parsed_candle.get('close', 0),     # Close Price
                                        'volume': parsed_candle.get('volume', 0)    # Volume
                                    }
                                    symbol = parsed_candle.get('key')  # Symbol (ticker symbol)
                                    print(f"Symbol: {symbol}, Equity symbols: {self.equity_symbols}")
                                    if symbol in self.equity_symbols:
                                        print(f"Processing new candle for {symbol}: {formatted_candle}")
                                        # Process new candle with the new workflow
                                        self.process_new_candle(symbol, '1m', formatted_candle)
                                        if self.debug:
                                            print(f"Processed new candle for {symbol}: {formatted_candle}")
                                    else:
                                        print(f"⚠️ Symbol {symbol} not in equity_symbols list")
                                else:
                                    print(f"❌ Failed to parse candle data: {candle_data}")
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
            if self.streamer_info is not None:
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
                if self.ws:
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
            try:
                timestamp_dt = pd.Timestamp(timestamp_ms, unit='ms', tz='US/Eastern')
                
                # Validate timestamp conversion was successful
                if pd.isna(timestamp_dt):
                    print(f"❌ Invalid timestamp conversion for {symbol}: {timestamp_ms}")
                    return
            except Exception as e:
                print(f"❌ Error converting timestamp {timestamp_ms}: {e}")
                return
            
            # Filter out 1-minute data before 9:31 AM ET
            if timeframe == '1m':
                try:
                    candle_time = timestamp_dt.time()
                    market_start_time = datetime.strptime('09:31', '%H:%M').time()
                    
                    if candle_time < market_start_time:
                        if self.debug:
                            print(f"⏰ Skipping 1m data before 9:31 AM: {timestamp_dt.strftime('%H:%M:%S %Z')} for {symbol}")
                        return
                except (AttributeError, ValueError) as e:
                    if self.debug:
                        print(f"⚠️ Invalid time component for {symbol}, skipping")
                    return
            
            # Create new row in the format expected by DataManager (matching CSV structure)
            new_row = pd.Series({
                'timestamp': timestamp_ms,  # Keep original milliseconds format
                'datetime': timestamp_dt.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'open': candle_data['open'],
                'high': candle_data['high'],
                'low': candle_data['low'],
                'close': candle_data['close'],
                'volume': candle_data['volume']
            })
                    
            # Fetch the DataFrame for the symbol and timeframe
            df = self.data_manager._get_dataframe(symbol, timeframe)
            df_inverse = self.data_manager._get_dataframe(f"{symbol}_inverse", timeframe)
            
            # Initialize DataFrame if it doesn't exist
            if df is None or len(df) == 0:
                print(f"📊 Initializing new DataFrame for {symbol} {timeframe}")
                df = pd.DataFrame(columns=pd.Index(['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume']))
                self.data_manager.latestDF[f"{symbol}_{timeframe}"] = df
            
            # Check if this candle already exists (avoid duplicates)
            if len(df) > 0:
                last_timestamp = df.iloc[-1]['timestamp']
                # Check if the new row is within 30 seconds of the last row
                if abs(new_row['timestamp'] - last_timestamp) < 30000:  # Within 30 seconds (30000 ms), likely duplicate
                    if self.debug:
                        print(f"🔄 Duplicate candle detected for {symbol}, skipping")
                    return
            
            # Create a DataFrame with just the new row for indicator calculation
            new_row_df = pd.DataFrame([new_row])  # Convert Series to DataFrame
            
            # Calculate indicators for the new data ONLY, using existing DataFrame as context
            result_df, index_of_first_new_row = self.data_manager.indicator_generator.calculate_real_time_indicators(symbol, timeframe, df, new_row_df)
            
            # Check if indicators were calculated properly
            if len(result_df) > 0:
                last_row = result_df.iloc[-1]
                if self.debug:
                    print(f"📊 Last row indicators - EMA: {last_row.get('ema', 'N/A')}, VWMA: {last_row.get('vwma', 'N/A')}, ROC: {last_row.get('roc', 'N/A')}, MACD Line: {last_row.get('macd_line', 'N/A')}, MACD Signal: {last_row.get('macd_signal', 'N/A')}")
                    
            # Generate inverse data
            new_inverse_df = self.data_manager._generate_inverse_data(new_row_df)

            # Calculate indicators for inverse data
            result_inverse_df, index_of_first_new_row_inverse = self.data_manager.indicator_generator.calculate_real_time_indicators(
                f"{symbol}_inverse", timeframe, df_inverse, new_inverse_df
            )

            # Process signals for original symbol
            self.data_manager.process_signals(symbol, timeframe, result_df, index_of_first_new_row, is_streaming=True)
            print(f"✅ Processed signals for {symbol} {timeframe}")

            # Process trading signals for the inverse symbol
            self.data_manager.process_signals(f"{symbol}_inverse", timeframe, result_inverse_df, index_of_first_new_row_inverse, is_streaming=True)
            print(f"✅ Processed signals for {symbol}_inverse {timeframe}")
                    
            # Save df to csv and update latest DataFrames
            self.data_manager.save_df_to_csv(result_df, symbol, timeframe)
            self.data_manager.save_df_to_csv(result_inverse_df, f"{symbol}_inverse", timeframe)
            self.data_manager.latestDF[f"{symbol}_{timeframe}"] = result_df
            self.data_manager.latestDF[f"{symbol}_inverse_{timeframe}"] = result_inverse_df
            print(f"✅ Generated data for {symbol} {timeframe} and {symbol}_inverse {timeframe} with {len(result_df)} records")

            # Handle higher timeframe aggregation for 1m data
            if timeframe == '1m':
                self.add_to_candle_buffer(symbol, timeframe)

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
            df_1m = self.data_manager._get_dataframe(symbol, '1m')
            
            if df_1m is None or len(df_1m) == 0:
                print(f"⚠️ No 1m data found for {symbol}, initializing empty buffer")
                self.candle_buffers[symbol] = {}
                for timeframe in self.timeframes[1:]:
                    self.candle_buffers[symbol][timeframe] = 0
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
                    self.candle_buffers[symbol][timeframe] = len(df_1m) % timeframe_minutes
                    if self.debug:
                        print(f"📊 {symbol} {timeframe}: {incomplete_count} incomplete candles in buffer")
                # No incomplete candles
                if symbol not in self.candle_buffers:
                    self.candle_buffers[symbol] = {}
                self.candle_buffers[symbol][timeframe] = 0
                if self.debug:
                    print(f"📊 {symbol} {timeframe}: no incomplete candles")
            print(f"✅ Initialized incomplete candle buffers for {symbol}")
            
        except Exception as e:
            print(f"❌ Error initializing candle buffer for {symbol}: {e}")
            if symbol not in self.candle_buffers:
                self.candle_buffers[symbol] = {}
            for timeframe in self.timeframes[1:]:
                self.candle_buffers[symbol][timeframe] = 0

    def add_to_candle_buffer(self, symbol: str, timeframe: str):
        """
        Check if any higher timeframe intervals are complete and need aggregation.
        """
        # Get the latest 1m candle timestamp
        df_1m = self.data_manager._get_dataframe(symbol, '1m')
        if df_1m is None or len(df_1m) == 0:
            return
            
        latest_timestamp_ms = df_1m['timestamp'].iloc[-1]
        try:
            latest_dt = pd.Timestamp(latest_timestamp_ms, unit='ms', tz='US/Eastern')
            if pd.isna(latest_dt):
                print(f"❌ Invalid timestamp in 1m data for {symbol}")
                return
            current_minute = latest_dt.minute
        except Exception as e:
            print(f"❌ Error processing timestamp in add_to_candle_buffer: {e}")
            return
        
        # Check each higher timeframe to see if an interval is complete
        for tf in self.timeframes:
            if tf == '1m':  # Skip 1m timeframe
                continue
                
            timeframe_minutes = int(tf.replace('m', ''))
            
            # Check if we're at the end of an interval
            # 1-minute candles: 9:30-9:31, 9:31-9:32, etc.
            # 5-minute intervals: 9:30-9:35, 9:35-9:40, etc.
            # 10-minute intervals: 9:30-9:40, 9:40-9:50, etc.
            
            # Calculate minutes since market open (9:30 AM)
            market_open_minute = 30  # 9:30 AM
            total_minutes = latest_dt.hour * 60 + latest_dt.minute
            market_open_total = 9 * 60 + market_open_minute
            minutes_since_open = total_minutes - market_open_total
            
            # Check if we're at the end of an interval (intervals complete at multiples of timeframe_minutes)
            if self.debug:
                print(f"🔍 {tf}: current_minute={current_minute}, minutes_since_open={minutes_since_open}, timeframe_minutes={timeframe_minutes}")
                print(f"🔍 {tf}: Check condition: minutes_since_open > 0 = {minutes_since_open > 0}, minutes_since_open % timeframe_minutes = {minutes_since_open % timeframe_minutes}")
                
            if minutes_since_open > 0 and minutes_since_open % timeframe_minutes == 0:
                if self.debug:
                    print(f"📊 {tf} interval complete at minute {current_minute}, triggering aggregation")
                
                # Check if we already have a candle for this interval in the higher timeframe data
                df_higher = self.data_manager._get_dataframe(symbol, tf)
                if self.debug:
                    print(f"🔍 {tf}: Checking existing data - df_higher is None: {df_higher is None}, df_higher length: {len(df_higher) if df_higher is not None else 'N/A'}")
                    
                if df_higher is not None and len(df_higher) > 0:
                    # Calculate the expected timestamp for this interval
                    interval_start_minute = (current_minute // timeframe_minutes) * timeframe_minutes
                    interval_start_dt = latest_dt.replace(minute=interval_start_minute, second=0, microsecond=0)
                    interval_end_dt = interval_start_dt + pd.Timedelta(minutes=timeframe_minutes)
                    expected_timestamp_ms = int(interval_end_dt.timestamp() * 1000) - 1000
                    
                    # Check if this interval already exists
                    if (df_higher['timestamp'] == expected_timestamp_ms).any():
                        if self.debug:
                            print(f"📊 {tf} candle for this interval already exists, skipping")
                        continue
                    else:
                        if self.debug:
                            print(f"🔍 {tf}: No existing candle found for timestamp {expected_timestamp_ms}, proceeding with aggregation")
                else:
                    if self.debug:
                        print(f"🔍 {tf}: No existing higher timeframe data, proceeding with aggregation")
                
                # Aggregate the candles
                if self.debug:
                    print(f"🔍 {tf}: Calling aggregate_candles for {symbol}")
                aggregated_candle = self.aggregate_candles(symbol, tf)
                if aggregated_candle:
                    print(f"📊 Processing aggregated {tf} candle for {symbol}")
                    if self.debug:
                        print(f"🔍 Aggregated candle data: {aggregated_candle}")
                    self.process_new_candle(symbol, tf, aggregated_candle)
                    
                    if self.debug:
                        print(f"✅ Aggregated {tf} candle for {symbol} processed successfully")
                else:
                    if self.debug:
                        print(f"📊 No aggregated candle generated for {symbol} {tf}")
            else:
                if self.debug:
                    print(f"📊 {tf} interval not complete yet (minute {current_minute}, need minute ending with {timeframe_minutes-1})")

    def aggregate_candles(self, symbol: str, timeframe: str) -> dict:
        """Aggregate 1m candles into higher timeframe candle with proper time alignment"""
        if self.debug:
            print(f"🔍 Checking aggregation for {symbol} {timeframe}")
            
        if symbol not in self.candle_buffers:
            if self.debug:
                print(f"📊 No candle buffers found for {symbol}")
            return {}
            
        if not self.candle_buffers[symbol]:
            if self.debug:
                print(f"📊 Empty candle buffers for {symbol}")
            return {}
        
        # Get timeframe in minutes
        timeframe_minutes = int(timeframe.replace('m', ''))

        df_1m = self.data_manager._get_dataframe(symbol, '1m')
        if df_1m is None or len(df_1m) == 0:
            if self.debug:
                print(f"📊 No 1m data available for {symbol}")
            return {}

        # Get the latest timestamp from 1m data
        latest_timestamp_ms = df_1m['timestamp'].iloc[-1]
        try:
            latest_dt = pd.Timestamp(latest_timestamp_ms, unit='ms', tz='US/Eastern')
            if pd.isna(latest_dt):
                print(f"❌ Invalid timestamp in 1m data for aggregation: {symbol}")
                return {}
        except Exception as e:
            print(f"❌ Error processing timestamp in aggregate_candles: {e}")
            return {}
        
        # Calculate the proper interval start time for the COMPLETED interval
        # For example, for 5m at 10:35, we want the interval 10:30-10:35 (just completed)
        # For 10m at 10:50, we want the interval 10:40-10:50 (just completed)
        minute = latest_dt.minute
        
        # Calculate minutes since market open to align with market hours
        market_open_minute = 30  # 9:30 AM
        total_minutes = latest_dt.hour * 60 + latest_dt.minute
        market_open_total = 9 * 60 + market_open_minute
        minutes_since_open = total_minutes - market_open_total
        
        # Find the start of the completed interval
        completed_intervals = minutes_since_open // timeframe_minutes
        interval_start_minutes_since_open = (completed_intervals - 1) * timeframe_minutes
        
        # Convert back to actual time
        interval_start_total_minutes = market_open_total + interval_start_minutes_since_open
        interval_start_hour = interval_start_total_minutes // 60
        interval_start_minute = interval_start_total_minutes % 60
        
        # Create the interval start datetime
        try:
            interval_start_dt = latest_dt.replace(hour=int(interval_start_hour), minute=int(interval_start_minute), second=0, microsecond=0)
            interval_end_dt = interval_start_dt + pd.Timedelta(minutes=timeframe_minutes)
            
            # Convert back to milliseconds for filtering
            interval_start_ms = int(interval_start_dt.timestamp() * 1000)
            interval_end_ms = int(interval_end_dt.timestamp() * 1000)
        except Exception as e:
            print(f"❌ Error calculating interval times: {e}")
            return {}

        if self.debug:
            print(f"📊 Aggregating {symbol} {timeframe}")
            print(f"📊 Latest 1m candle: {latest_dt.strftime('%H:%M:%S')}")
            print(f"📊 Interval: {interval_start_dt.strftime('%H:%M:%S')} to {interval_end_dt.strftime('%H:%M:%S')}")
        
        # Filter 1m candles within this interval
        mask = (df_1m['timestamp'] >= interval_start_ms) & (df_1m['timestamp'] < interval_end_ms)
        interval_candles = df_1m[mask]
        
        if interval_candles.empty:
            if self.debug:
                print(f"📊 No candles found in interval")
            return {}

        if self.debug:
            print(f"📊 Found {len(interval_candles)} candles in interval")
        
        # Check if we have enough candles for a complete interval
        if len(interval_candles) < timeframe_minutes:
            if self.debug:
                print(f"📊 Incomplete interval: {len(interval_candles)}/{timeframe_minutes} candles")
            return {}

        # Aggregate OHLCV data using the interval end time as timestamp
        aggregated_candle = {
            'timestamp': interval_end_ms - 1000,  # End of interval minus 1 second
            'open': interval_candles['open'].iloc[0],  # First candle's open
            'high': interval_candles['high'].max(),  # Highest high
            'low': interval_candles['low'].min(),    # Lowest low
            'close': interval_candles['close'].iloc[-1],  # Last candle's close
            'volume': interval_candles['volume'].sum()  # Total volume
        }
        
        if self.debug:
            print(f"📊 Aggregated {len(interval_candles)} candles for {symbol} {timeframe}")
            print(f"   Interval: {interval_start_dt.strftime('%H:%M')} - {interval_end_dt.strftime('%H:%M')}")
            print(f"   Result: O={aggregated_candle['open']:.2f} H={aggregated_candle['high']:.2f} L={aggregated_candle['low']:.2f} C={aggregated_candle['close']:.2f} V={aggregated_candle['volume']}")
        
        return aggregated_candle

    def send_daily_trades_email(self):
        """Send daily trades email and disconnect the stream"""
        try:
            print("📧 Sending daily trades email...")
            success = self.data_manager.signal_processor.email_daily_summary()
            
            if success:
                print("✅ Daily trades email sent successfully")
            else:
                print("❌ Failed to send daily trades email")
                
            # Flush any pending signal batches before disconnecting
            print("📧 Flushing any pending signal batches...")
            self.data_manager.signal_processor._send_all_pending_batches()
            
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
        print("✅ Bootstrap completed, now initializing candle buffers...")
        
        # Initialize candle buffers for all symbols after bootstrap
        for symbol in client.equity_symbols:
            client.initialize_candle_buffer(symbol)
        print("✅ Candle buffers initialized, now checking market hours...")
        
        # Check if it's before market hours and wait until 9:30 AM if needed
        if not client.wait_for_market_open():
            print("❌ Market is closed (weekend). Exiting...")
            exit(0)
        
        # Enable debug mode for streaming to see real-time processing
        client.debug = True
        print("🔍 Debug mode enabled for streaming")
        
        # Now connect to streaming after market is open
        print("🔌 Connecting to Schwab Streaming API...")
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