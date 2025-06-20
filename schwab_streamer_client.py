"""
This file contains the SchwabStreamerClient class, which is used to connect to the Schwab Streaming API and stream data to the aggregator.

It also contains the parse_chart_data function, which is used to parse the CHART_EQUITY data into a dictionary.

It also contains the get_user_preferences function, which is used to get the user preferences from the Schwab API.
"""
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

class SchwabStreamerClient:
    """Main client for Schwab Streaming API - OHLCV Data Collection"""
    
    def __init__(self, debug: bool = False, symbols_filepath: str = 'symbols.txt', timeframes_filepath: str = 'timeframe.txt'):
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        self.schwab_auth = SchwabAuth()
        # Read equity symbols from file path, timeframe from file path
        self.equity_symbols = self.get_symbols_from_file(symbols_filepath)
        self.timeframes = self.get_timeframes_from_file(timeframes_filepath)
        self.data_manager = DataManager(self.schwab_auth, self.equity_symbols, self.timeframes)

        self.daily_email_sent = False  # Track if 4pm email was sent today
        
        # Candle aggregation buffers for higher timeframes
        self.candle_buffers = {}  # Store 1m candles for aggregation
        self.last_aggregation_times = {}  # Track when we last aggregated for each timeframe
        
        self.debug = debug
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
    
    def get_symbols_from_file(self, symbols_filepath: str) -> List[str]:
        """Get symbols from file separated by commas or newlines"""
        with open(symbols_filepath, 'r') as file:
            content = file.read().strip()
            
            # Check if file uses comma separation or newline separation
            if ',' in content:
                symbols = content.split(',')
            else:
                symbols = content.split('\n')
                
            return [symbol.strip().upper() for symbol in symbols if symbol.strip()]

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
            
            # Reset daily flag at midnight
            if current_time.hour == 0 and current_time.minute == 0 and self.daily_email_sent:
                self.daily_email_sent = False
            
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

    

   

    def process_new_candle(self, symbol: str, candle_data: dict):
        """
        Process new candle data from streaming API
        - Add to CSV file and in-memory DataFrame
        - Generate indicators for new row
        - Process signals for new row
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
            
            # Process for 1m timeframe (streaming data is 1-minute candles)
            timeframe = '1m'
            
            # Add to candle buffer for aggregation to higher timeframes
            self.add_to_candle_buffer(symbol, candle_data)
            
            # Get the DataFrame key for caching
            df_key = self.data_manager._get_df_key(symbol, timeframe)
            
            # Load existing DataFrame or create new one
            if df_key in self.data_manager.latestDF:
                df = self.data_manager.latestDF[df_key].copy()
            else:
                # Load from CSV if exists
                df = self.data_manager._load_df_from_csv(symbol, timeframe)
                if df is None or df.empty:
                    # Create new DataFrame with proper columns (matching CSV structure)
                    df = pd.DataFrame(columns=['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume'])
            
            # Check if this candle already exists (avoid duplicates)
            if not df.empty and len(df) > 0:
                last_timestamp = df.iloc[-1]['timestamp']
                if abs(new_row['timestamp'] - last_timestamp) < 30000:  # Within 30 seconds (30000 ms), likely duplicate
                    if self.debug:
                        print(f"🔄 Duplicate candle detected for {symbol}, skipping")
                    return
            
            # Add new row to DataFrame
            df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
            
            # Ensure timestamp column is numeric (should already be milliseconds)
            if df['timestamp'].dtype == 'object':
                df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
            
            # Generate indicators for the new row only (incremental processing)
            try:
                # Get bootstrap parameters for the timeframe
                bootstrap_params = self.data_manager.get_bootstrap_parameters()
            
                if self.debug:
                    print(f"📊 Using bootstrap parameters for {symbol} {timeframe}:")
                    print(f"   EMA: {bootstrap_params['ema_period'].get(timeframe, 5)}")
                    print(f"   VWMA: {bootstrap_params['vwma_period'].get(timeframe, 16)}")
                    print(f"   ROC: {bootstrap_params['roc_period'].get(timeframe, 6)}")
                    print(f"   MACD: {bootstrap_params['fast_ema'].get(timeframe, 15)}/{bootstrap_params['slow_ema'].get(timeframe, 39)}/{bootstrap_params['signal_ema'].get(timeframe, 11)}")
                
                # Check if we have enough historical data for incremental processing
                roc_period = bootstrap_params['roc_period'].get(timeframe, 6)
                if len(df) < roc_period + 1:  # Need at least roc_period + 1 rows for ROC calculation
                    if self.debug:
                        print(f"📊 Insufficient data for incremental processing ({len(df)} rows < {roc_period + 1}), using bulk calculation")
                    # Use bulk calculation for the entire DataFrame
                    df_with_indicators = self.data_manager.indicator_generator.smart_indicator_calculation(
                        symbol, timeframe, df, None,  # Pass entire DataFrame for bulk processing
                        ema_period=bootstrap_params['ema_period'].get(timeframe, 7),
                        vwma_period=bootstrap_params['vwma_period'].get(timeframe, 17),
                        roc_period=bootstrap_params['roc_period'].get(timeframe, 8),
                        fast_ema=bootstrap_params['fast_ema'].get(timeframe, 12),
                        slow_ema=bootstrap_params['slow_ema'].get(timeframe, 26),
                        signal_ema=bootstrap_params['signal_ema'].get(timeframe, 9)
                    )
                else:
                    if self.debug:
                        print(f"📊 Using incremental processing ({len(df)} rows >= {roc_period + 1})")
                    # Use incremental processing with proper state management
                    # Check if we have more than one row to split properly
                    if len(df) == 1:
                        if self.debug:
                            print(f"📊 Only one row, using bulk calculation instead of incremental")
                        # Use bulk calculation for single row
                        df_with_indicators = self.data_manager.indicator_generator.smart_indicator_calculation(
                            symbol, timeframe, df, None,  # Pass entire DataFrame for bulk processing
                            ema_period=bootstrap_params['ema_period'].get(timeframe, 7),
                            vwma_period=bootstrap_params['vwma_period'].get(timeframe, 17),
                            roc_period=bootstrap_params['roc_period'].get(timeframe, 8),
                            fast_ema=bootstrap_params['fast_ema'].get(timeframe, 12),
                            slow_ema=bootstrap_params['slow_ema'].get(timeframe, 26),
                            signal_ema=bootstrap_params['signal_ema'].get(timeframe, 9)
                        )
                    else:
                        # Use incremental processing with proper state management
                        df_with_indicators = self.data_manager.indicator_generator.smart_indicator_calculation(
                            symbol, timeframe, df.iloc[:-1], df.iloc[-1:],  # Pass existing data and new row separately
                            ema_period=bootstrap_params['ema_period'].get(timeframe, 7),
                            vwma_period=bootstrap_params['vwma_period'].get(timeframe, 17),
                            roc_period=bootstrap_params['roc_period'].get(timeframe, 8),
                            fast_ema=bootstrap_params['fast_ema'].get(timeframe, 12),
                            slow_ema=bootstrap_params['slow_ema'].get(timeframe, 26),
                            signal_ema=bootstrap_params['signal_ema'].get(timeframe, 9)
                        )
            
            except Exception as e:
                print(f"⚠️  Error generating indicators for {symbol} {timeframe}: {e}")
                df_with_indicators = df
            
            # Update in-memory cache
            self.data_manager.latestDF[df_key] = df_with_indicators
            
            # Save to CSV efficiently (append only the new row)
            if len(df_with_indicators) > 0:
                latest_row_with_indicators = df_with_indicators.iloc[-1]
                self.data_manager.append_row_to_csv(latest_row_with_indicators, symbol, timeframe)
            # Process signals for the new row only (real-time processing)
            if len(df_with_indicators) > 0:
                latest_row = df_with_indicators.iloc[-1]
                # Use data manager's process_latest_signal which handles both original and inverse symbols
                self.data_manager.process_latest_signal(symbol, timeframe, latest_row)
                if self.debug:
                    print(f"📊 Processed signal for {symbol} {timeframe}: Close=${latest_row['close']:.2f}")
            
            # Process aggregated timeframes (5m, 10m, 15m, 30m) if it's time
            if not symbol.endswith('_inverse'):  # Only aggregate base symbols
                self.process_aggregated_timeframes(symbol, candle_data['timestamp'])
            
            # Print confirmation (only in debug mode to avoid spam)
            if self.debug:
                print(f"✅ Processed new candle: {symbol} {timeframe} @ {timestamp_dt.strftime('%H:%M:%S')} Close=${candle_data['close']:.2f}")

        except Exception as e:
            print(f"❌ Error processing new candle for {symbol}: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()

    def initialize_candle_buffer(self, symbol: str):
        """Initialize candle buffer for a symbol"""
        if symbol not in self.candle_buffers:
            self.candle_buffers[symbol] = []
        if symbol not in self.last_aggregation_times:
            self.last_aggregation_times[symbol] = {}
            for timeframe in ['5m', '10m', '15m', '30m']:
                self.last_aggregation_times[symbol][timeframe] = 0

    def add_to_candle_buffer(self, symbol: str, candle_data: dict):
        """Add 1m candle to buffer for aggregation"""
        self.initialize_candle_buffer(symbol)
        
        # Add candle with timestamp for sorting
        candle_with_ts = candle_data.copy()
        self.candle_buffers[symbol].append(candle_with_ts)
        
        # Keep only last 60 minutes of data (enough for 30m aggregation)
        if len(self.candle_buffers[symbol]) > 60:
            self.candle_buffers[symbol] = self.candle_buffers[symbol][-60:]
        
        if self.debug:
            print(f"📊 Added candle to buffer for {symbol}, buffer size: {len(self.candle_buffers[symbol])}")

    def should_aggregate(self, symbol: str, timeframe: str, current_timestamp: int) -> bool:
        """Check if we should aggregate for this timeframe based on time intervals"""
        if symbol not in self.last_aggregation_times:
            return True
            
        timeframe_minutes = int(timeframe.replace('m', ''))
        interval_ms = timeframe_minutes * 60 * 1000  # Convert to milliseconds
        
        last_aggregation = self.last_aggregation_times[symbol].get(timeframe, 0)
        
        # Check if enough time has passed for this timeframe
        time_since_last = current_timestamp - last_aggregation
        
        # Also check if we're at the right minute boundary
        timestamp_dt = pd.Timestamp(current_timestamp, unit='ms', tz='US/Eastern')
        current_minute = timestamp_dt.minute
        
        should_aggregate = False
        
        if timeframe == '5m':
            # Aggregate every 5 minutes (at minutes 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)
            should_aggregate = (current_minute % 5 == 0) and (time_since_last >= interval_ms * 0.8)
        elif timeframe == '10m':
            # Aggregate every 10 minutes (at minutes 0, 10, 20, 30, 40, 50)
            should_aggregate = (current_minute % 10 == 0) and (time_since_last >= interval_ms * 0.8)
        elif timeframe == '15m':
            # Aggregate every 15 minutes (at minutes 0, 15, 30, 45)
            should_aggregate = (current_minute % 15 == 0) and (time_since_last >= interval_ms * 0.8)
        elif timeframe == '30m':
            # Aggregate every 30 minutes (at minutes 0, 30)
            should_aggregate = (current_minute % 30 == 0) and (time_since_last >= interval_ms * 0.8)
        
        if should_aggregate and self.debug:
            print(f"⏰ Time to aggregate {symbol} {timeframe} at minute {current_minute}")
            
        return should_aggregate

    def aggregate_candles(self, symbol: str, timeframe: str, current_timestamp: int) -> dict:
        """Aggregate 1m candles into higher timeframe candle"""
        if symbol not in self.candle_buffers or not self.candle_buffers[symbol]:
            return None
        
        timeframe_minutes = int(timeframe.replace('m', ''))
        interval_ms = timeframe_minutes * 60 * 1000
        
        # Get candles within the aggregation window
        end_time = current_timestamp
        start_time = end_time - interval_ms
        
        # Filter candles in the time window
        relevant_candles = [
            candle for candle in self.candle_buffers[symbol]
            if start_time <= candle['timestamp'] <= end_time
        ]
        
        if not relevant_candles:
            return None
        
        # Sort by timestamp
        relevant_candles.sort(key=lambda x: x['timestamp'])
        
        # Aggregate OHLCV data
        aggregated_candle = {
            'timestamp': end_time,  # Use end time as the candle timestamp
            'open': relevant_candles[0]['open'],  # First candle's open
            'high': max(candle['high'] for candle in relevant_candles),  # Highest high
            'low': min(candle['low'] for candle in relevant_candles),    # Lowest low
            'close': relevant_candles[-1]['close'],  # Last candle's close
            'volume': sum(candle['volume'] for candle in relevant_candles)  # Total volume
        }
        
        if self.debug:
            print(f"📊 Aggregated {len(relevant_candles)} candles for {symbol} {timeframe}")
            print(f"   Result: O={aggregated_candle['open']:.2f} H={aggregated_candle['high']:.2f} L={aggregated_candle['low']:.2f} C={aggregated_candle['close']:.2f} V={aggregated_candle['volume']}")
        
        return aggregated_candle

    def process_aggregated_timeframes(self, symbol: str, current_timestamp: int):
        """Process aggregated candles for all higher timeframes"""
        higher_timeframes = ['5m', '10m', '15m', '30m']
        
        for timeframe in higher_timeframes:
            if self.should_aggregate(symbol, timeframe, current_timestamp):
                # Aggregate candles for this timeframe
                aggregated_candle = self.aggregate_candles(symbol, timeframe, current_timestamp)
                
                if aggregated_candle:
                    # Process the aggregated candle (this handles CSV, indicators, signals)
                    self.process_new_candle_for_timeframe(symbol, aggregated_candle, timeframe)
                    
                    # Update last aggregation time
                    self.last_aggregation_times[symbol][timeframe] = current_timestamp
                    
                    if self.debug:
                        print(f"✅ Processed aggregated {timeframe} candle for {symbol}")

    def process_new_candle_for_timeframe(self, symbol: str, candle_data: dict, timeframe: str):
        """
        Process new candle data for a specific timeframe
        - Add to CSV file and in-memory DataFrame
        - Generate indicators for new row
        - Process signals for new row
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
            
            # Get the DataFrame key for caching
            df_key = self.data_manager._get_df_key(symbol, timeframe)
            
            # Load existing DataFrame or create new one
            if df_key in self.data_manager.latestDF:
                df = self.data_manager.latestDF[df_key].copy()
            else:
                # Load from CSV if exists
                df = self.data_manager._load_df_from_csv(symbol, timeframe)
                if df is None or df.empty:
                    # Create new DataFrame with proper columns (matching CSV structure)
                    df = pd.DataFrame(columns=['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume'])
            
            # Check if this candle already exists (avoid duplicates)
            if not df.empty and len(df) > 0:
                last_timestamp = df.iloc[-1]['timestamp']
                timeframe_minutes = int(timeframe.replace('m', ''))
                interval_ms = timeframe_minutes * 60 * 1000
                if abs(new_row['timestamp'] - last_timestamp) < interval_ms * 0.5:  # Within half the interval
                    if self.debug:
                        print(f"🔄 Duplicate candle detected for {symbol} {timeframe}, skipping")
                    return
            
            # Add new row to DataFrame
            df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
            
            # Ensure timestamp column is numeric (should already be milliseconds)
            if df['timestamp'].dtype == 'object':
                df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
            
            # Generate indicators for the new row only (incremental processing)
            try:
                # Get bootstrap parameters for the timeframe
                bootstrap_params = self.data_manager.get_bootstrap_parameters()

            if self.debug:
                    print(f"📊 Using bootstrap parameters for {symbol} {timeframe}:")
                    print(f"   EMA: {bootstrap_params['ema_period'].get(timeframe, 7)}")
                    print(f"   VWMA: {bootstrap_params['vwma_period'].get(timeframe, 17)}")
                    print(f"   ROC: {bootstrap_params['roc_period'].get(timeframe, 8)}")
                    print(f"   MACD: {bootstrap_params['fast_ema'].get(timeframe, 12)}/{bootstrap_params['slow_ema'].get(timeframe, 26)}/{bootstrap_params['signal_ema'].get(timeframe, 9)}")
                
                # Check if we have enough historical data for incremental processing
                roc_period = bootstrap_params['roc_period'].get(timeframe, 6)
                if len(df) < roc_period + 1:  # Need at least roc_period + 1 rows for ROC calculation
                    if self.debug:
                        print(f"📊 Insufficient data for incremental processing ({len(df)} rows < {roc_period + 1}), using bulk calculation")
                    # Use bulk calculation for the entire DataFrame
                    df_with_indicators = self.data_manager.indicator_generator.smart_indicator_calculation(
                        symbol, timeframe, df, None,  # Pass entire DataFrame for bulk processing
                        ema_period=bootstrap_params['ema_period'].get(timeframe, 7),
                        vwma_period=bootstrap_params['vwma_period'].get(timeframe, 17),
                        roc_period=bootstrap_params['roc_period'].get(timeframe, 8),
                        fast_ema=bootstrap_params['fast_ema'].get(timeframe, 12),
                        slow_ema=bootstrap_params['slow_ema'].get(timeframe, 26),
                        signal_ema=bootstrap_params['signal_ema'].get(timeframe, 9)
                    )
                else:
                    if self.debug:
                        print(f"📊 Using incremental processing ({len(df)} rows >= {roc_period + 1})")
                    # Use incremental processing with proper state management
                    # Check if we have more than one row to split properly
                    if len(df) == 1:
                        if self.debug:
                            print(f"📊 Only one row, using bulk calculation instead of incremental")
                        # Use bulk calculation for single row
                        df_with_indicators = self.data_manager.indicator_generator.smart_indicator_calculation(
                            symbol, timeframe, df, None,  # Pass entire DataFrame for bulk processing
                            ema_period=bootstrap_params['ema_period'].get(timeframe, 7),
                            vwma_period=bootstrap_params['vwma_period'].get(timeframe, 17),
                            roc_period=bootstrap_params['roc_period'].get(timeframe, 8),
                            fast_ema=bootstrap_params['fast_ema'].get(timeframe, 12),
                            slow_ema=bootstrap_params['slow_ema'].get(timeframe, 26),
                            signal_ema=bootstrap_params['signal_ema'].get(timeframe, 9)
                        )
                    else:
                        # Use incremental processing with proper state management
                        df_with_indicators = self.data_manager.indicator_generator.smart_indicator_calculation(
                            symbol, timeframe, df.iloc[:-1], df.iloc[-1:],  # Pass existing data and new row separately
                            ema_period=bootstrap_params['ema_period'].get(timeframe, 7),
                            vwma_period=bootstrap_params['vwma_period'].get(timeframe, 17),
                            roc_period=bootstrap_params['roc_period'].get(timeframe, 8),
                            fast_ema=bootstrap_params['fast_ema'].get(timeframe, 12),
                            slow_ema=bootstrap_params['slow_ema'].get(timeframe, 26),
                            signal_ema=bootstrap_params['signal_ema'].get(timeframe, 9)
                        )
            
            except Exception as e:
                print(f"⚠️  Error generating indicators for {symbol} {timeframe}: {e}")
                df_with_indicators = df
            
            # Update in-memory cache
            self.data_manager.latestDF[df_key] = df_with_indicators
            
            # Save to CSV efficiently (append only the new row)
            if len(df_with_indicators) > 0:
                latest_row_with_indicators = df_with_indicators.iloc[-1]
                self.data_manager.append_row_to_csv(latest_row_with_indicators, symbol, timeframe)
            # Process signals for the new row only (real-time processing)
            if len(df_with_indicators) > 0:
                latest_row = df_with_indicators.iloc[-1]
                # Use data manager's process_latest_signal which handles both original and inverse symbols
                self.data_manager.process_latest_signal(symbol, timeframe, latest_row)
                if self.debug:
                    print(f"📊 Processed signal for {symbol} {timeframe}: Close=${latest_row['close']:.2f}")
            # Print confirmation (only in debug mode to avoid spam)
            if self.debug:
                print(f"✅ Processed new {timeframe} candle: {symbol} @ {timestamp_dt.strftime('%H:%M:%S')} Close=${candle_data['close']:.2f}")

        except Exception as e:
            print(f"❌ Error processing new {timeframe} candle for {symbol}: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()


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