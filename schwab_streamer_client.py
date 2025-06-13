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
from datetime import datetime
import httpx
import websocket
import requests
from asset_manager import AssetManager
import pandas as pd

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
        self.timeframes = ["3m", "5m","10m", "15m", "30m"]
        # Get start time for 9:30am ET in current timezone
        start_time = pd.Timestamp.now(tz='US/Eastern').replace(hour=9, minute=30, second=0, microsecond=0)
        print(start_time)
        #Get 4:00pm ET in current timezone
        end_time = pd.Timestamp.now(tz='US/Eastern').replace(hour=16, minute=0, second=0, microsecond=0)    
        print(end_time)
        # Get 9:30am ET in epoch milliseconds
        start_date = int(start_time.timestamp() * 1000)
        # Get 4:00pm ET in epoch milliseconds
        end_date = int(end_time.timestamp() * 1000)
        self.asset_manager = AssetManager(self.auth, self.tracked_symbols, self.timeframes, self.start_date, self.end_date)
        
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
        parsed = {}
        
        for field_id, value in content_item.items():
            if field_id == 'key':
                parsed['symbol'] = value
            elif field_id.isdigit():
                field_num = int(field_id)
                field_name = self.chart_equity_fields.get(field_num, f'field_{field_num}')
                parsed[field_name] = value
            else:
                parsed[field_id] = value
                
        return parsed

    def on_message(self, ws, message):
        """Handle WebSocket messages"""
        try:
            data = json.loads(message)
            
            # Always show raw data for CHART_EQUITY messages
            if isinstance(data, dict) and "data" in data:
                for data_item in data["data"]:
                    service = data_item.get("service")
                    if service == "CHART_EQUITY":
                        print(f"\n🔍 RAW CHART_EQUITY DATA:")
                        print(f"   Service: {service}")
                        print(f"   Timestamp: {data_item.get('timestamp')}")
                        print(f"   Content: {data_item.get('content', [])}")
                        print(f"   Raw JSON: {json.dumps(data_item, indent=2)}")
                        print("-" * 80)
                    elif service == "LEVELONE_OPTIONS":
                        print(f"\n🔍 RAW LEVELONE_OPTIONS DATA:")
                        print(f"   Service: {service}")
                        print(f"   Timestamp: {data_item.get('timestamp')}")
                        print(f"   Content: {data_item.get('content', [])}")
                        print(f"   Raw JSON: {json.dumps(data_item, indent=2)}")
                        print("-" * 80)
            
            if self.debug:
                print(f"📨 Full Raw Message: {json.dumps(data, indent=2)}")
            
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
                        else:
                            print(f"❌ WebSocket login failed with code: {code}")
                            print(f"   Message: {content.get('msg', 'Unknown error')}")
                
                elif "notify" in data:
                    # Heartbeat or other notifications
                    notify_data = data["notify"][0]
                    if notify_data.get("heartbeat"):
                        if self.debug:
                            print("💓 Heartbeat received")
                
                elif "data" in data:
                    # Market data
                    for data_item in data["data"]:
                        service = data_item.get("service")
                        content = data_item.get("content", [])
                        
                        if service == "CHART_EQUITY" and content:
                            for candle in content:
                                # Check if this is a completed candle (not the current forming candle)
                                if candle.get("complete", False):
                                    # Format the candle data for the asset manager
                                    formatted_candle = {
                                        'timestamp': candle.get('timestamp', 0),
                                        'open': candle.get('open', 0),
                                        'high': candle.get('high', 0),
                                        'low': candle.get('low', 0),
                                        'close': candle.get('close', 0),
                                        'volume': candle.get('volume', 0)
                                    }
                                    
                                    symbol = candle.get("key")
                                    if symbol in self.tracked_symbols:
                                        self.asset_manager.add_new_candle(symbol, formatted_candle)
                                        if self.debug:
                                            print(f"Added completed candle for {symbol}: {formatted_candle}")
                                elif self.debug:
                                    print(f"Skipping incomplete candle for {candle.get('key')}")
                        
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
                                
                                if service == "CHART_EQUITY" and content_item:
                                    for candle in content_item:
                                        symbol = candle.get("key")
                                        if symbol in self.tracked_symbols:
                                            self.asset_manager.add_new_candle(symbol, candle)
                        
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            print(f"   Raw message: {message}")
        except Exception as e:
            print(f"❌ Message handling error: {e}")
            print(f"   Raw message: {message}")
            import traceback
            traceback.print_exc()

    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        print(f"❌ WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        print(f"🔌 WebSocket closed: {close_status_code} - {close_msg}")
        self.connected = False

    def on_open(self, ws):
        """Handle WebSocket open"""
        print("🔗 WebSocket connected, attempting login...")
        self.login()

    def login(self):
        """Login to the streaming service"""
        try:
            # Get user preferences which contain streaming info
            user_prefs = self.get_user_preferences()
            
            if self.debug:
                print(f"🔍 User preferences: {json.dumps(user_prefs, indent=2)}")
            
            # Get fresh access token for login
            access_token = self.auth.get_access_token()
            if not access_token:
                raise Exception("Failed to get access token for login")
            
            # Extract streaming info from user preferences
            stream_info = user_prefs.get("streamerInfo", [{}])
            if not stream_info:
                raise Exception("No streaming info found in user preferences")
            
            self.streamer_info = stream_info[0]  # Store for later use in subscriptions
            
            login_request = {
                "service": "ADMIN",
                "command": "LOGIN",
                "requestid": str(self.request_id),
                "SchwabClientCustomerId": self.streamer_info.get("schwabClientCustomerId", ""),
                "SchwabClientCorrelId": self.streamer_info.get("schwabClientCorrelId", ""),
                "parameters": {
                    "Authorization": access_token,
                    "SchwabClientChannel": self.streamer_info.get("schwabClientChannel", ""),
                    "SchwabClientFunctionId": self.streamer_info.get("schwabClientFunctionId", "")
                }
            }
            
            if self.debug:
                # Hide the full token in debug logs
                debug_request = json.loads(json.dumps(login_request))
                if len(debug_request["parameters"]["Authorization"]) > 20:
                    debug_request["parameters"]["Authorization"] = debug_request["parameters"]["Authorization"][:20] + "..."
                print(f"📤 Sending login: {json.dumps(debug_request, indent=2)}")
            
            self.ws.send(json.dumps(login_request))
            self.request_id += 1
            
        except Exception as e:
            print(f"❌ Login failed: {e}")

    def connect(self):
        """Connect to Schwab WebSocket API"""
        if not self.auth.is_authenticated():
            print("❌ Authentication failed. Cannot connect to WebSocket.")
            return False
        
        try:
            # Get user preferences first to get the correct WebSocket URL
            user_prefs = self.get_user_preferences()
            stream_info = user_prefs.get("streamerInfo", [{}])
            if not stream_info:
                raise Exception("No streaming info found in user preferences")
            
            streamer_info = stream_info[0]
            websocket_url = streamer_info.get("streamerSocketUrl", "wss://streamer-api.schwab.com/ws")
            
            if self.debug:
                print(f"🔗 Connecting to: {websocket_url}")
            
            websocket.enableTrace(self.debug)
            self.ws = websocket.WebSocketApp(
                websocket_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )
            
            # Start WebSocket in separate thread
            def run_ws():
                self.ws.run_forever()
            
            self.ws_thread = threading.Thread(target=run_ws, daemon=True)
            self.ws_thread.start()
            self.running = True
            
            # Wait a moment for connection
            time.sleep(2)
            return self.connected
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def subscribe_chart_data(self, symbols: List[str]):
        """Subscribe to CHART_EQUITY data (1-minute OHLCV candles)"""
        if not self.connected:
            print("❌ Not connected to WebSocket")
            return False
        
        if not self.streamer_info:
            print("❌ No streamer info available")
            return False
        
        try:
            keys = ",".join(symbols)
            fields = "0,1,2,3,4,5,6,7,8"
            
            request = {
                "service": "CHART_EQUITY",
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
                print(f"📤 Chart subscription: {json.dumps(request, indent=2)}")
            
            self.ws.send(json.dumps(request))
            self.subscriptions["CHART_EQUITY"] = symbols
            self.request_id += 1
            
            print(f"✅ Subscribed to CHART_EQUITY for: {symbols}")
            return True
            
        except Exception as e:
            print(f"❌ Chart subscription failed: {e}")
            return False

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
    
    try:
        # Connect to WebSocket
        print("🔄 Connecting to Schwab Streamer...")
        client.connect()
        
        # Get upcoming expirations for AAPL
        symbol = "AAPL"
        expirations = client.get_upcoming_expirations(symbol)
        print(f"\n📅 Next 6 expiration dates for {symbol}:")
        for i, exp in enumerate(expirations, 1):
            # Convert YYMMDD to a more readable format
            exp_date = datetime.strptime(exp, '%y%m%d')
            print(f"   {i}. {exp_date.strftime('%Y-%m-%d')} (YYMMDD: {exp})")
        
        # Subscribe to option chain for next expiration
        print(f"\n📊 Subscribing to option chain for {symbol}...")
        client.subscribe_option_chain(symbol, expirations[0])
        
        # Keep the script running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        client.disconnect()
    except Exception as e:
        print(f"❌ Error: {e}")
        client.disconnect()