from schwab_auth import SchwabAuth
import httpx
from typing import Optional, Dict, List
import os
import csv
from datetime import datetime

class OptionsManager:
    def __init__(self, auth: Optional[SchwabAuth] = None):
        self.auth = auth or SchwabAuth()
        self.csv_writers = {}  # Store CSV writers for each option symbol
        self.csv_files = {}    # Store file handles for each option symbol
        
    def ensure_options_data_directory(self):
        """Ensure the data/options directory exists."""
        os.makedirs('data/options', exist_ok=True)
        
    def get_csv_filename(self, option_symbol: str) -> str:
        """
        Get the CSV filename for an option symbol.
        
        Args:
            option_symbol (str): The option symbol (e.g., 'SPY   250620C00597000')
            
        Returns:
            str: The CSV filename
        """
        # Extract the symbol part for the filename
        # Remove spaces and special characters
        clean_symbol = option_symbol.replace(' ', '').replace('/', '_')
        return f"data/options/{clean_symbol}.csv"
        
    def initialize_csv_writer(self, option_symbol: str):
        """
        Initialize CSV writer for an option symbol.
        
        Args:
            option_symbol (str): The option symbol
        """
        self.ensure_options_data_directory()  # Ensure directory exists before writing
        try:
            filename = self.get_csv_filename(option_symbol)
            
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.exists(filename)
            
            # Open file in append mode
            csv_file = open(filename, 'a', newline='', encoding='utf-8')
            writer = csv.writer(csv_file)
            
            # Store references
            self.csv_files[option_symbol] = csv_file
            self.csv_writers[option_symbol] = writer
            
            # Write headers if file is new
            if not file_exists:
                headers = [
                    'timestamp', 'symbol', 'bid', 'ask', 'last', 'mark', 'bidSize', 'askSize',
                    'lastSize', 'highPrice', 'lowPrice', 'openPrice', 'closePrice', 'totalVolume',
                    'tradeTime', 'quoteTime', 'netChange', 'volatility', 'delta', 'gamma', 'theta',
                    'vega', 'rho', 'openInterest', 'timeValue', 'theoreticalOptionValue',
                    'theoreticalVolatility', 'strikePrice', 'expirationDate', 'daysToExpiration',
                    'multiplier', 'percentChange', 'markChange', 'markPercentChange',
                    'intrinsicValue', 'extrinsicValue', 'inTheMoney', 'pennyPilot'
                ]
                writer.writerow(headers)
                
            print(f"📄 Initialized CSV writer for {option_symbol} -> {filename}")
            
        except Exception as e:
            print(f"❌ Error initializing CSV writer for {option_symbol}: {e}")
            
    def save_option_data_to_csv(self, option_symbol: str, data: dict):
        """
        Save option data to CSV file, only for SPY and QQQ options, and map Schwab stream fields to the correct columns.
        """
        # Only track SPY and QQQ options
        if not (option_symbol.startswith('SPY') or option_symbol.startswith('QQQ')):
            return
        try:
            if option_symbol not in self.csv_writers:
                self.initialize_csv_writer(option_symbol)
            writer = self.csv_writers[option_symbol]
            # Map Schwab stream fields to your desired columns
            row = [
                datetime.now().isoformat(),
                data.get('underlying', ''),  # symbol
                data.get('2', ''),  # bid
                data.get('3', ''),  # ask
                data.get('4', ''),  # last
                data.get('37', ''), # mark
                data.get('16', ''), # bidSize
                data.get('17', ''), # askSize
                data.get('18', ''), # lastSize
                data.get('5', ''),  # highPrice
                data.get('6', ''),  # lowPrice
                data.get('15', ''), # openPrice
                data.get('7', ''),  # closePrice
                data.get('8', ''),  # totalVolume
                data.get('39', ''), # tradeTime
                data.get('38', ''), # quoteTime
                data.get('19', ''), # netChange
                data.get('10', ''), # volatility
                data.get('28', ''), # delta
                data.get('29', ''), # gamma
                data.get('30', ''), # theta
                data.get('31', ''), # vega
                data.get('32', ''), # rho
                data.get('9', ''),  # openInterest
                data.get('25', ''), # timeValue
                data.get('34', ''), # theoreticalOptionValue
                data.get('35', ''), # theoreticalVolatility
                data.get('20', ''), # strikePrice
                data.get('12', ''), # expirationDate (may need formatting)
                data.get('27', ''), # daysToExpiration
                data.get('13', ''), # multiplier
                data.get('44', ''), # percentChange
                data.get('45', ''), # markChange
                data.get('46', ''), # markPercentChange
                data.get('11', ''), # intrinsicValue
                data.get('25', ''), # extrinsicValue (may need calculation)
                data.get('33', ''), # inTheMoney
                data.get('48', ''), # pennyPilot
            ]
            writer.writerow(row)
            # Ensure data is written to disk immediately
            self.csv_files[option_symbol].flush()
        except Exception as e:
            print(f"❌ Error saving data to CSV for {option_symbol}: {e}")
            
    def close_csv_files(self):
        """Close all CSV files."""
        for symbol, csv_file in self.csv_files.items():
            try:
                csv_file.close()
                print(f"📄 Closed CSV file for {symbol}")
            except Exception as e:
                print(f"❌ Error closing CSV file for {symbol}: {e}")
        
        self.csv_files.clear()
        self.csv_writers.clear()
        

    def get_expiration_chain(self, symbol: str) -> List[Dict]:
        """
        Fetch the expiration chain for a given symbol from Schwab's API.
        
        Args:
            symbol (str): The stock symbol (e.g., 'SPY')
            
        Returns:
            List[Dict]: List of expiration dates and their details
        """
        try:
            # Get fresh token
            access_token = self.auth.get_access_token()
            if not access_token:
                raise Exception("Failed to get valid access token")
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }
            
            url = f"https://api.schwabapi.com/marketdata/v1/expirationchain?symbol={symbol}"
            
            with httpx.Client() as client:
                response = client.get(url, headers=headers)
                
            if response.status_code != 200:
                raise Exception(f"Expiration chain request failed: {response.status_code} - {response.text}")
            
            data = response.json()
            return data.get('expirationList', [])
            
        except Exception as e:
            print(f"❌ Error getting expiration chain: {e}")
            return []
    
    def find_expiration_date(self, symbol: str, days_to_expiration: int) -> Optional[str]:
        """
        Find the first expiration date that is greater than or equal to the specified days to expiration.
        
        Args:
            symbol (str): The stock symbol (e.g., 'SPY')
            days_to_expiration (int): Minimum number of days to expiration
            
        Returns:
            Optional[str]: The expiration date in YYYY-MM-DD format, or None if not found
        """
        try:
            expiration_chain = self.get_expiration_chain(symbol)
            if not expiration_chain:
                return None
            
            # Sort by days to expiration
            sorted_chain = sorted(expiration_chain, key=lambda x: x['daysToExpiration'])
            
            # Find the first expiration date that meets or exceeds our target
            for expiration in sorted_chain:
                if expiration['daysToExpiration'] >= days_to_expiration:
                    return expiration['expirationDate']
            
            # If no expiration date meets the criteria, return the furthest expiration
            return sorted_chain[-1]['expirationDate'] if sorted_chain else None
            
        except Exception as e:
            print(f"❌ Error finding expiration date: {e}")
            return None

    def get_option_expiration(self, symbol: str, days_to_expiration: int) -> Optional[str]:
        """
        Convenience function to get the expiration date for a given symbol and days to expiration.
        
        Args:
            symbol (str): The stock symbol (e.g., 'SPY')
            days_to_expiration (int): Minimum number of days to expiration
            
        Returns:
            Optional[str]: The expiration date in YYYY-MM-DD format, or None if not found
        """
        return self.find_expiration_date(symbol, days_to_expiration)

    def get_option_chains(self, symbol: str, expiration_date: str, range_type: str = "ITM") -> Dict:
        """
        Fetch option chains for a given symbol and expiration date.
        
        Args:
            symbol (str): The stock symbol (e.g., 'SPY')
            expiration_date (str): The expiration date in YYYY-MM-DD format
            range_type (str): Either "ITM" or "OTM"
            
        Returns:
            Dict: The option chain data
        """
        try:
            # Get fresh token
            access_token = self.auth.get_access_token()
            if not access_token:
                raise Exception("Failed to get valid access token")
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }
            
            url = f"https://api.schwabapi.com/marketdata/v1/chains"
            params = {
                'symbol': symbol,
                'contractType': 'ALL',
                'strikeCount': '3',
                'includeUnderlyingQuote': 'true',
                'range': range_type,
                'fromDate': expiration_date,
                'toDate': expiration_date
            }
            
            with httpx.Client() as client:
                response = client.get(url, headers=headers, params=params)
                
            if response.status_code != 200:
                raise Exception(f"Option chains request failed: {response.status_code} - {response.text}")
            
            return response.json()
            
        except Exception as e:
            print(f"❌ Error getting option chains: {e}")
            return {}

    def get_all_option_chains(self, symbol: str, expiration_date: str) -> Dict:
        """
        Fetch all option chains for a given symbol and expiration date.
        
        Args:
            symbol (str): The stock symbol (e.g., 'SPY')
            expiration_date (str): The expiration date in YYYY-MM-DD format
            
        Returns:
            Dict: The option chain data
        """
        try:
            # Get fresh token
            access_token = self.auth.get_access_token()
            if not access_token:
                raise Exception("Failed to get valid access token")
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }
            
            url = f"https://api.schwabapi.com/marketdata/v1/chains"
            params = {
                'symbol': symbol,
                'contractType': 'ALL',
                'strikeCount': '8',
                'fromDate': expiration_date,
                'toDate': expiration_date
            }
            
            with httpx.Client() as client:
                response = client.get(url, headers=headers, params=params)
                
            if response.status_code != 200:
                raise Exception(f"Option chains request failed: {response.status_code} - {response.text}")
            
            return response.json()
            
        except Exception as e:
            print(f"❌ Error getting option chains: {e}")
            return {}

    def get_option_symbols(self, symbol: str, expiration_date: str) -> Dict[str, List[str]]:
        """
        Get option symbols for the nearest strike price plus 3 strikes above and 3 strikes below for both calls and puts.
        
        Args:
            symbol (str): The stock symbol (e.g., 'SPY')
            expiration_date (str): The expiration date in YYYY-MM-DD format
            
        Returns:
            Dict[str, List[str]]: Dictionary with 'calls' and 'puts' lists containing option symbols for 7 strikes total
        """
        try:
            # Get all option chains in one call
            all_chains = self.get_all_option_chains(symbol, expiration_date)
            
            option_symbols = {
                'calls': [],
                'puts': []
            }
            
            # Get current underlying price to find nearest strike
            underlying_price = all_chains.get('underlyingPrice', 0)
            
            # Collect all available strikes for calls and puts
            call_strikes = []
            put_strikes = []
            
            # Extract all call strikes
            if 'callExpDateMap' in all_chains:
                for exp_key, strikes in all_chains['callExpDateMap'].items():
                    for strike, options in strikes.items():
                        strike_price = float(strike)
                        call_strikes.append(strike_price)
                        # Get the first call option for this strike
                        for option in options:
                            if option['putCall'] == 'CALL':
                                option_symbols['calls'].append(option['symbol'])
                                break
            
            # Extract all put strikes
            if 'putExpDateMap' in all_chains:
                for exp_key, strikes in all_chains['putExpDateMap'].items():
                    for strike, options in strikes.items():
                        strike_price = float(strike)
                        put_strikes.append(strike_price)
                        # Get the first put option for this strike
                        for option in options:
                            if option['putCall'] == 'PUT':
                                option_symbols['puts'].append(option['symbol'])
                                break
            
            # Sort strikes and find nearest strike
            call_strikes.sort()
            put_strikes.sort()
            
            # Find nearest strike for calls
            nearest_call_strike = min(call_strikes, key=lambda x: abs(x - underlying_price))
            nearest_call_index = call_strikes.index(nearest_call_strike)
            
            # Find nearest strike for puts
            nearest_put_strike = min(put_strikes, key=lambda x: abs(x - underlying_price))
            nearest_put_index = put_strikes.index(nearest_put_strike)
            
            # Get the 7 strikes for calls (nearest + 3 above + 3 below)
            call_start_index = max(0, nearest_call_index - 3)
            call_end_index = min(len(call_strikes), nearest_call_index + 4)
            selected_call_strikes = call_strikes[call_start_index:call_end_index]
            
            # Get the 7 strikes for puts (nearest + 3 above + 3 below)
            put_start_index = max(0, nearest_put_index - 3)
            put_end_index = min(len(put_strikes), nearest_put_index + 4)
            selected_put_strikes = put_strikes[put_start_index:put_end_index]
            
            # Clear and rebuild the option symbols lists with only the selected strikes
            option_symbols['calls'] = []
            option_symbols['puts'] = []
            
            # Get call symbols for selected strikes
            if 'callExpDateMap' in all_chains:
                for exp_key, strikes in all_chains['callExpDateMap'].items():
                    for strike, options in strikes.items():
                        strike_price = float(strike)
                        if strike_price in selected_call_strikes:
                            for option in options:
                                if option['putCall'] == 'CALL':
                                    option_symbols['calls'].append(option['symbol'])
                                    break
            
            # Get put symbols for selected strikes
            if 'putExpDateMap' in all_chains:
                for exp_key, strikes in all_chains['putExpDateMap'].items():
                    for strike, options in strikes.items():
                        strike_price = float(strike)
                        if strike_price in selected_put_strikes:
                            for option in options:
                                if option['putCall'] == 'PUT':
                                    option_symbols['puts'].append(option['symbol'])
                                    break
            
            return option_symbols
            
        except Exception as e:
            print(f"❌ Error getting option symbols: {e}")
            return {'calls': [], 'puts': []}

    def format_option_symbol_for_streaming(self, option_symbol: str) -> str:
        """
        Format option symbol to Schwab-standard format for LEVELONE_OPTIONS streaming.
        
        Schwab-standard option symbol format: RRRRRRYYMMDDsWWWWWddd
        Where:
        - R is the space-filled root symbol (6 characters)
        - YY is the expiration year
        - MM is the expiration month
        - DD is the expiration day
        - s is the side: C/P (call/put)
        - WWWWW is the whole portion of the strike price (5 characters)
        - ddd is the decimal portion of the strike price (3 characters)
        
        Args:
            option_symbol (str): Option symbol from API (e.g., 'SPY   250620C00597000')
            
        Returns:
            str: Formatted option symbol for streaming
        """
        try:
            # Parse the option symbol
            # Example: 'SPY   250620C00597000' -> 'SPY   ', '25', '06', '20', 'C', '00597', '000'
            
            # Extract root symbol (first 6 characters, space-padded)
            root = option_symbol[:6]
            
            # Extract expiration year (2 digits)
            year = option_symbol[6:8]
            
            # Extract expiration month (2 digits)
            month = option_symbol[8:10]
            
            # Extract expiration day (2 digits)
            day = option_symbol[10:12]
            
            # Extract option type (C or P)
            option_type = option_symbol[12]
            
            # Extract strike price (8 characters total: 5 whole + 3 decimal)
            strike_part = option_symbol[13:21]
            
            # Format the strike price properly
            # Remove leading zeros from whole part and ensure proper decimal format
            whole_part = strike_part[:5].lstrip('0').zfill(5)  # Ensure 5 digits
            decimal_part = strike_part[5:8]  # 3 decimal digits
            
            # Construct the formatted symbol
            formatted_symbol = f"{root}{year}{month}{day}{option_type}{whole_part}{decimal_part}"
            
            return formatted_symbol
            
        except Exception as e:
            print(f"❌ Error formatting option symbol {option_symbol}: {e}")
            return option_symbol

    def get_formatted_option_symbols(self, symbol: str, expiration_date: str) -> Dict[str, List[str]]:
        """
        Get option symbols formatted for LEVELONE_OPTIONS streaming.
        
        Args:
            symbol (str): The stock symbol (e.g., 'SPY')
            expiration_date (str): The expiration date in YYYY-MM-DD format
            
        Returns:
            Dict[str, List[str]]: Dictionary with 'calls' and 'puts' lists containing formatted option symbols
        """
        # Get raw option symbols
        raw_option_symbols = self.get_option_symbols(symbol, expiration_date)
        
        # Format them for streaming
        formatted_symbols = {
            'calls': [],
            'puts': []
        }
        
        # Format call symbols
        for call_symbol in raw_option_symbols['calls']:
            formatted_call = self.format_option_symbol_for_streaming(call_symbol)
            formatted_symbols['calls'].append(formatted_call)
        
        # Format put symbols
        for put_symbol in raw_option_symbols['puts']:
            formatted_put = self.format_option_symbol_for_streaming(put_symbol)
            formatted_symbols['puts'].append(formatted_put)
        
        return formatted_symbols

    def get_option_symbols_for_multiple_symbols(self, symbols: List[str], days_to_expiration: int) -> Dict[str, Dict[str, List[str]]]:
        """
        Get option symbols for multiple symbols with the same days to expiration.
        
        Args:
            symbols (List[str]): List of stock symbols (e.g., ['SPY', 'QQQ'])
            days_to_expiration (int): Minimum number of days to expiration
            
        Returns:
            Dict[str, Dict[str, List[str]]]: Dictionary with symbol as key and 'calls'/'puts' lists as values
        """
        all_option_symbols = {}
        
        for symbol in symbols:
            try:
                print(f"🔍 Processing {symbol}...")
                
                # Get expiration date for this symbol
                expiration_date = self.get_option_expiration(symbol, days_to_expiration)
                if not expiration_date:
                    print(f"❌ No suitable expiration date found for {symbol}")
                    continue
                
                print(f"📅 {symbol} expiration date: {expiration_date}")
                
                # Get formatted option symbols for this symbol
                option_symbols = self.get_formatted_option_symbols(symbol, expiration_date)
                
                if not option_symbols['calls'] and not option_symbols['puts']:
                    print(f"❌ No option symbols found for {symbol}")
                    continue
                
                all_option_symbols[symbol] = option_symbols
                print(f"✅ {symbol}: {len(option_symbols['calls'])} calls, {len(option_symbols['puts'])} puts")
                
            except Exception as e:
                print(f"❌ Error processing {symbol}: {e}")
                continue
        
        return all_option_symbols

    def stream_multiple_symbols_options(self, symbols: List[str], days_to_expiration: int, debug: bool = True):
        """
        Stream option data for multiple symbols with the same days to expiration.
        
        Args:
            symbols (List[str]): List of stock symbols (e.g., ['SPY', 'QQQ'])
            days_to_expiration (int): Minimum number of days to expiration
            debug (bool): Whether to enable debug mode for streaming
        """
        try:
            # Ensure data directory exists
            self.ensure_options_data_directory()
            
            print(f"🚀 Starting option streaming for {len(symbols)} symbols: {', '.join(symbols)}")
            
            # Get option symbols for all symbols
            all_option_symbols = self.get_option_symbols_for_multiple_symbols(symbols, days_to_expiration)
            
            if not all_option_symbols:
                print("❌ No option symbols found for any symbol")
                return
            
            # Collect all option symbols from all symbols
            all_option_symbols_flat = []
            symbol_mapping = {}  # Map option symbol back to original symbol
            
            for symbol, option_data in all_option_symbols.items():
                calls = option_data['calls']
                puts = option_data['puts']
                
                # Add calls and puts to flat list
                for call_symbol in calls:
                    all_option_symbols_flat.append(call_symbol)
                    symbol_mapping[call_symbol] = symbol
                
                for put_symbol in puts:
                    all_option_symbols_flat.append(put_symbol)
                    symbol_mapping[put_symbol] = symbol
                
                print(f"📊 {symbol}: {len(calls)} calls + {len(puts)} puts = {len(calls) + len(puts)} total options")
            
            print(f"📋 Total option symbols to stream: {len(all_option_symbols_flat)}")
            return all_option_symbols_flat
        except Exception as e:
            print(f"❌ Error during option streaming: {e}")
            return []

    def stream_options(self, symbols: list[str], days_to_expiration: int, debug: bool = True):
        """
        Stream option data for a single symbol and days to expiration using LEVELONE_OPTIONS service.
        
        Args:
            symbol (str): The stock symbol (e.g., 'SPY')
            days_to_expiration (int): Minimum number of days to expiration
            debug (bool): Whether to enable debug mode for streaming
        """
        # Use the multi-symbol function with a single symbol
        self.stream_multiple_symbols_options(symbols, days_to_expiration, debug)
