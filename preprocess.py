import os
import pandas as pd
from indicators import Indicators
import requests
import logging

logger = logging.getLogger(__name__)

class Preprocess:
    def __init__(self, symbols, timeframes, ema_period, vwma_period, roc_period, macd_fast_period, macd_slow_period, macd_signal_period):
        self.symbols = symbols
        self.timeframes = timeframes
        self.ema_period = ema_period
        self.vwma_period = vwma_period
        self.roc_period = roc_period
        self.macd_fast_period = macd_fast_period
        self.macd_slow_period = macd_slow_period
        self.macd_signal_period = macd_signal_period

        # Load dataframes per symbol/timeframe (with indicators loaded or calculated)
        self.dfs = {}
        for symbol in symbols:
            for timeframe in timeframes:
                df = self.load_or_create_df(symbol, timeframe)
                self.dfs[(symbol, timeframe)] = df
                
        # Load or create Indicators manager
        self.indicators = Indicators(symbols, timeframes, ema_period, vwma_period, roc_period,
                                     macd_fast_period, macd_slow_period, macd_signal_period)


        # Placeholder for signals storage if needed
        self.signals = {}

    def load_or_create_df(self, symbol, timeframe):
        filepath = f'data/{timeframe}/{symbol}.csv'
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            # Convert timestamp
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            elif 'datetime' in df.columns:
                df['timestamp'] = pd.to_datetime(df['datetime'])

            # Check if indicators exist, if not, calculate them
            indicator = self.indicators.get_indicator(symbol, timeframe)

            # Assign dataframe to indicator's df for processing
            indicator.df = df

            # Check which indicators are missing and calculate accordingly
            missing_historical = indicator.check_missing_historical_indicators()
            missing_intraday = indicator.check_missing_intraday_indicators()

            if missing_historical:
                indicator.calculate_historical_indicators()
            if missing_intraday:
                indicator.calculate_intraday_indicators()

            # Save updated df with indicators if any calculation was done
            indicator.save_to_csv()

            return indicator.df
        else:
            # No file - create blank or minimal df or raise error
            print(f"File not found for {symbol} {timeframe}, initializing empty DataFrame")
            return pd.DataFrame()

    def fetch_latest(self, symbol, timeframe):
        """
        For each symbol/timeframe:
        - find last timestamp in current df
        - fetch new data after that timestamp
        - append new data and update indicators
        - save updated dataframe

        fetch_raw_data_func(symbol, timeframe, after_timestamp) -> pd.DataFrame
        """
        for (symbol, timeframe), df in self.dfs.items():
            if df.empty:
                print(f"No existing data for {symbol} {timeframe}, skipping fetch_latest.")
                continue

            last_timestamp = df['timestamp'].max()

            # Fetch new data after last_timestamp using user-provided function
            new_data = fetch(symbol, timeframe, last_timestamp)

            if new_data is None or new_data.empty:
                print(f"No new data for {symbol} {timeframe} after {last_timestamp}")
                continue

            # Convert timestamp in new_data if needed
            if 'timestamp' in new_data.columns and not pd.api.types.is_datetime64_any_dtype(new_data['timestamp']):
                new_data['timestamp'] = pd.to_datetime(new_data['timestamp'], unit='ms')
            elif 'datetime' in new_data.columns and not pd.api.types.is_datetime64_any_dtype(new_data['datetime']):
                new_data['timestamp'] = pd.to_datetime(new_data['datetime'])
                new_data.drop(columns=['datetime'], inplace=True)

            # Append new data to existing df
            updated_df = pd.concat([df, new_data], ignore_index=True)
            updated_df.drop_duplicates(subset='timestamp', inplace=True)
            updated_df.sort_values('timestamp', inplace=True)
            updated_df.reset_index(drop=True, inplace=True)

            # Update indicators for the combined dataframe
            indicator = self.indicators.get_indicator(symbol, timeframe)
            indicator.df = updated_df

            # Calculate indicators only for new rows if possible (or full recalculation if needed)
            indicator.update_with_new_data(new_data)

            # Save updated dataframe to CSV
            indicator.save_to_csv()

            # Update local copy in preprocess
            self.dfs[(symbol, timeframe)] = indicator.df

            print(f"Updated {symbol} {timeframe} with {len(new_data)} new rows.")

    def fetch_raw_data(self, symbol, timeframe, last_timestamp, period, frequencyType):
        try:
            url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
            access_token = self.auth.get_access_token()
            if not access_token:
                logger.error("No valid access token available.")
                return None
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {access_token}"
            }
            today_timestamp = pd.Timestamp.now(tz='US/Eastern').replace(hour=0, minute=0, second=0, microsecond=0)
            params = {
                "symbol": symbol,
                "periodType": "day",
                "period": period,
                "frequencyType": frequencyType,
                "frequency": int(timeframe.replace('m', '')),
                "startDate": last_timestamp,
                "endDate": today_timestamp,  # Use last complete minute as end date
            }

            # Print params and headers for debugging
            print(f"Request params: {params}")
            print(f"Request headers: {headers}")

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Print the response data for debugging
            print(f"Response data for {symbol}: {data}")

            # Convert to DataFrame
            df = pd.DataFrame(data.get('candles', []))
            if df.empty:
                logger.warning(f"No data received for {symbol}")
                return None

            logger.info(f"Received {len(df)} rows of data for {symbol}")

            # Convert datetime to pandas datetime for filtering
            df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
            df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(self.TIMEZONE)
            
            # Log the datetime range in the data
            logger.info(f"Data datetime range: {df['datetime'].min()} to {df['datetime'].max()}")
            
            # Filter for market hours (9:30 AM to 3:59 PM ET)
            df['time'] = df['datetime'].dt.time
            market_open = pd.to_datetime('09:30:00').time()
            market_close = pd.to_datetime('15:59:00').time()
            
            # Log the time range we're filtering for
            logger.info(f"Filtering for market hours: {market_open} to {market_close}")
            
            # Log some sample times from data
            logger.info(f"Sample times from data: {df['time'].head()}")
            
            # Apply market hours filter
            df = df[(df['time'] >= market_open) & (df['time'] <= market_close)]
            
            logger.info(f"After market hours filter: {len(df)} rows")
            
            # Drop the temporary time column
            df = df.drop('time', axis=1)
            
                        
            # Calculate the last complete minute (current minute - 1)
            current_time = pd.Timestamp.now(tz=self.TIMEZONE)
            last_complete_minute = current_time.floor('min') - pd.Timedelta(minutes=1)
            last_complete_timestamp = int(last_complete_minute.timestamp() * 1000)
            
            logger.info(f"Current time: {current_time}")
            logger.info(f"Last complete minute: {last_complete_minute}")
            logger.info(f"Last complete timestamp: {last_complete_timestamp}")
            

            # Filter out the current minute's data
            df = df[df['datetime'].dt.floor('min') < current_time.floor('min')]
            
            if df.empty:
                logger.warning(f"No completed candles to save for {symbol}")
                return None
                
            logger.info(f"After filtering current minute: {len(df)} rows")
            logger.info(f"Final datetime range: {df['datetime'].min()} to {df['datetime'].max()}")
            
            # Convert back to milliseconds for storage
            df['datetime'] = df['datetime'].dt.tz_convert('UTC').astype('int64') // 10**6
            df = df.rename(columns={'datetime': 'timestamp'})  # Rename datetime to timestamp

            # Export to CSV
            self.export_to_csv(df, symbol, timeframe)
            
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None