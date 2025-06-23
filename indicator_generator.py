import pandas as pd
import numpy as np
import os
from typing import Optional

class IndicatorGenerator:

    def __init__(self):
        # Store the last calculated indicator values for each symbol/timeframe combination
        self.last_values = {}
        # Maptimeframe to EMA, VWMA, ROC, FAST_EMA, SLOW_EMA, SIGNAL_EMA PERIODS
        self.periods = {
            '1m': {'ema': 5, 'vwma': 16, 'roc': 6, 'fast_ema': 15, 'slow_ema': 39, 'signal_ema': 11},
            '5m': {'ema': 7, 'vwma': 6, 'roc': 11, 'fast_ema': 21, 'slow_ema': 37, 'signal_ema': 15},
            '10m': {'ema': 9, 'vwma': 5, 'roc': 10, 'fast_ema': 16, 'slow_ema': 31, 'signal_ema': 10},
            '15m': {'ema': 6, 'vwma': 4, 'roc': 7, 'fast_ema': 14, 'slow_ema': 30, 'signal_ema': 10},
            '30m': {'ema': 6, 'vwma': 2, 'roc': 5, 'fast_ema': 22, 'slow_ema': 39, 'signal_ema': 12},
        }

    
    def initialize_indicators_state(self, symbol: str, timeframe: str, df: pd.DataFrame):
        # If DF exists, load state from it
        if df is None or df.empty:
            print(f"❌ No data available to initialize state for {symbol} {timeframe}")
            # Initialize state with default values
            self.last_values[(symbol, timeframe)] = self._initialize_empty_state()
            return
        self.load_state_from_dataframe(symbol, timeframe, df)

    def generate_all_indicators(self, symbol: str, timeframe: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process all indicators for a single symbol and timeframe using provided DataFrame.
        
        Args:
            symbol (str): Symbol name
            timeframe (str): Timeframe (e.g., '5m', '10m')
            df (pd.DataFrame): DataFrame with columns: ['time', 'open', 'high', 'low', 'close', 'volume']
            ema_period (int): EMA period
            vwma_period (int): VWMA period
            roc_period (int): ROC period
            fast_ema (int): Fast EMA for MACD
            slow_ema (int): Slow EMA for MACD
            signal_ema (int): Signal EMA for MACD
            
        Returns:
            pd.DataFrame: DataFrame with calculated indicators
        """
        try:
            # Remove existing indicator columns to prevent duplicates
            indicator_columns = [col for col in df.columns if any(col.startswith(prefix) for prefix in ['ema_', 'vwma_', 'roc_', 'macd_'])]
            if indicator_columns:
                df = df.drop(columns=indicator_columns)
                print(f"🔄 Removed {len(indicator_columns)} existing indicator columns")
            
            new_cols = {}
            ema_period = self.periods[symbol][timeframe]['ema']
            vwma_period = self.periods[symbol][timeframe]['vwma']
            roc_period = self.periods[symbol][timeframe]['roc']
            fast_ema = self.periods[symbol][timeframe]['fast_ema']
            slow_ema = self.periods[symbol][timeframe]['slow_ema']
            signal_ema = self.periods[symbol][timeframe]['signal_ema']
            
            # Calculate all EMAs
            if ema_period:
                new_cols[f'ema_{ema_period}'] = df['close'].ewm(span=ema_period, adjust=False).mean()
            
            # Calculate all VWMAs
            if vwma_period:
                new_cols[f'vwma_{vwma_period}'] = (
                    (df['close'] * df['volume']).rolling(window=vwma_period).sum() /
                    df['volume'].rolling(window=vwma_period).sum()
                )
            
            # Calculate all ROCs
            if roc_period:
                new_cols[f'roc_{roc_period}'] = df['close'].pct_change(periods=roc_period)
                # Debug ROC calculation in bulk processing
                debug_bulk_roc = True
                if debug_bulk_roc:
                    print(f"   [Bulk ROC Debug] Calculating ROC for {len(df)} rows with period {roc_period}")
                    print(f"   [Bulk ROC Debug] First few ROC values: {new_cols[f'roc_{roc_period}'].head(10).tolist()}")
                    print(f"   [Bulk ROC Debug] Last few ROC values: {new_cols[f'roc_{roc_period}'].tail(10).tolist()}")
                    print(f"   [Bulk ROC Debug] NaN count: {new_cols[f'roc_{roc_period}'].isna().sum()}")
            
            # Calculate all MACDs
            if fast_ema and slow_ema and signal_ema:
                macd_line = (
                    df['close'].ewm(span=fast_ema, adjust=False).mean() -
                    df['close'].ewm(span=slow_ema, adjust=False).mean()
                )
                new_cols[f'macd_line_{fast_ema}_{slow_ema}'] = macd_line
                new_cols[f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}'] = macd_line.ewm(span=signal_ema, adjust=False).mean()
            
            # Concatenate all new columns at once
            new_cols_df = pd.DataFrame(new_cols)
            result_df = pd.concat([df, new_cols_df], axis=1)
            result_df = result_df.copy()  # Defragment
            
            print(f"✅ Processed {symbol} {timeframe} with {len(result_df)} rows")
            
            return result_df
            
        except Exception as e:
            print(f"❌ Error processing {symbol} {timeframe}: {str(e)}")
            return df

    def calculate_real_time_indicators(self, symbol: str, timeframe: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate indicators for the last row only, using stored state.
        If stored state is invalid, try calculating from last X periods.
        If not enough candles, default to base case (None/0/empty).
        """
        key = (symbol, timeframe)
        last_vals = self.last_values[key]
        last_row_index = len(df) - 1
        
        # Get indicator periods
        ema_period = self.periods[symbol][timeframe]['ema']
        vwma_period = self.periods[symbol][timeframe]['vwma']
        roc_period = self.periods[symbol][timeframe]['roc']
        fast_ema = self.periods[symbol][timeframe]['fast_ema']
        slow_ema = self.periods[symbol][timeframe]['slow_ema']
        signal_ema = self.periods[symbol][timeframe]['signal_ema']
        
        # Calculate EMA
        if ema_period:
            if last_vals['ema'] is not None:
                # Valid stored state - use it
                alpha = 2.0 / (ema_period + 1)
                new_ema = alpha * df.iloc[-1]['close'] + (1 - alpha) * last_vals['ema']
                df.at[last_row_index, f'ema_{ema_period}'] = new_ema
                last_vals['ema'] = new_ema
            else:
                # Invalid state - try calculating from last X periods
                if len(df) >= ema_period:
                    # Enough data - calculate proper EMA
                    ema_series = df['close'].ewm(span=ema_period, adjust=False).mean()
                    df.at[last_row_index, f'ema_{ema_period}'] = ema_series.iloc[-1]
                    last_vals['ema'] = ema_series.iloc[-1]
                else:
                    # Not enough data - default to base case
                    df.at[last_row_index, f'ema_{ema_period}'] = None
                    last_vals['ema'] = None
        
        # Calculate VWMA
        if vwma_period:
            vwma_buffer = last_vals['vwma_buffer']
            
            # Add current row to buffer
            vwma_buffer.append((df.iloc[-1]['close'], df.iloc[-1]['volume']))
            
            # Keep only last vwma_period points
            if len(vwma_buffer) > vwma_period:
                vwma_buffer.pop(0)
            
            # Calculate VWMA
            if len(vwma_buffer) == vwma_period:
                # Full buffer - calculate proper VWMA
                price_volume_sum = sum(price * volume for price, volume in vwma_buffer)
                volume_sum = sum(volume for _, volume in vwma_buffer)
                vwma = price_volume_sum / volume_sum if volume_sum > 0 else None
            else:
                # Not enough data - default to base case
                vwma = None
            
            df.at[last_row_index, f'vwma_{vwma_period}'] = vwma
            last_vals['vwma_buffer'] = vwma_buffer
        
        # Calculate ROC
        if roc_period:
            roc_buffer = last_vals['roc_buffer']
            
            # Add current price to buffer
            roc_buffer.append(df.iloc[-1]['close'])
            
            # Keep only last roc_period points
            if len(roc_buffer) > roc_period:
                roc_buffer.pop(0)
            
            # Calculate ROC
            if len(roc_buffer) == roc_period:
                # Full buffer - calculate proper ROC
                roc = (roc_buffer[-1] - roc_buffer[0]) / roc_buffer[0]
            else:
                # Not enough data - default to base case
                roc = None
            
            df.at[last_row_index, f'roc_{roc_period}'] = roc
            last_vals['roc_buffer'] = roc_buffer
        
        # Calculate MACD
        if fast_ema and slow_ema and signal_ema:
            if (last_vals['macd_fast_ema'] is not None and 
                last_vals['macd_slow_ema'] is not None and 
                last_vals['macd_signal_ema'] is not None):
                # Valid stored state - use it
                alpha_fast = 2.0 / (fast_ema + 1)
                alpha_slow = 2.0 / (slow_ema + 1)
                alpha_signal = 2.0 / (signal_ema + 1)
                
                # Update EMAs
                last_vals['macd_fast_ema'] = alpha_fast * df.iloc[-1]['close'] + (1 - alpha_fast) * last_vals['macd_fast_ema']
                last_vals['macd_slow_ema'] = alpha_slow * df.iloc[-1]['close'] + (1 - alpha_slow) * last_vals['macd_slow_ema']
                
                # Calculate MACD line
                macd_line = last_vals['macd_fast_ema'] - last_vals['macd_slow_ema']
                df.at[last_row_index, f'macd_line_{fast_ema}_{slow_ema}'] = macd_line
                
                # Update MACD signal
                last_vals['macd_signal_ema'] = alpha_signal * macd_line + (1 - alpha_signal) * last_vals['macd_signal_ema']
                df.at[last_row_index, f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}'] = last_vals['macd_signal_ema']
            else:
                # Invalid state - try calculating from last X periods
                max_period = max(fast_ema, slow_ema, signal_ema)
                if len(df) >= max_period:
                    # Enough data - calculate proper MACD
                    fast_ema_series = df['close'].ewm(span=fast_ema, adjust=False).mean()
                    slow_ema_series = df['close'].ewm(span=slow_ema, adjust=False).mean()
                    
                    last_vals['macd_fast_ema'] = fast_ema_series.iloc[-1]
                    last_vals['macd_slow_ema'] = slow_ema_series.iloc[-1]
                    
                    # Calculate MACD line
                    macd_line = last_vals['macd_fast_ema'] - last_vals['macd_slow_ema']
                    df.at[last_row_index, f'macd_line_{fast_ema}_{slow_ema}'] = macd_line
                    
                    # Calculate MACD signal
                    macd_line_series = fast_ema_series - slow_ema_series
                    signal_series = macd_line_series.ewm(span=signal_ema, adjust=False).mean()
                    last_vals['macd_signal_ema'] = signal_series.iloc[-1]
                    df.at[last_row_index, f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}'] = last_vals['macd_signal_ema']
                else:
                    # Not enough data - default to base case
                    last_vals['macd_fast_ema'] = None
                    last_vals['macd_slow_ema'] = None
                    last_vals['macd_signal_ema'] = None
                    
                    df.at[last_row_index, f'macd_line_{fast_ema}_{slow_ema}'] = None
                    df.at[last_row_index, f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}'] = None

    def load_state_from_dataframe(self, symbol: str, timeframe: str, df: pd.DataFrame) -> bool:
        """
        Load state from an existing DataFrame to maintain continuity.
        
        Args:
            symbol (str): Symbol name
            timeframe (str): Timeframe (e.g., '5m', '10m')
            df (pd.DataFrame): DataFrame with existing data and indicators
            
        Returns:
            bool: True if state was loaded successfully, False otherwise
        """
        try:
            ema_period = self.periods[timeframe]['ema']
            vwma_period = self.periods[timeframe]['vwma']
            roc_period = self.periods[timeframe]['roc']
            fast_ema = self.periods[timeframe]['fast_ema']
            slow_ema = self.periods[timeframe]['slow_ema']
            signal_ema = self.periods[timeframe]['signal_ema']
            if len(df) == 0:
                return False
                
            key = (symbol, timeframe)
            
            # Initialize last values
            last_values = {
                'ema': None,
                'vwma_sum': 0,
                'vwma_volume_sum': 0,
                'vwma_buffer': [],
                'last_close': None,
                'roc_buffer': [],
                'macd_fast_ema': None,
                'macd_slow_ema': None,
                'macd_signal_ema': None
            }
            
            # Get the last row
            last_row = df.iloc[-1]
            
            # Load EMA value if it exists
            if ema_period and 'ema' in df.columns:
                last_values['ema'] = last_row['ema']
                print(f"📊 Loaded EMA_{ema_period} value: {last_values['ema']}")
            else:
                print(f"📊 No EMA_{ema_period} value found")
            # Load VWMA buffer - need to reconstruct from last few rows
            if vwma_period and 'vwma' in df.columns:
                # Get last vwma_period rows to reconstruct buffer
                last_n_rows = df.tail(vwma_period)
                vwma_buffer = []
                for _, row in last_n_rows.iterrows():
                    vwma_buffer.append((row['close'], row['volume']))
                last_values['vwma_buffer'] = vwma_buffer
                print(f"📊 Loaded VWMA_{vwma_period} buffer with {len(vwma_buffer)} points")
            else:
                print(f"📊 No VWMA_{vwma_period} value found")
            # Load ROC buffer - need to reconstruct from last roc_period rows
            if roc_period:
                # Get last roc_period rows to reconstruct buffer
                last_n_rows = df.tail(roc_period)
                roc_buffer = []
                for _, row in last_n_rows.iterrows():
                    roc_buffer.append(row['close'])
                last_values['roc_buffer'] = roc_buffer
                print(f"📊 Loaded ROC_{roc_period} buffer with {len(roc_buffer)} close prices")
            else:
                print(f"📊 No ROC_{roc_period} value found")
            # Load last close price
            last_values['last_close'] = last_row['close']
            print(f"📊 Loaded last close price: {last_values['last_close']}")
            
            # Load MACD values if they exist
            if fast_ema and slow_ema and signal_ema:
                macd_line_col = f'macd_line_{fast_ema}_{slow_ema}'
                macd_signal_col = f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}'
                
                if macd_line_col in df.columns and macd_signal_col in df.columns:
                    last_values['macd_signal_ema'] = last_row[macd_signal_col]
                    last_values['macd_fast_ema'] = last_row['close']  # Approximation
                    last_values['macd_slow_ema'] = last_row['close']  # Approximation
                    print(f"📊 Loaded MACD signal value: {last_values['macd_signal_ema']}")
            else:
                print(f"📊 No MACD values found")
            # Store the last values
            self.last_values[key] = last_values
            print(f"✅ Loaded existing state for {symbol} {timeframe}")
            return True
            
        except Exception as e:
            print(f"❌ Error loading state from DataFrame for {symbol} {timeframe}: {str(e)}")
            # Initialize state with default values
            self.last_values[(symbol, timeframe)] = self._initialize_empty_state()
            return False

    def _store_last_values_from_dataframe(self, symbol: str, timeframe: str, df: pd.DataFrame):
        """Helper function to store last values from a DataFrame."""
        ema_period = self.periods[symbol][timeframe]['ema']
        vwma_period = self.periods[symbol][timeframe]['vwma']
        roc_period = self.periods[symbol][timeframe]['roc']
        fast_ema = self.periods[symbol][timeframe]['fast_ema']
        slow_ema = self.periods[symbol][timeframe]['slow_ema']
        signal_ema = self.periods[symbol][timeframe]['signal_ema']
        key = (symbol, timeframe)
        last_values = {
            'ema': None,
            'vwma_sum': 0,
            'vwma_volume_sum': 0,
            'vwma_buffer': [],
            'last_close': None,
            'roc_buffer': [],
            'macd_fast_ema': None,
            'macd_slow_ema': None,
            'macd_signal_ema': None
        }
        
        # Store last close price
        last_values['last_close'] = df['close'].iloc[-1]
        
        # Store last EMA value
        if ema_period and f'ema_{ema_period}' in df.columns:
            last_values['ema'] = df[f'ema_{ema_period}'].iloc[-1]
        
        # Store VWMA buffer (last vwma_period rows)
        if vwma_period and f'vwma_{vwma_period}' in df.columns:
            last_n_rows = df.tail(vwma_period)
            vwma_buffer = []
            for _, row in last_n_rows.iterrows():
                vwma_buffer.append((row['close'], row['volume']))
            last_values['vwma_buffer'] = vwma_buffer
        
        # Store ROC buffer (last roc_period rows)
        if roc_period and f'roc_{roc_period}' in df.columns:
            last_n_rows = df.tail(roc_period)
            roc_buffer = []
            for _, row in last_n_rows.iterrows():
                roc_buffer.append(row['close'])
            last_values['roc_buffer'] = roc_buffer
        
        # Store MACD values
        if fast_ema and slow_ema and signal_ema:
            if f'macd_line_{fast_ema}_{slow_ema}' in df.columns and f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}' in df.columns:
                last_values['macd_signal_ema'] = df[f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}'].iloc[-1]
                last_values['macd_fast_ema'] = df['close'].iloc[-1]
                last_values['macd_slow_ema'] = df['close'].iloc[-1]
        
        # Store the last values
        self.last_values[key] = last_values
        print(f"💾 Stored last values for {symbol} {timeframe} calculations")

    def smart_indicator_calculation(self, symbol: str, timeframe: str, original_df: pd.DataFrame, 
                                   additional_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Smart indicator calculation that handles both bulk and incremental processing
        """
        # Treat None as empty DataFrame
        if original_df is None:
            original_df = pd.DataFrame()
        if additional_df is None:
            additional_df = pd.DataFrame()
        # If both are empty, return empty
        if (original_df.empty or len(original_df) == 0) and (additional_df.empty or len(additional_df) == 0):
            print(f"❌ Both original_df and additional_df are empty for {symbol} {timeframe}")
            return pd.DataFrame()
        key = (symbol, timeframe)
        try:
            # Only additional_df present
            if (original_df.empty or len(original_df) == 0) and (not additional_df.empty and len(additional_df) > 0):
                print(f"📊 Only additional_df present for {symbol} {timeframe}, processing bulk on additional_df")
                if key not in self.last_values:
                    self.initialize_indicators_state(symbol, timeframe, additional_df)
                result = self.generate_all_indicators(symbol, timeframe, additional_df)
                self._store_last_values_from_dataframe(symbol, timeframe, result)
                return result
            # Only original_df present
            if (not original_df.empty and len(original_df) > 0) and (additional_df.empty or len(additional_df) == 0):
                print(f"📊 Only original_df present for {symbol} {timeframe}, processing bulk on original_df")
                if key not in self.last_values:
                    self.initialize_indicators_state(symbol, timeframe, original_df)
                result = self.generate_all_indicators(symbol, timeframe, original_df)
                self._store_last_values_from_dataframe(symbol, timeframe, result)
                return result
            # Both present: process original first, then additional on top
            print(f"⚡ Incremental calculation for {symbol} {timeframe}")
            print(f"📊 Processing {len(original_df)} original + {len(additional_df)} additional data points")
            if key not in self.last_values:
                self.initialize_indicators_state(symbol, timeframe, original_df)
            # Bulk on original
            result = self.generate_all_indicators(symbol, timeframe, original_df)
            self._store_last_values_from_dataframe(symbol, timeframe, result)
            # Incremental on additional
            additional_with_indicators = self.calculate_real_time_indicators(
                symbol, timeframe, additional_df
            )
            final_df = pd.concat([result, additional_with_indicators], ignore_index=True)
            print(f"📈 Combined {len(result)} original + {len(additional_with_indicators)} additional = {len(final_df)} total")
            return final_df
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            # Return original_df as fallback
            return original_df

    def _initialize_empty_state(self) -> dict:
        """
        Helper function to initialize a new state for real-time calculations.
        """
        return {
            'ema': None,
            'vwma_sum': 0,
            'vwma_volume_sum': 0,
            'vwma_buffer': [],
            'last_close': None,
            'roc_buffer': [],
            'macd_fast_ema': None,
            'macd_slow_ema': None,
            'macd_signal_ema': None
        }

    def load_indicator_states(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Loads indicator state from data/{timeframe}/{symbol}.csv if it exists.
        Returns a pandas DataFrame or None if the file does not exist.
        """
        file_path = os.path.join('data', timeframe, f'{symbol}.csv')
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                return df
            except Exception as e:
                print(f"Error loading indicator state for {symbol} {timeframe}: {e}")
                return None
        return None

    def save_indicator_states(self, symbol: str, timeframe: str, df: pd.DataFrame):
        """
        Saves indicator state to data/{timeframe}/{symbol}.csv.
        """
        dir_path = os.path.join('data', timeframe)
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, f'{symbol}.csv')
        try:
            df.to_csv(file_path, index=False)
        except Exception as e:
            print(f"Error saving indicator state for {symbol} {timeframe}: {e}")

    def generate_indicators_incremental(self, df, symbol, timeframe):
        """
        Incrementally calculate indicators for new data, using previous state loaded from CSV if available.
        Ensures indicator continuity by always loading the last indicator row from CSV and initializing state.
        """
        # Always load previous state from CSV (not just if states is None)
        prev_df = self.load_indicator_states(symbol, timeframe)
        if prev_df is not None and not prev_df.empty:
            # Use the last row of the CSV to initialize state for incremental calculation
            self.load_state_from_dataframe(symbol, timeframe, prev_df)
        
        # If DataFrame is empty, nothing to do
        if df is None or len(df) == 0:
            return df
        
        key = (symbol, timeframe)
        last_ts = None
        if prev_df is not None and 'timestamp' in prev_df.columns and len(prev_df) > 0:
            last_ts = prev_df['timestamp'].iloc[-1]
        if last_ts is not None:
            # Only process new rows
            new_rows = df[df['timestamp'] > last_ts]
            if len(new_rows) == 0:
                return df  # No new data
            # Calculate indicators for new rows using the loaded state
            new_rows_with_ind = self.calculate_real_time_indicators(
                symbol, timeframe, new_rows
            )
            # Merge with old DataFrame (preserve all columns)
            df = pd.concat([df[df['timestamp'] <= last_ts], new_rows_with_ind], ignore_index=True)
        else:
            # No previous timestamp, process all
            df = self.generate_all_indicators(symbol, timeframe, df)
        # Save updated state
        self.save_indicator_states(symbol, timeframe, df)
        return df
