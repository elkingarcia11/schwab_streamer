import pandas as pd
import numpy as np
from typing import Tuple

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
            self.last_values[f"{symbol}_{timeframe}"] = self._initialize_empty_state()
            return
        if not self.load_state_from_dataframe(symbol, timeframe, df):
            print(f"❌ Failed to load state from DataFrame for {symbol} {timeframe}")
            # Initialize state with default values
            self.last_values[f"{symbol}_{timeframe}"] = self._initialize_empty_state()
            return
            
    def _initialize_empty_state(self) -> dict:
        """
        Helper function to initialize a new state for real-time calculations.
        """
        return {
            'ema': None,
            'vwma_buffer': [],
            'roc_buffer': [],
            'macd_fast_ema': None,
            'macd_slow_ema': None,
            'macd_signal_ema': None
        }

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
            print(f"🔍 [DEBUG] load_state_from_dataframe for {symbol} {timeframe}")
            print(f"🔍 [DEBUG] DataFrame shape: {df.shape}")
            print(f"🔍 [DEBUG] DataFrame columns: {list(df.columns)}")
            
            ema_period = self.periods[timeframe]['ema']
            vwma_period = self.periods[timeframe]['vwma']
            roc_period = self.periods[timeframe]['roc']
            fast_ema = self.periods[timeframe]['fast_ema']
            slow_ema = self.periods[timeframe]['slow_ema']
            signal_ema = self.periods[timeframe]['signal_ema']

            if len(df) == 0:
                print(f"🔍 [DEBUG] DataFrame is empty")
                return False
                
            key = f"{symbol}_{timeframe}"
            
            # Initialize last values
            last_values = {
                'ema': None,
                'vwma_buffer': [],
                'roc_buffer': [],
                'macd_fast_ema': None,
                'macd_slow_ema': None,
                'macd_signal_ema': None
            }
            
            # Get the last row
            last_row = df.iloc[-1]
            print(f"🔍 [DEBUG] Last row data: {last_row.to_dict()}")
            
            # Track if all required fields were successfully loaded
            all_fields_loaded = True
            
            # Load EMA value if it exists
            if ema_period and 'ema' in df.columns:
                ema_value = last_row['ema']
                if not pd.isna(ema_value):
                    last_values['ema'] = ema_value
                print(f"📊 Loaded EMA value: {last_values['ema']}")
            else:
                print(f"📊 No EMA column found")
                all_fields_loaded = False
                
            # Load VWMA buffer - need to reconstruct from last few rows
            if vwma_period and 'vwma' in df.columns:
                # Get last vwma_period rows to reconstruct buffer
                last_n_rows = df.tail(vwma_period)
                vwma_buffer = []
                for _, row in last_n_rows.iterrows():
                    if not pd.isna(row['close']) and not pd.isna(row['volume']):
                        vwma_buffer.append((row['close'], row['volume']))
                last_values['vwma_buffer'] = vwma_buffer
                print(f"📊 Loaded VWMA buffer with {len(vwma_buffer)} points")
                if len(vwma_buffer) == 0:
                    all_fields_loaded = False
            else:
                print(f"📊 No VWMA column found")
                all_fields_loaded = False
                
            # Load ROC buffer - need to reconstruct from last roc_period rows
            if roc_period:
                # Get last roc_period rows to reconstruct buffer
                last_n_rows = df.tail(roc_period)
                roc_buffer = []
                for _, row in last_n_rows.iterrows():
                    if not pd.isna(row['close']):
                        roc_buffer.append(row['close'])
                last_values['roc_buffer'] = roc_buffer
                print(f"📊 Loaded ROC buffer with {len(roc_buffer)} close prices")
                if len(roc_buffer) == 0:
                    all_fields_loaded = False
            else:
                print(f"📊 No ROC period defined")
                all_fields_loaded = False
                
            # Load MACD values if they exist
            if fast_ema and slow_ema and signal_ema:
                macd_line_col = 'macd_line'
                macd_signal_col = 'macd_signal'
                
                if macd_line_col in df.columns and macd_signal_col in df.columns:
                    signal_value = last_row[macd_signal_col]
                    if not pd.isna(signal_value):
                        last_values['macd_signal_ema'] = signal_value
                    last_values['macd_fast_ema'] = last_row['close']  # Approximation
                    last_values['macd_slow_ema'] = last_row['close']  # Approximation
                    print(f"📊 Loaded MACD signal value: {last_values['macd_signal_ema']}")
                else:
                    print(f"📊 No MACD columns found")
                    all_fields_loaded = False
            else:
                print(f"📊 No MACD periods defined")
                all_fields_loaded = False
                
            # Store the last values
            self.last_values[key] = last_values
            print(f"🔍 [DEBUG] Stored last_values for {key}: {last_values}")
            
            if all_fields_loaded:
                print(f"✅ Loaded existing state for {symbol} {timeframe}")
                return True
            else:
                print(f"⚠️ Incomplete state loaded for {symbol} {timeframe}, some fields missing")
                return False
            
        except Exception as e:
            print(f"❌ Error loading state from DataFrame for {symbol} {timeframe}: {str(e)}")
            # Initialize state with default values
            self.last_values[f"{symbol}_{timeframe}"] = self._initialize_empty_state()
            return False
    
    def calculate_real_time_indicators(self, symbol: str, timeframe: str, df: pd.DataFrame, new_df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """
        Calculate indicators for new rows only, using stored state.
        If stored state is invalid, try generate all indicators from scratch
        """
        key = f"{symbol}_{timeframe}"
        
        # Check if key exists in last_values
        if key not in self.last_values:
            print(f"❌ No state found for {symbol} {timeframe}, generating all indicators from scratch")
            # Combine df and new_df
            combined_df = pd.concat([df, new_df])
            combined_df_with_indicators = self.generate_indicators(symbol, timeframe, combined_df)
            self._update_last_values_from_dataframe(symbol, timeframe, combined_df_with_indicators)
            return combined_df_with_indicators, len(df)
        
        last_vals = self.last_values[key]
        
        # If df is empty, then we need to generate all indicators from scratch
        if df.empty:
            print(f"❌ Empty DataFrame for {symbol} {timeframe}, generating all indicators from scratch")
            new_df_with_indicators = self.generate_indicators(symbol, timeframe, new_df)
            self._update_last_values_from_dataframe(symbol, timeframe, new_df_with_indicators)
            return new_df_with_indicators, 0
        
        # Check if critical values are None or empty (EMA and MACD signal are critical for incremental calculation)
        ema_val = last_vals.get('ema')
        macd_signal_val = last_vals.get('macd_signal_ema')
        vwma_buffer = last_vals.get('vwma_buffer', [])
        roc_buffer = last_vals.get('roc_buffer', [])
        
        # Helper function to check if a value is None or NaN
        def is_none_or_nan(val):
            if val is None:
                return True
            try:
                return pd.isna(val)
            except:
                return False
        
        critical_missing = (
            is_none_or_nan(ema_val) or
            is_none_or_nan(macd_signal_val) or
            not vwma_buffer or len(vwma_buffer) == 0 or
            not roc_buffer or len(roc_buffer) == 0
        )
        
        if critical_missing:
            print(f"❌ Critical indicator state missing for {symbol} {timeframe}, generating all indicators from scratch")
            print(f"   EMA: {last_vals.get('ema')}, MACD Signal: {last_vals.get('macd_signal_ema')}")
            print(f"   VWMA buffer length: {len(last_vals.get('vwma_buffer', []))}, ROC buffer length: {len(last_vals.get('roc_buffer', []))}")
            # Combine df and new_df
            combined_df = pd.concat([df, new_df])
            combined_df_with_indicators = self.generate_indicators(symbol, timeframe, combined_df)
            # Update the stored last_values with the new state
            self._update_last_values_from_dataframe(symbol, timeframe, combined_df_with_indicators) 
            return combined_df_with_indicators, len(df)
        else:
            print(f"✅ Valid state for {symbol} {timeframe}, calculating indicators incrementally for new rows")
            # Calculate indicators incrementally for new rows, then combine with df
            new_df_with_indicators = self.generate_indicators_incremental(symbol, timeframe, new_df, last_vals)
            combined_df = pd.concat([df, new_df_with_indicators])
            
            # Update the stored last_values with the new state
            self._update_last_values_from_dataframe(symbol, timeframe, combined_df)
        
        # Return index of first row of new_df
        return combined_df, len(df)

    def generate_indicators_incremental(self, symbol: str, timeframe: str, df: pd.DataFrame, last_vals: dict) -> pd.DataFrame:
        """
        Calculate indicators incrementally using stored last_values for efficiency.
        This method calculates indicator values for all new data points efficiently.
        
        Args:
            symbol (str): Symbol name
            timeframe (str): Timeframe (e.g., '5m', '10m')
            df (pd.DataFrame): DataFrame with new data only
            last_vals (dict): Stored last values from previous calculations
            
        Returns:
            pd.DataFrame: DataFrame with calculated indicators for all new data points
        """
        try:
            ema_period = self.periods[timeframe]['ema']
            vwma_period = self.periods[timeframe]['vwma']
            roc_period = self.periods[timeframe]['roc']
            fast_ema = self.periods[timeframe]['fast_ema']
            slow_ema = self.periods[timeframe]['slow_ema']
            signal_ema = self.periods[timeframe]['signal_ema']
            
            # Debug output only for streaming (not bootstrap)
            # print(f"🔍 [DEBUG] Incremental calculation for {symbol} {timeframe}")
            # print(f"🔍 [DEBUG] New data points: {len(df)}")
            # print(f"🔍 [DEBUG] Stored buffers - VWMA: {len(last_vals.get('vwma_buffer', []))}, ROC: {len(last_vals.get('roc_buffer', []))}")
            
            # Pre-allocate result DataFrame with original data
            result_df = df.copy()
            close_prices = df['close'].values
            volumes = df['volume'].values
            num_rows = len(close_prices)
            
            # Initialize all indicator columns with NaN
            result_df['ema'] = np.nan
            result_df['vwma'] = np.nan
            result_df['roc'] = np.nan
            result_df['macd_line'] = np.nan
            result_df['macd_signal'] = np.nan
            
            # Calculate EMA incrementally (only needs previous EMA value)
            if ema_period and last_vals['ema'] is not None and not pd.isna(last_vals['ema']):
                alpha = 2.0 / (ema_period + 1)
                current_ema = last_vals['ema']
                ema_values = np.zeros(num_rows)
                
                for i, close_price in enumerate(close_prices):
                    current_ema = alpha * close_price + (1 - alpha) * current_ema
                    ema_values[i] = current_ema
                
                result_df['ema'] = ema_values
                # print(f"📊 Calculated incremental EMA for {num_rows} new rows")
            else:
                pass
                # print(f"📊 Cannot calculate EMA - missing previous EMA value")
            
            # Calculate VWMA incrementally (needs buffer of previous prices/volumes)
            if vwma_period and last_vals['vwma_buffer'] and len(last_vals['vwma_buffer']) > 0:
                vwma_buffer = last_vals['vwma_buffer'].copy()
                vwma_values = np.zeros(num_rows)
                
                for i, (close_price, volume) in enumerate(zip(close_prices, volumes)):
                    # Add new point to buffer
                    vwma_buffer.append((close_price, volume))
                    if len(vwma_buffer) > vwma_period:
                        vwma_buffer.pop(0)
                    
                    # Calculate VWMA for this point (use available data even if less than full period)
                    if len(vwma_buffer) >= 2:  # Need at least 2 points for meaningful VWMA
                        prices = np.array([p for p, _ in vwma_buffer])
                        vols = np.array([v for _, v in vwma_buffer])
                        weighted_sum = np.sum(prices * vols)
                        volume_sum = np.sum(vols)
                        vwma = weighted_sum / volume_sum if volume_sum > 0 else close_price
                    else:
                        vwma = close_price  # Use current close if not enough data
                    
                    vwma_values[i] = vwma
                
                result_df['vwma'] = vwma_values
                # print(f"📊 Calculated incremental VWMA for {num_rows} new rows")
            else:
                pass
                # print(f"📊 Cannot calculate VWMA - missing previous buffer")
            
            # Calculate ROC incrementally (needs buffer of previous prices)
            if roc_period and last_vals['roc_buffer'] and len(last_vals['roc_buffer']) > 0:
                roc_buffer = last_vals['roc_buffer'].copy()
                roc_values = np.zeros(num_rows)
                
                for i, close_price in enumerate(close_prices):
                    # Add new close price to buffer
                    roc_buffer.append(close_price)
                    if len(roc_buffer) > roc_period + 1:
                        roc_buffer.pop(0)
                    
                    # Calculate ROC for this point (use available data even if less than full period)
                    if len(roc_buffer) >= 2:  # Need at least 2 points for ROC
                        current_price = roc_buffer[-1]
                        past_price = roc_buffer[0]
                        roc = (current_price - past_price) / past_price if past_price != 0 else 0
                    else:
                        roc = 0  # Not enough data yet
                    
                    roc_values[i] = roc
                
                result_df['roc'] = roc_values
                # print(f"📊 Calculated incremental ROC for {num_rows} new rows")
            else:
                pass
                # print(f"📊 Cannot calculate ROC - missing previous buffer")
            
            # Calculate MACD incrementally (needs previous EMA values)
            if (fast_ema and slow_ema and signal_ema and 
                last_vals['macd_signal_ema'] is not None and 
                not pd.isna(last_vals['macd_signal_ema'])):
                
                # Initialize EMAs from last values or use current close as fallback
                fast_ema_val = last_vals.get('macd_fast_ema', close_prices[0]) if last_vals.get('macd_fast_ema') is not None else close_prices[0]
                slow_ema_val = last_vals.get('macd_slow_ema', close_prices[0]) if last_vals.get('macd_slow_ema') is not None else close_prices[0]
                signal_ema_val = last_vals['macd_signal_ema']
                
                # Pre-calculate alpha values
                fast_alpha = 2.0 / (fast_ema + 1)
                slow_alpha = 2.0 / (slow_ema + 1)
                signal_alpha = 2.0 / (signal_ema + 1)
                
                macd_line_values = np.zeros(num_rows)
                macd_signal_values = np.zeros(num_rows)
                
                for i, close_price in enumerate(close_prices):
                    # Update EMAs
                    fast_ema_val = fast_alpha * close_price + (1 - fast_alpha) * fast_ema_val
                    slow_ema_val = slow_alpha * close_price + (1 - slow_alpha) * slow_ema_val
                    
                    # Calculate MACD line
                    macd_line = fast_ema_val - slow_ema_val
                    macd_line_values[i] = macd_line
                    
                    # Update signal line
                    signal_ema_val = signal_alpha * macd_line + (1 - signal_alpha) * signal_ema_val
                    macd_signal_values[i] = signal_ema_val
                
                result_df['macd_line'] = macd_line_values
                result_df['macd_signal'] = macd_signal_values
                # print(f"📊 Calculated incremental MACD for {num_rows} new rows")
            else:
                pass
                # print(f"📊 Cannot calculate MACD - missing previous signal EMA")
            
            # print(f"✅ Incrementally processed {symbol} {timeframe} with {num_rows} new rows")
            # print(f"🔍 [DEBUG] Result indicators - EMA: {result_df['ema'].iloc[-1] if not pd.isna(result_df['ema'].iloc[-1]) else 'NaN'}, VWMA: {result_df['vwma'].iloc[-1] if not pd.isna(result_df['vwma'].iloc[-1]) else 'NaN'}, ROC: {result_df['roc'].iloc[-1] if not pd.isna(result_df['roc'].iloc[-1]) else 'NaN'}")
            return result_df
            
        except Exception as e:
            print(f"❌ Error in incremental processing {symbol} {timeframe}: {str(e)}")
            # Return DataFrame with NaN indicators as fallback
            result_df = df.copy()
            result_df['ema'] = np.nan
            result_df['vwma'] = np.nan
            result_df['roc'] = np.nan
            result_df['macd_line'] = np.nan
            result_df['macd_signal'] = np.nan
            return result_df

    def _update_last_values_from_dataframe(self, symbol: str, timeframe: str, df: pd.DataFrame):
        """Update stored last_values with the latest values from the DataFrame."""
        ema_period = self.periods[timeframe]['ema']
        vwma_period = self.periods[timeframe]['vwma']
        roc_period = self.periods[timeframe]['roc']
        fast_ema = self.periods[timeframe]['fast_ema']
        slow_ema = self.periods[timeframe]['slow_ema']
        signal_ema = self.periods[timeframe]['signal_ema']
        key = f"{symbol}_{timeframe}"
        
        # Initialize with default values
        last_values = {
            'ema': None,
            'vwma_buffer': [],
            'roc_buffer': [],
            'macd_fast_ema': None,
            'macd_slow_ema': None,
            'macd_signal_ema': None
        }
        
        # Get the last row once
        last_row = df.iloc[-1]
        
        # Store last EMA value
        if ema_period and 'ema' in df.columns:
            last_values['ema'] = last_row['ema']
        
        # Store VWMA buffer (last vwma_period rows) - optimized
        if vwma_period and 'vwma' in df.columns:
            # Use tail() once and convert to list efficiently
            last_n_rows = df.tail(vwma_period)
            vwma_buffer = list(zip(last_n_rows['close'].values, last_n_rows['volume'].values))
            last_values['vwma_buffer'] = vwma_buffer
        
        # Store ROC buffer (last roc_period rows) - optimized
        if roc_period and 'roc' in df.columns:
            # Use tail() once and convert to list efficiently
            last_n_rows = df.tail(roc_period)
            roc_buffer = last_n_rows['close'].tolist()
            last_values['roc_buffer'] = roc_buffer
        
        # Store MACD values
        if fast_ema and slow_ema and signal_ema:
            if 'macd_line' in df.columns and 'macd_signal' in df.columns:
                last_values['macd_signal_ema'] = last_row['macd_signal']
                last_values['macd_fast_ema'] = last_row['close']  # Approximation
                last_values['macd_slow_ema'] = last_row['close']  # Approximation
        
        # Store the last values
        self.last_values[key] = last_values
        print(f"💾 Stored last values for {symbol} {timeframe} calculations")

    def generate_indicators(self, symbol: str, timeframe: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process all indicators for a single symbol and timeframe using provided DataFrame.
        This method recalculates everything from scratch - use only when incremental calculation is not possible.
        
        Args:
            symbol (str): Symbol name
            timeframe (str): Timeframe (e.g., '5m', '10m')
            df (pd.DataFrame): DataFrame with columns: ['time', 'open', 'high', 'low', 'close', 'volume']
            
        Returns:
            pd.DataFrame: DataFrame with calculated indicators
        """
        try:
            new_cols = {}
            ema_period = self.periods[timeframe]['ema']
            vwma_period = self.periods[timeframe]['vwma']
            roc_period = self.periods[timeframe]['roc']
            fast_ema = self.periods[timeframe]['fast_ema']
            slow_ema = self.periods[timeframe]['slow_ema']
            signal_ema = self.periods[timeframe]['signal_ema']
            
            # Check if we have sufficient data for calculations
            min_required_data = max(ema_period, vwma_period, roc_period + 1, fast_ema, slow_ema, signal_ema)
            
            if len(df) < min_required_data:
                print(f"⚠️ Insufficient data for {symbol} {timeframe}: {len(df)} rows, need at least {min_required_data} rows")
                print(f"   Required periods: EMA={ema_period}, VWMA={vwma_period}, ROC={roc_period}, Fast EMA={fast_ema}, Slow EMA={slow_ema}, Signal EMA={signal_ema}")
                # Return DataFrame with NaN indicators
                result_df = df.copy()
                result_df['ema'] = np.nan
                result_df['vwma'] = np.nan
                result_df['roc'] = np.nan
                result_df['macd_line'] = np.nan
                result_df['macd_signal'] = np.nan
                return result_df
            
            # Calculate all EMAs
            if ema_period:
                new_cols['ema'] = df['close'].ewm(span=ema_period, adjust=False).mean()
            
            # Calculate all VWMAs
            if vwma_period:
                new_cols['vwma'] = (
                    (df['close'] * df['volume']).rolling(window=vwma_period).sum() /
                    df['volume'].rolling(window=vwma_period).sum()
                )
            
            # Calculate all ROCs
            if roc_period:
                new_cols['roc'] = df['close'].pct_change(periods=roc_period)
                # Debug ROC calculation in bulk processing
                debug_bulk_roc = True
                if debug_bulk_roc:
                    print(f"   [Bulk ROC Debug] Calculating ROC for {len(df)} rows with period {roc_period}")
                    print(f"   [Bulk ROC Debug] First few ROC values: {new_cols['roc'].head(10).tolist()}")
                    print(f"   [Bulk ROC Debug] Last few ROC values: {new_cols['roc'].tail(10).tolist()}")
                    print(f"   [Bulk ROC Debug] NaN count: {new_cols['roc'].isna().sum()}")
            
            # Calculate all MACDs
            if fast_ema and slow_ema and signal_ema:
                macd_line = (
                    df['close'].ewm(span=fast_ema, adjust=False).mean() -
                    df['close'].ewm(span=slow_ema, adjust=False).mean()
                )
                new_cols['macd_line'] = macd_line
                new_cols['macd_signal'] = macd_line.ewm(span=signal_ema, adjust=False).mean()
            
            # Update DataFrame with indicator columns (replace if they already exist)
            result_df = df.copy()
            for col_name, col_data in new_cols.items():
                result_df[col_name] = col_data
            
            print(f"✅ Bulk processed {symbol} {timeframe} with {len(result_df)} rows")
            
            return result_df
            
        except Exception as e:
            print(f"❌ Error processing {symbol} {timeframe}: {str(e)}")
            return df