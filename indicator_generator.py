"""
Indicator Generator Module
The Indicator Generator module handles three primary use cases: initial historical data processing (using efficient bulk vectorized operations), additional data processing (using incremental calculations that build on existing state), and real-time data processing (using single-point incremental updates). It calculates common technical indicators including Exponential Moving Averages (EMA), Volume Weighted Moving Averages (VWMA), Rate of Change (ROC), and MACD (Moving Average Convergence Divergence) with customizable periods. The system features automatic state persistence and seamless recovery of calculation state after program restarts, and smart auto-detection that chooses bulk processing for initial data and incremental processing for additional data. All calculations are performed in-memory for optimal performance while maintaining data continuity across sessions, making it suitable for both historical backtesting and real-time trading applications.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class IndicatorGenerator:
    """
    Smart indicator generator that automatically optimizes calculation methods with state persistence support.
    
    Primary Usage:
    - smart_indicator_calculation(): Use this for all indicator calculations (recommended)
    - generate_all_indicators(): Use this for bulk processing with provided DataFrame
    - calculate_real_time_indicators(): Use this for incremental processing
    - reset_state(): Use this to clear stored state
    
    Features:
    - Pure in-memory processing for performance
    - Automatic state persistence and recovery
    - Smart auto-detection of initial vs incremental processing
    - Continuity across all calculation types
    - No CSV file operations (handled by MarketDataFetcher)
    """
    def __init__(self):
        # Store the last calculated values for each symbol/timeframe combination
        self.last_values = {}
        
    def generate_all_indicators(self, symbol: str, timeframe: str, df: pd.DataFrame,
                               ema_period: int = None, vwma_period: int = None, roc_period: int = None, 
                               fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> pd.DataFrame:
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

    def calculate_real_time_indicators(self, symbol: str, timeframe: str, new_data: pd.DataFrame, 
                                     ema_period: int = None, vwma_period: int = None, roc_period: int = None, 
                                     fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> pd.DataFrame:
        """
        Calculate indicators for new real-time data points efficiently.
        
        Args:
            symbol (str): Symbol name
            timeframe (str): Timeframe (e.g., '5m', '10m')
            new_data (pd.DataFrame): New data points with columns: ['time', 'open', 'high', 'low', 'close', 'volume']
            ema_period (int): EMA period
            vwma_period (int): VWMA period
            roc_period (int): ROC period
            fast_ema (int): Fast EMA for MACD
            slow_ema (int): Slow EMA for MACD
            signal_ema (int): Signal EMA for MACD
            
        Returns:
            pd.DataFrame: New data with calculated indicators
        """
        try:
            key = f"{symbol}_{timeframe}"
            
            # Initialize last values if not exists
            if key not in self.last_values:
                print(f"🔄 Initializing state for {symbol} {timeframe}...")
                self.last_values[key] = self._initialize_state()
                print(f"🆕 Initialized new state for {symbol} {timeframe}")
            
            last_vals = self.last_values[key]
            result_df = new_data.copy()
            
            # Calculate EMA for new data points
            if ema_period:
                if last_vals['ema'] is None:
                    # First time, calculate EMA for all data
                    result_df[f'ema_{ema_period}'] = result_df['close'].ewm(span=ema_period, adjust=False).mean()
                    last_vals['ema'] = result_df[f'ema_{ema_period}'].iloc[-1]
                else:
                    # Update EMA for new data points
                    ema_values = []
                    current_ema = last_vals['ema']
                    alpha = 2.0 / (ema_period + 1)
                    
                    for close_price in result_df['close']:
                        current_ema = alpha * close_price + (1 - alpha) * current_ema
                        ema_values.append(current_ema)
                    
                    result_df[f'ema_{ema_period}'] = ema_values
                    last_vals['ema'] = current_ema
            
            # Calculate VWMA for new data points
            if vwma_period:
                vwma_values = []
                vwma_buffer = last_vals['vwma_buffer']
                
                for _, row in result_df.iterrows():
                    # Add new data point to buffer
                    vwma_buffer.append((row['close'], row['volume']))
                    
                    # Keep only the last vwma_period points
                    if len(vwma_buffer) > vwma_period:
                        vwma_buffer.pop(0)
                    
                    # Calculate VWMA
                    if len(vwma_buffer) == vwma_period:
                        price_volume_sum = sum(price * volume for price, volume in vwma_buffer)
                        volume_sum = sum(volume for _, volume in vwma_buffer)
                        vwma = price_volume_sum / volume_sum if volume_sum > 0 else row['close']
                    else:
                        vwma = row['close']  # Not enough data yet
                    
                    vwma_values.append(vwma)
                
                result_df[f'vwma_{vwma_period}'] = vwma_values
                last_vals['vwma_buffer'] = vwma_buffer
            
            # Calculate ROC for new data points
            if roc_period:
                roc_values = []
                roc_buffer = last_vals['roc_buffer']
                
                for close_price in result_df['close']:
                    if len(roc_buffer) > roc_period:
                        roc_buffer.pop(0)
                    roc_buffer.append(close_price)
                    if len(roc_buffer) == roc_period:
                        roc = (close_price - roc_buffer[0]) / roc_buffer[0]
                    else:
                        roc = np.nan
                    
                    roc_values.append(roc)
                
                result_df[f'roc_{roc_period}'] = roc_values
                last_vals['roc_buffer'] = roc_buffer
            
            # Calculate MACD for new data points
            if fast_ema and slow_ema and signal_ema:
                macd_line_values = []
                macd_signal_values = []
                
                # Update fast and slow EMAs
                if last_vals['macd_fast_ema'] is None:
                    # First time, calculate EMAs for all data
                    fast_ema_series = result_df['close'].ewm(span=fast_ema, adjust=False).mean()
                    slow_ema_series = result_df['close'].ewm(span=slow_ema, adjust=False).mean()
                    last_vals['macd_fast_ema'] = fast_ema_series.iloc[-1]
                    last_vals['macd_slow_ema'] = slow_ema_series.iloc[-1]
                    
                    # Calculate MACD line
                    macd_line_series = fast_ema_series - slow_ema_series
                    result_df[f'macd_line_{fast_ema}_{slow_ema}'] = macd_line_series
                    
                    # Calculate MACD signal
                    signal_series = macd_line_series.ewm(span=signal_ema, adjust=False).mean()
                    result_df[f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}'] = signal_series
                    last_vals['macd_signal_ema'] = signal_series.iloc[-1]
                else:
                    # Update EMAs for new data points
                    alpha_fast = 2.0 / (fast_ema + 1)
                    alpha_slow = 2.0 / (slow_ema + 1)
                    alpha_signal = 2.0 / (signal_ema + 1)
                    
                    for close_price in result_df['close']:
                        # Update fast EMA
                        last_vals['macd_fast_ema'] = alpha_fast * close_price + (1 - alpha_fast) * last_vals['macd_fast_ema']
                        
                        # Update slow EMA
                        last_vals['macd_slow_ema'] = alpha_slow * close_price + (1 - alpha_slow) * last_vals['macd_slow_ema']
                        
                        # Calculate MACD line
                        macd_line = last_vals['macd_fast_ema'] - last_vals['macd_slow_ema']
                        macd_line_values.append(macd_line)
                        
                        # Update MACD signal
                        last_vals['macd_signal_ema'] = alpha_signal * macd_line + (1 - alpha_signal) * last_vals['macd_signal_ema']
                        macd_signal_values.append(last_vals['macd_signal_ema'])
                    
                    result_df[f'macd_line_{fast_ema}_{slow_ema}'] = macd_line_values
                    result_df[f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}'] = macd_signal_values
            
            return result_df
            
        except Exception as e:
            print(f"❌ Error calculating real-time indicators for {symbol} {timeframe}: {str(e)}")
            return new_data

    def reset_state(self, symbol: str, timeframe: str):
        """Reset the stored state for a symbol/timeframe combination."""
        key = f"{symbol}_{timeframe}"
        if key in self.last_values:
            del self.last_values[key]
            print(f"🔄 Reset state for {symbol} {timeframe}")

    def load_state_from_dataframe(self, symbol: str, timeframe: str, df: pd.DataFrame,
                                 ema_period: int = None, vwma_period: int = None, roc_period: int = None,
                                 fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> bool:
        """
        Load state from an existing DataFrame to maintain continuity.
        
        Args:
            symbol (str): Symbol name
            timeframe (str): Timeframe (e.g., '5m', '10m')
            df (pd.DataFrame): DataFrame with existing data and indicators
            ema_period (int): EMA period
            vwma_period (int): VWMA period
            roc_period (int): ROC period
            fast_ema (int): Fast EMA for MACD
            slow_ema (int): Slow EMA for MACD
            signal_ema (int): Signal EMA for MACD
            
        Returns:
            bool: True if state was loaded successfully, False otherwise
        """
        try:
            if len(df) == 0:
                return False
                
            key = f"{symbol}_{timeframe}"
            
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
            if ema_period and f'ema_{ema_period}' in df.columns:
                last_values['ema'] = last_row[f'ema_{ema_period}']
                print(f"📊 Loaded EMA_{ema_period} value: {last_values['ema']}")
            
            # Load VWMA buffer - need to reconstruct from last few rows
            if vwma_period and f'vwma_{vwma_period}' in df.columns:
                # Get last vwma_period rows to reconstruct buffer
                last_n_rows = df.tail(vwma_period)
                vwma_buffer = []
                for _, row in last_n_rows.iterrows():
                    vwma_buffer.append((row['close'], row['volume']))
                last_values['vwma_buffer'] = vwma_buffer
                print(f"📊 Loaded VWMA_{vwma_period} buffer with {len(vwma_buffer)} points")
            
            # Load ROC buffer - need to reconstruct from last roc_period rows
            if roc_period:
                # Get last roc_period rows to reconstruct buffer
                last_n_rows = df.tail(roc_period)
                roc_buffer = []
                for _, row in last_n_rows.iterrows():
                    roc_buffer.append(row['close'])
                last_values['roc_buffer'] = roc_buffer
                print(f"📊 Loaded ROC_{roc_period} buffer with {len(roc_buffer)} close prices")
            
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
            
            # Store the last values
            self.last_values[key] = last_values
            print(f"✅ Loaded existing state for {symbol} {timeframe}")
            return True
            
        except Exception as e:
            print(f"❌ Error loading state from DataFrame for {symbol} {timeframe}: {str(e)}")
            return False

    def _store_last_values_from_dataframe(self, symbol: str, timeframe: str, df: pd.DataFrame,
                                         ema_period: int = None, vwma_period: int = None, roc_period: int = None,
                                         fast_ema: int = None, slow_ema: int = None, signal_ema: int = None):
        """Helper function to store last values from a DataFrame."""
        key = f"{symbol}_{timeframe}"
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
                                   additional_df: pd.DataFrame = None, ema_period: int = None, 
                                   vwma_period: int = None, roc_period: int = None,
                                   fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> pd.DataFrame:
        """
        Smart indicator calculation with state persistence support.
        
        Two main use cases:
        1. Initial bulk calculation: Pass only original_df
        2. Incremental calculation: Pass both original_df (with existing indicators) and additional_df (new data)
        
        Args:
            symbol (str): Symbol name
            timeframe (str): Timeframe (e.g., '5m', '10m')
            original_df (pd.DataFrame): Original data with existing indicators (for incremental processing)
            additional_df (pd.DataFrame): New data to process and add indicators to
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
            key = f"{symbol}_{timeframe}"
            
            # CASE 1: No additional_df passed - Initial bulk calculation
            if not additional_df:
                print(f"📊 Initial bulk calculation for {symbol} {timeframe}")
                print(f"📊 Processing {len(original_df)} original data points")
                
                # Initialize state if not exists
                if key not in self.last_values:
                    print(f"🔄 Initializing new state for {symbol} {timeframe}")
                    self.last_values[key] = self._initialize_state()
                
                # Perform bulk calculation
                result = self.generate_all_indicators(symbol, timeframe, original_df, 
                                              ema_period, vwma_period, roc_period, 
                                              fast_ema, slow_ema, signal_ema)
                
                # Store final state from bulk calculation
                self._store_last_values_from_dataframe(symbol, timeframe, result, 
                                                          ema_period, vwma_period, roc_period,
                                                          fast_ema, slow_ema, signal_ema)
                
                return result
            
            # CASE 2: Additional_df passed - Incremental calculation
            print(f"⚡ Incremental calculation for {symbol} {timeframe}")
            print(f"📊 Processing {len(additional_df)} additional data points")
            
            # Initialize state if not exists
            if key not in self.last_values:
                print(f"🔄 Initializing state for {symbol} {timeframe}...")
                
                # Try to load state from original data if provided
                if len(original_df) > 0:
                    loaded = self.load_state_from_dataframe(
                        symbol, timeframe, original_df, ema_period, vwma_period, roc_period,
                        fast_ema, slow_ema, signal_ema
                    )
                    if not loaded:
                        # Initialize with default values if loading failed
                        self.last_values[key] = self._initialize_state()
                        print(f"🆕 Initialized new state for {symbol} {timeframe}")
                else:
                    # Initialize with default values if no original data
                    self.last_values[key] = self._initialize_state()
                    print(f"🆕 Initialized new state for {symbol} {timeframe}")
            
            # Calculate indicators for additional data only
            additional_with_indicators = self.calculate_real_time_indicators(
                symbol, timeframe, additional_df,
                ema_period, vwma_period, roc_period,
                fast_ema, slow_ema, signal_ema
            )
            
            # Combine original_df and additional_df with indicators
            final_df = pd.concat([original_df, additional_with_indicators], ignore_index=True)
            print(f"📈 Combined {len(original_df)} original + {len(additional_with_indicators)} additional = {len(final_df)} total")
            
            return final_df
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            # Return original_df as fallback
            return original_df

    def _initialize_state(self) -> dict:
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

if __name__ == "__main__":
    import pandas as pd
    from datetime import datetime, timedelta

    # Create sample original data (historical)
    base_time = datetime(2025, 1, 20, 9, 30, 0)
    original_data = pd.DataFrame({
        'time': [base_time + timedelta(minutes=i) for i in range(100)],
        'open': [100.0 + i * 0.01 for i in range(100)],
        'high': [100.8 + i * 0.01 for i in range(100)],
        'low': [99.8 + i * 0.01 for i in range(100)],
        'close': [100.5 + i * 0.01 for i in range(100)],
        'volume': [1000 + i * 10 for i in range(100)]
    })

    # Create sample additional data (new data to append)
    additional_data = pd.DataFrame({
        'time': [base_time + timedelta(minutes=100 + i) for i in range(10)],
        'open': [101.0 + i * 0.01 for i in range(10)],
        'high': [101.8 + i * 0.01 for i in range(10)],
        'low': [100.8 + i * 0.01 for i in range(10)],
        'close': [101.5 + i * 0.01 for i in range(10)],
        'volume': [2000 + i * 10 for i in range(10)]
    })

    # Initialize indicator generator
    indicator_gen = IndicatorGenerator()

    # Define indicator parameters
    ema_period = 7
    vwma_period = 17
    roc_period = 8
    fast_ema = 12
    slow_ema = 26
    signal_ema = 9

    print("\n=== CASE 1: Initial bulk calculation (no additional_df) ===")
    print("Processing original data with bulk calculation...")
    result1 = indicator_gen.smart_indicator_calculation(
        "SPY", "5m",
        original_df=original_data,  # Process this data
        additional_df=None,         # No additional data
        ema_period=ema_period, vwma_period=vwma_period, roc_period=roc_period,
        fast_ema=fast_ema, slow_ema=slow_ema, signal_ema=signal_ema
    )
    print(f"✅ Result: {len(result1)} rows with indicators")
    print("Last row indicators:")
    print(f"  EMA_{ema_period}: {result1[f'ema_{ema_period}'].iloc[-1]:.4f}")
    print(f"  VWMA_{vwma_period}: {result1[f'vwma_{vwma_period}'].iloc[-1]:.4f}")
    print(f"  ROC_{roc_period}: {result1[f'roc_{roc_period}'].iloc[-1]:.4f}")

    print("\n=== CASE 2: Incremental calculation (with additional_df) ===")
    print("Processing additional data incrementally, assuming original_df already has indicators...")
    result2 = indicator_gen.smart_indicator_calculation(
        "SPY", "5m",
        original_df=result1,        # Original data with existing indicators
        additional_df=additional_data,  # New data to process
        ema_period=ema_period, vwma_period=vwma_period, roc_period=roc_period,
        fast_ema=fast_ema, slow_ema=slow_ema, signal_ema=signal_ema
    )
    print(f"✅ Result: {len(result2)} rows (original + additional with indicators)")
    print("Last row indicators (from additional data):")
    print(f"  EMA_{ema_period}: {result2[f'ema_{ema_period}'].iloc[-1]:.4f}")
    print(f"  VWMA_{vwma_period}: {result2[f'vwma_{vwma_period}'].iloc[-1]:.4f}")
    print(f"  ROC_{roc_period}: {result2[f'roc_{roc_period}'].iloc[-1]:.4f}")

    print("\n=== Summary ===")
    print(f"Original data: {len(original_data)} rows")
    print(f"Additional data: {len(additional_data)} rows")
    print(f"Final result: {len(result2)} rows")
    print("✅ Incremental calculation maintains state continuity!")
