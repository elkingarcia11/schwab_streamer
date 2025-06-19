"""
Indicator Generator Module
Handles generating indicator files for all symbols and timeframes for different combinations of ema, vwma, roc periods
"""
import os
import pandas as pd
import numpy as np

class IndicatorGenerator:
    def __init__(self):
        # Store the last calculated values for each symbol/timeframe combination
        self.last_values = {}
        
    def generate_indicators(self, symbol: str, timeframe: str, ema_period: int = None, vwma_period: int = None, roc_period: int = None, fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> None:
        """Process all indicators for a single symbol and timeframe"""
        try:
            # Load data once
            filepath = f"data/timeframe/{symbol}.csv"
            if not os.path.exists(filepath):
                print(f"⚠️ File not found: {filepath}")
                return
                
            df = pd.read_csv(filepath)
            
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
            df = pd.concat([df, new_cols_df], axis=1)
            df = df.copy()  # Defragment
            
            # Write all indicators at once
            df.to_csv(filepath, index=False)
            print(f"✅ Processed {symbol} {timeframe}")
            
            # Store last values for real-time calculations
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
                    # Store the last MACD signal value
                    last_values['macd_signal_ema'] = df[f'macd_signal_{fast_ema}_{slow_ema}_{signal_ema}'].iloc[-1]
                    
                    # For MACD EMAs, we'll use the last close price as starting point
                    # This is an approximation - in a more sophisticated system, you might store the actual EMA values
                    last_values['macd_fast_ema'] = df['close'].iloc[-1]
                    last_values['macd_slow_ema'] = df['close'].iloc[-1]
            
            # Store the last values
            self.last_values[key] = last_values
            print(f"💾 Stored last values for {symbol} {timeframe} real-time calculations")
            
        except Exception as e:
            print(f"❌ Error processing {symbol} {timeframe}: {str(e)}")

    def load_last_values_from_csv(self, symbol: str, timeframe: str, 
                                 ema_period: int = None, vwma_period: int = None, roc_period: int = None,
                                 fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> dict:
        """
        Load the last calculated values from existing CSV file to maintain continuity.
        
        Args:
            symbol (str): Symbol name
            timeframe (str): Timeframe (e.g., '5m', '10m')
            ema_period (int): EMA period
            vwma_period (int): VWMA period
            roc_period (int): ROC period
            fast_ema (int): Fast EMA for MACD
            slow_ema (int): Slow EMA for MACD
            signal_ema (int): Signal EMA for MACD
            
        Returns:
            dict: Last values loaded from CSV file
        """
        try:
            filepath = f"data/timeframe/{symbol}.csv"
            if not os.path.exists(filepath):
                return None
                
            # Load the last few rows to get the most recent values
            df = pd.read_csv(filepath)
            if len(df) == 0:
                return None
                
            # Get the last row
            last_row = df.iloc[-1]
            
            # Initialize last values
            last_values = {
                'ema': None,
                'vwma_sum': 0,
                'vwma_volume_sum': 0,
                'vwma_buffer': [],
                'last_close': None,
                'roc_buffer': [],  # Buffer to store last roc_period close prices
                'macd_fast_ema': None,
                'macd_slow_ema': None,
                'macd_signal_ema': None
            }
            
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
                    # Reconstruct MACD EMAs from the last values
                    # We need to work backwards from the MACD line and signal
                    last_macd_line = last_row[macd_line_col]
                    last_macd_signal = last_row[macd_signal_col]
                    
                    # For the first time, we'll need to calculate the EMAs
                    # This is a simplified approach - in practice, you might want to store the actual EMA values
                    if not pd.isna(last_macd_line) and not pd.isna(last_macd_signal):
                        # Use the last close price as a starting point for EMAs
                        # This is an approximation - ideally you'd store the actual EMA values
                        last_values['macd_fast_ema'] = last_row['close']  # Will be recalculated
                        last_values['macd_slow_ema'] = last_row['close']  # Will be recalculated
                        last_values['macd_signal_ema'] = last_macd_signal
                        print(f"📊 Loaded MACD signal value: {last_values['macd_signal_ema']}")
            
            return last_values
            
        except Exception as e:
            print(f"❌ Error loading last values from CSV for {symbol} {timeframe}: {str(e)}")
            return None

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
                
                # Try to load last values from existing CSV file
                loaded_values = self.load_last_values_from_csv(
                    symbol, timeframe, ema_period, vwma_period, roc_period,
                    fast_ema, slow_ema, signal_ema
                )
                
                if loaded_values:
                    self.last_values[key] = loaded_values
                    print(f"✅ Loaded existing state for {symbol} {timeframe}")
                else:
                    # Initialize with default values if no existing data
                    self.last_values[key] = {
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

    def append_real_time_data(self, symbol: str, timeframe: str, new_data: pd.DataFrame, 
                             ema_period: int = None, vwma_period: int = None, roc_period: int = None, 
                             fast_ema: int = None, slow_ema: int = None, signal_ema: int = None) -> None:
        """
        Append new real-time data with calculated indicators to the existing CSV file.
        
        Args:
            symbol (str): Symbol name
            timeframe (str): Timeframe (e.g., '5m', '10m')
            new_data (pd.DataFrame): New data points
            ema_period (int): EMA period
            vwma_period (int): VWMA period
            roc_period (int): ROC period
            fast_ema (int): Fast EMA for MACD
            slow_ema (int): Slow EMA for MACD
            signal_ema (int): Signal EMA for MACD
        """
        try:
            # Calculate indicators for new data
            data_with_indicators = self.calculate_real_time_indicators(
                symbol, timeframe, new_data, ema_period, vwma_period, roc_period, 
                fast_ema, slow_ema, signal_ema
            )
            
            # Append to existing file
            filepath = f"data/timeframe/{symbol}.csv"
            
            if os.path.exists(filepath):
                # Append mode
                data_with_indicators.to_csv(filepath, mode='a', header=False, index=False)
                print(f"📈 Appended {len(new_data)} new data points with indicators for {symbol} {timeframe}")
            else:
                # Create new file
                data_with_indicators.to_csv(filepath, index=False)
                print(f"📄 Created new file with {len(new_data)} data points for {symbol} {timeframe}")
                
        except Exception as e:
            print(f"❌ Error appending real-time data for {symbol} {timeframe}: {str(e)}")

    def reset_state(self, symbol: str, timeframe: str):
        """Reset the stored state for a symbol/timeframe combination."""
        key = f"{symbol}_{timeframe}"
        if key in self.last_values:
            del self.last_values[key]
            print(f"🔄 Reset state for {symbol} {timeframe}")

if __name__ == "__main__":
    generator = IndicatorGenerator()
    
    # Example 1: Full file processing (existing functionality)
    generator.generate_indicators("SPY", "5m", ema_period=7, vwma_period=17, roc_period=8, fast_ema=12, slow_ema=26, signal_ema=9)
    
    # Example 2: Real-time data processing
    # Create sample new data
    new_data = pd.DataFrame({
        'time': ['2025-01-20 10:00:00', '2025-01-20 10:01:00'],
        'open': [100.0, 100.5],
        'high': [100.8, 100.9],
        'low': [99.8, 100.2],
        'close': [100.5, 100.7],
        'volume': [1000, 1200]
    })
    
    # Process real-time data
    result = generator.calculate_real_time_indicators(
        "SPY", "5m", new_data, 
        ema_period=7, vwma_period=17, roc_period=8, 
        fast_ema=12, slow_ema=26, signal_ema=9
    )
    
    print("Real-time indicators calculated:")
    print(result)

