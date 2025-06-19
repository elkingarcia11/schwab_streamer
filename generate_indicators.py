"""
Indicator Generator Module
Handles generating indicator files for all symbols and timeframes for different combinations of ema, vwma, roc periods
"""
import pandas as pd
import os

class IndicatorGenerator:
    def __init__(self, ema_periods: list[int] = None, vwma_periods: list[int] = None, roc_periods: list[int] = None, fast_emas: list[int] = None, slow_emas: list[int] = None, signal_emas: list[int] = None) -> None:
        # Optimized parameter ranges
        self.ema_periods = ema_periods or [3, 5, 8, 10, 12, 14, 16, 18, 20]  # Fibonacci sequence
        self.vwma_periods = vwma_periods or [16, 18, 20, 22, 24, 26, 28, 30]  # Common moving averages
        self.roc_periods = roc_periods or [3, 5, 8, 10, 12, 14, 16, 18, 20]  # Common momentum periods
        self.fast_emas = fast_emas or [12, 14, 16, 18, 20]
        self.slow_emas = slow_emas or [26, 28, 30, 32, 34]
        self.signal_emas = signal_emas or [9, 10, 11, 12, 13]

    def process_symbol_timeframe(self, symbol: str, timeframe: str) -> None:
        """Process all indicators for a single symbol and timeframe"""
        try:
            # Load data once
            filepath = f"data/{symbol}_{timeframe}.csv"
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
            for ema_period in self.ema_periods:
                new_cols[f'ema_{ema_period}'] = df['close'].ewm(span=ema_period, adjust=False).mean()
            
            # Calculate all VWMAs
            for vwma_period in self.vwma_periods:
                new_cols[f'vwma_{vwma_period}'] = (
                    (df['close'] * df['volume']).rolling(window=vwma_period).sum() /
                    df['volume'].rolling(window=vwma_period).sum()
                )
            
            # Calculate all ROCs
            for roc_period in self.roc_periods:
                new_cols[f'roc_{roc_period}'] = df['close'].pct_change(periods=roc_period)
            
            # Calculate all MACDs
            for fast_ema in self.fast_emas:
                for slow_ema in self.slow_emas:
                    if fast_ema < slow_ema:
                        for signal_ema in self.signal_emas:  
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
            
        except Exception as e:
            print(f"❌ Error processing {symbol} {timeframe}: {str(e)}")

    def generate_indicators(self, symbol: str, timeframe: str) -> None:
        """Generate all indicators for all symbols and timeframes in parallel"""
        from itertools import product
        total_combinations = len(self.ema_periods) * len(self.vwma_periods) * len(self.roc_periods) * len(self.fast_emas) * len(self.slow_emas) * len(self.signal_emas)
        print(f"\nGenerating indicators for {symbol} {timeframe}")
        print(f"Total combinations to process: {total_combinations:,}")
        
        # Process the symbol and timeframe
        self.process_symbol_timeframe(symbol, timeframe)
        print(f"✅ Completed generating indicators for {symbol} {timeframe}")

if __name__ == "__main__":
    generator = IndicatorGenerator()
    generator.generate_indicators("SPY", "5m")

