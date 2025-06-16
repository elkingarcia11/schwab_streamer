import pandas as pd
import pytz

class Indicators:
    def __init__(self, symbols, timeframes, ema_period, vwma_period, roc_period,
                 macd_fast_period, macd_slow_period, macd_signal_period):
        """
        Initialize the Indicators class.
        """
        self.indicators = {}
        for symbol in symbols:
            for timeframe in timeframes:
                key = (symbol, timeframe)
                self.indicators[key] = Indicator(
                    symbol, timeframe,
                    ema_period, vwma_period, roc_period,
                    macd_fast_period, macd_slow_period, macd_signal_period
                )

    def get_indicator(self, symbol, timeframe):
        """
        Get the indicator for a given symbol and timeframe.
        """
        key = (symbol, timeframe)
        if key not in self.indicators:
            raise KeyError(f"Indicator for ({symbol}, {timeframe}) not found.")
        return self.indicators[key]

class Indicator:
    def __init__(self, symbol, timeframe, ema_period, vwma_period, roc_period,
                 macd_fast_period, macd_slow_period, macd_signal_period):
        """
        Initialize the Indicator class.
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.ema_period = ema_period
        self.vwma_period = vwma_period
        self.roc_period = roc_period
        self.macd_fast_period = macd_fast_period
        self.macd_slow_period = macd_slow_period
        self.macd_signal_period = macd_signal_period

        self.filepath = f"data/{timeframe}/{symbol}.csv"
        self.df = self.load_or_calculate_indicators()

    def load_or_calculate_indicators(self):
        """
        Load or calculate the indicators for the given symbol and timeframe.
        """
        filepath = f'data/{self.timeframe}/{self.symbol}.csv'
        df = pd.read_csv(filepath)

        # Parse timestamps
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        elif 'datetime' in df.columns:
            df['timestamp'] = pd.to_datetime(df['datetime'])
        df.sort_values('timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.df = df

        # Check for historical indicators
        historical_cols = [
            f'ema_{self.ema_period}_h',
            f'vwma_{self.vwma_period}_h',
            f'roc_{self.roc_period}_h',
            'macd_line_h',
            'macd_signal_line_h'
        ]
        historical_missing = any(col not in df.columns for col in historical_cols)

        # Check for intraday indicators
        intraday_cols = [
            f'ema_{self.ema_period}_t',
            f'vwma_{self.vwma_period}_t',
            f'roc_{self.roc_period}_t',
            'macd_line_t',
            'macd_signal_line_t'
        ]
        intraday_missing = any(col not in df.columns for col in intraday_cols)

        if historical_missing:
            print(f"[{self.symbol} {self.timeframe}] Calculating missing historical indicators...")
            self.calculate_historical_indicators()

        # Always calculate intraday for current day
        print(f"[{self.symbol} {self.timeframe}] Calculating intraday indicators for today...")
        if intraday_missing:
            print(f"[{self.symbol} {self.timeframe}] Calculating missing intraday indicators...")
            self.calculate_intraday_indicators()

        # Save updated dataframe to CSV
        self.df.to_csv(filepath, index=False)


    def calculate_ema(self, df):
        """
        Calculate the EMA for the given dataframe.
        """
        return df['close'].ewm(span=self.ema_period, adjust=False).mean()

    def calculate_vwma(self, df):
        """
        Calculate the VWMA for the given dataframe.
        """
        price_volume = df['close'] * df['volume']
        return price_volume.rolling(window=self.vwma_period).sum() / df['volume'].rolling(window=self.vwma_period).sum()

    def calculate_macd(self, df):
        """
        Calculate the MACD for the given dataframe.
        """
        ema_fast = df['close'].ewm(span=self.macd_fast_period, adjust=False).mean()
        ema_slow = df['close'].ewm(span=self.macd_slow_period, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=self.macd_signal_period, adjust=False).mean()
        return macd_line, signal_line


    def calculate_roc(self, df):
        """
        Calculate the ROC for the given dataframe.
        """
        return df['close'].pct_change(self.roc_period) * 100

    def calculate_historical_indicators(self):
        """
        Calculate the historical indicators for the given dataframe.
        """
        df = self.df.sort_values('timestamp').reset_index(drop=True)
        n = len(df)

        # Helper to find last valid index or -1 if not present
        def last_valid_index(col):
            if col not in df.columns:
                return -1
            non_nan = df[col].dropna().index
            return non_nan[-1] if not non_nan.empty else -1

        # Get last valid indices for each indicator
        last_ema = last_valid_index(f'ema_{self.ema_period}_h')
        last_vwma = last_valid_index(f'vwma_{self.vwma_period}_h')
        last_roc = last_valid_index(f'roc_{self.roc_period}_h')
        last_macd_line = last_valid_index('macd_line_h')
        last_macd_signal = last_valid_index('macd_signal_line_h')

        # Find earliest index among the last valid indices
        last_valid = min(last_ema, last_vwma, last_roc, last_macd_line, last_macd_signal)

        # If all indicators up to date (last_valid at end), no need to calculate
        if last_valid >= n - 1:
            return df

        # Start a bit earlier for rolling windows
        start_idx = max(0, last_valid - max(self.ema_period, self.vwma_period, self.roc_period) + 1)

        df_historical = df.iloc[start_idx:].copy()

        # Calculate EMA if needed
        if last_ema < n - 1:
            df.loc[df_historical.index, f'ema_{self.ema_period}_h'] = df_historical['close'].ewm(span=self.ema_period, adjust=False).mean()

        # Calculate VWMA if needed
        if last_vwma < n - 1:
            vwma = (df_historical['close'] * df_historical['volume']).rolling(self.vwma_period).sum() / df_historical['volume'].rolling(self.vwma_period).sum()
            df.loc[df_historical.index, f'vwma_{self.vwma_period}_h'] = vwma

        # Calculate ROC if needed
        if last_roc < n - 1:
            df.loc[df_historical.index, f'roc_{self.roc_period}_h'] = df_historical['close'].pct_change(self.roc_period) * 100

        # Calculate MACD if needed
        if last_macd_line < n - 1 or last_macd_signal < n - 1:
            macd_line, macd_signal = self.calculate_macd(df_historical)
            df.loc[df_historical.index, 'macd_line_h'] = macd_line
            df.loc[df_historical.index, 'macd_signal_line_h'] = macd_signal

        self.df = df
        return df

    def calculate_intraday_indicators(self):
        """
        Calculate the intraday indicators for the given dataframe.
        """
        df = self.df.sort_values('timestamp').reset_index(drop=True)

        # Define intraday indicator column names
        ema_col = f'ema_{self.ema_period}_t'
        vwma_col = f'vwma_{self.vwma_period}_t'
        roc_col = f'roc_{self.roc_period}_t'
        macd_line_col = 'macd_line_t'
        macd_signal_col = 'macd_signal_line_t'

        # Helper to find last valid index or -1 if not present
        def last_valid_index(col):
            if col not in df.columns:
                return -1
            non_nan = df[col].dropna().index
            return non_nan[-1] if not non_nan.empty else -1


        eastern = pytz.timezone('US/Eastern')
        now = pd.Timestamp.now(tz=eastern)
        today_open = now.normalize() + pd.Timedelta(hours=9, minutes=30)

        # Filter rows for today only
        df_today = df[df['timestamp'] >= today_open].copy()
        if df_today.empty:
            # No intraday data for today yet
            return df

        today_indices = df_today.index

        # Find last valid indices for each intraday indicator only in today's data
        last_ema = last_valid_index(ema_col)
        last_vwma = last_valid_index(vwma_col)
        last_roc = last_valid_index(roc_col)
        last_macd_line = last_valid_index(macd_line_col)
        last_macd_signal = last_valid_index(macd_signal_col)

        # Find earliest last valid index among intraday indicators for today
        last_valid = min(last_ema, last_vwma, last_roc, last_macd_line, last_macd_signal)

        # If all indicators are up to date for today's data, skip calculation
        if last_valid >= today_indices[-1]:
            return df

        # Start a bit earlier for rolling window calculations
        start_idx = max(today_indices[0], last_valid - max(self.ema_period, self.vwma_period, self.roc_period) + 1)

        df_calc = df.loc[start_idx:].copy()

        # Calculate EMA if needed
        if last_ema < today_indices[-1]:
            ema = df_calc['close'].ewm(span=self.ema_period, adjust=False).mean()
            df.loc[df_calc.index, ema_col] = ema

        # Calculate VWMA if needed
        if last_vwma < today_indices[-1]:
            vwma = (df_calc['close'] * df_calc['volume']).rolling(self.vwma_period).sum() / df_calc['volume'].rolling(self.vwma_period).sum()
            df.loc[df_calc.index, vwma_col] = vwma

        # Calculate ROC if needed
        if last_roc < today_indices[-1]:
            roc = df_calc['close'].pct_change(self.roc_period) * 100
            df.loc[df_calc.index, roc_col] = roc

        # Calculate MACD if needed
        if last_macd_line < today_indices[-1] or last_macd_signal < today_indices[-1]:
            macd_line, macd_signal = self.calculate_macd(df_calc)
            df.loc[df_calc.index, macd_line_col] = macd_line
            df.loc[df_calc.index, macd_signal_col] = macd_signal

        self.df = df
        return df

    def update_with_new_data(self, new_data, save_to_csv=True):
        """
        Update the indicators with new data.
        """
        self.df = pd.concat([self.df, new_data]).drop_duplicates().reset_index(drop=True)
        self.calculate_historical_indicators()
        self.calculate_intraday_indicators()
        if save_to_csv:
            self.save_to_csv()

    def save_to_csv(self):  
        """
        Save the indicators to a csv file.
        """
        filepath = f'data/{self.timeframe}/{self.symbol}.csv'
        self.df.to_csv(filepath, index=False)
