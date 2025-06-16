
import pandas as pd
from trades import Trades

class Signals:
    def __init__(self):
        self.trades_manager = Trades()

    def evaluate_entry_conditions(row):
        """
        Evaluates if a new trade entry should be triggered.
        Conditions:
        - EMA 7 > VWMA 17
        - MACD Line > MACD Signal Line
        - ROC 8 > 0
        """
        try:
            return (
                row['ema_7_t'] > row['vwma_17_t'] and
                row['macd_line_t'] > row['macd_signal_line_t'] and
                row['roc_8_t'] > 0
            )
        except KeyError as e:
            print(f"Missing expected indicator in row: {e}")
            return False
        except Exception as e:
            print(f"Error evaluating entry: {e}")
            return False


    def evaluate_exit_conditions(row):
        """
        Evaluates if a trade exit should be triggered.
        Exit if **at least 2 of the following 3 conditions** are met:
        - EMA 7 < VWMA 17
        - MACD Line < MACD Signal Line
        - ROC 8 < 0
        """
        try:
            conditions = [
                row['ema_7_t'] < row['vwma_17_t'],
                row['macd_line_t'] < row['macd_signal_line_t'],
                row['roc_8_t'] < 0
            ]
            return sum(conditions) >= 2
        except KeyError as e:
            print(f"Missing expected indicator in row: {e}")
            return False
        except Exception as e:
            print(f"Error evaluating exit: {e}")
            return False

    def generate_signal(self, row, symbol, timeframe):
        """
        Generate a signal for a given row.
        """
        open_trades = self.trades_manager.get_open_trades()

        # Check if there is an open trade for this symbol/timeframe
        current_trade = open_trades.get((symbol, timeframe))

        # If trade is open, evaluate exit conditions
        if current_trade and self.evaluate_exit_conditions(row):
            self.trades_manager.close_trade(current_trade, row)
            print(f"Trade closed for {symbol} {timeframe} at {row['timestamp']}")

        # If no trade is open, evaluate entry conditions to open new trade
        elif not current_trade and self.evaluate_entry_conditions(row):
            self.trades_manager.open_trade(symbol, timeframe, row)
            print(f"Trade opened for {symbol} {timeframe} at {row['timestamp']}")

    def generate_signals(self, df, symbol, timeframe):
        """
        Generate signals for a given dataframe.
        """
        for idx, row in df.iterrows():
            self.generate_signal(row, symbol, timeframe)