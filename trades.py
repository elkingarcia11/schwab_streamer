import os
import pandas as pd


class Trade:
    def __init__(self, symbol, timeframe, entry_time, entry_price):
        self.symbol = symbol
        self.timeframe = timeframe
        self.entry_time = entry_time
        self.entry_price = entry_price
        self.exit_time = None
        self.exit_price = None
        self.pnl = None
        self.status = 'open'

    def close(self, exit_time, exit_price):
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.pnl = (exit_price - self.entry_price) / self.entry_price * 100
        self.status = 'closed'

    def get_status(self):
        return self.status

    def get_pnl(self):
        return self.pnl

    def get_exit_time(self):
        return self.exit_time

    def get_exit_price(self):
        return self.exit_price

    def get_symbol(self):
        return self.symbol

    def get_timeframe(self):
        return self.timeframe

    def get_entry_time(self):
        return self.entry_time

    def get_entry_price(self):
        return self.entry_price

    def get_trade_id(self):
        return self.trade_id

    def get_trade_log(self):
        return self.trade_log

    def get_trade_log_csv(self):
        return self.trade_log_csv


class Trades:
    def __init__(self, open_trades_path='open_trades.csv', closed_trades_path='closed_trades.csv'):
        self.open_trades = {}
        self.closed_trades = []
        self.open_trades_path = open_trades_path
        self.closed_trades_path = closed_trades_path

        # Load existing data if files exist
        self.load_trades_from_csv()

    def load_trades_from_csv(self):
        if os.path.exists(self.open_trades_path):
            df_open = pd.read_csv(self.open_trades_path)
            for _, row in df_open.iterrows():
                trade = Trade(row['symbol'], row['timeframe'], row['entry_time'], row['entry_price'])
                # You can add more attributes if stored, e.g., status
                self.open_trades[(trade.get_symbol(), trade.get_timeframe())] = trade
        
        if os.path.exists(self.closed_trades_path):
            df_closed = pd.read_csv(self.closed_trades_path)
            for _, row in df_closed.iterrows():
                trade = Trade(row['symbol'], row['timeframe'], row['entry_time'], row['entry_price'])
                trade.close(row['exit_time'], row['exit_price'])
                self.closed_trades.append(trade)

    def save_open_trades_to_csv(self):
        df = pd.DataFrame([{
            'symbol': t.symbol,
            'timeframe': t.timeframe,
            'entry_time': t.entry_time,
            'entry_price': t.entry_price,
            'status': t.status
        } for t in self.open_trades.values()])
        df.to_csv(self.open_trades_path, index=False)

    def save_closed_trades_to_csv(self):
        df = pd.DataFrame([{
            'symbol': t.symbol,
            'timeframe': t.timeframe,
            'entry_time': t.entry_time,
            'entry_price': t.entry_price,
            'exit_time': t.exit_time,
            'exit_price': t.exit_price,
            'pnl': t.pnl,
            'status': t.status
        } for t in self.closed_trades])
        df.to_csv(self.closed_trades_path, index=False)

    def open_trade(self, symbol, timeframe, row):
        trade = Trade(symbol, timeframe, row['timestamp'], row['close'])
        self.open_trades[(trade.get_symbol(), trade.get_timeframe())] = trade
        self.save_open_trades_to_csv()
        return trade
    
    def close_trade(self, trade, row):
        trade.close(row['timestamp'], row['close'])
        self.closed_trades.append(trade)
        self.open_trades.pop((trade.get_symbol(), trade.get_timeframe()))
        self.save_closed_trades_to_csv()
        self.save_open_trades_to_csv()
        return trade
