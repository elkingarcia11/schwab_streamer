# Schwab Streamer

A Python-based system for streaming, aggregating, and analyzing real-time and historical equity data from the Schwab API, with automated email notifications for trading signals and position changes.

---

## Features

- **Real-time Streaming:** Connects to Schwab's streaming API to receive live OHLCV (Open, High, Low, Close, Volume) data for selected symbols.
- **Historical Data Fetching:** Fetches and stores historical minute-level data for all tracked symbols at startup.
- **Multi-Timeframe Aggregation:** Aggregates 1-minute data into higher timeframes (3m, 5m, 10m, 15m, 30m).
- **Technical Indicators:** Calculates indicators (ROC, EMA, VWMA, MACD) for each symbol and timeframe.
- **Signal Processing:** Detects trading signals based on indicator conditions and manages trade state.
- **Inverse Data:** Generates and analyzes inverse OHLC data for short strategies.
- **Automated Email Alerts:** Sends notifications for position changes, open trades, and system status.
- **Robust Authentication:** Handles Schwab API authentication and token refresh automatically.

---

## Project Structure

```
.
├── asset_manager.py
├── schwab_auth.py
├── schwab_streamer_client.py
├── email_manager/
│   └── email_manager.py
├── requirements.txt
├── symbols_to_stream.txt
├── email_credentials.env
├── schwab_credentials.env
└── data/
    └── [timeframe]/
        └── [symbol].csv
```

---

## Setup

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd schwab_streamer
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

#### Email Credentials (`email_credentials.env`)

```
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
EMAIL_ALERTS_ENABLED=true
SENDER_EMAIL=your_email@gmail.com
SENDER_PASSWORD=your_app_password
TO_EMAILS=recipient1@gmail.com,recipient2@gmail.com
SENDER_NAME=Schwab Trading Bot
SUBJECT_PREFIX=[SCHWAB]
```

#### Schwab API Credentials (`schwab_credentials.env`)

```
SCHWAB_APP_KEY=your_schwab_app_key
SCHWAB_APP_SECRET=your_schwab_app_secret
```

### 4. List Symbols to Stream

Edit `symbols_to_stream.txt` to specify which symbols to track, separated by commas:

```
SPY,QQQ,TSLA,NVDA,AMZN,AAPL,META,MSFT
```

---

## Usage

### 1. Fetch Historical Data and Start Streaming

```bash
python3 schwab_streamer_client.py
```

- On startup, the system fetches historical 1-minute data for all symbols and aggregates it into higher timeframes.
- Only completed candles (not the current minute) are saved and processed.
- The streamer then connects to Schwab's WebSocket API and begins processing live data.

### 2. Email Notifications

- Email alerts are sent for position changes (open/close), new trades, and system status.
- Email configuration is loaded from `email_credentials.env`.

---

## Main Components

### `schwab_streamer_client.py`

- Entry point for the system.
- Handles authentication, loads symbols, initializes the `AssetManager`, and manages the streaming connection.

### `asset_manager.py`

- Fetches and stores historical data.
- Aggregates data into higher timeframes.
- Calculates technical indicators.
- Processes trading signals and manages trade state.
- Handles data export and ensures only completed candles are saved.

### `email_manager/email_manager.py`

- Loads email configuration.
- Sends notifications for position changes, open trades, and system status.
- Supports test emails and summary emails.

### `schwab_auth.py`

- Manages Schwab API authentication.
- Handles token refresh and credential loading.

---

## Data Storage

- Data is stored in the `data/` directory, organized by timeframe and symbol.
- Each CSV contains OHLCV data, technical indicators, and is updated only with completed candles.

---

## Security

- **Never commit your `.env` files or credentials to public repositories.**
- Use app-specific passwords for email accounts.
- Rotate your Schwab API keys and email passwords regularly.

---

## Requirements

- Python 3.8+
- See `requirements.txt` for all dependencies.

---

## License

MIT License

---

## Disclaimer

This project is for educational and research purposes only. Use at your own risk. Not intended for live trading without further review and compliance with Schwab's API terms and financial regulations.

## Buy Conditions

### Regular Trades (LONG)

A regular trade is opened when ALL of the following conditions are met:

1. ROC8 > 0.05 (Rate of Change is positive and significant)
2. EMA7 > VWMA17 (Short-term trend is above long-term trend)
3. MACD Line > MACD Signal (Momentum is increasing)

### Inverse Trades (SHORT)

An inverse trade is opened when ALL of the following conditions are met:

1. ROC8 < -0.05 (Rate of Change is negative and significant)
2. EMA7 < VWMA17 (Short-term trend is below long-term trend)
3. MACD Line < MACD Signal (Momentum is decreasing)

### Position Management

- Each position is managed independently
- Positions are closed when the opposite conditions are met
- Maximum of one position per symbol at a time
- Trades are executed at the close price of the candle that triggers the signal

### Technical Indicators

- ROC8: 8-period Rate of Change
- EMA7: 7-period Exponential Moving Average
- VWMA17: 17-period Volume Weighted Moving Average
- MACD: 12,26,9 Moving Average Convergence Divergence
