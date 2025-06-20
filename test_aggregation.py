#!/usr/bin/env python3
"""
Test script for candle aggregation functionality
Tests the aggregation of 1m candles into 5m, 10m, 15m, and 30m timeframes
"""

import time
import pandas as pd
from schwab_streamer_client import SchwabStreamerClient

def test_candle_aggregation():
    """Test candle aggregation from 1m to higher timeframes"""
    print("🧪 Testing candle aggregation functionality...")
    
    # Create client
    client = SchwabStreamerClient(debug=True)
    print("✅ SchwabStreamerClient initialized")
    
    symbol = 'TEST'
    base_timestamp = 1735828200000  # 2025-01-02 09:30:00 EST (market open)
    base_price = 100.0
    
    print(f"\n📊 Simulating 10 minutes of 1m candles for {symbol}...")
    
    # Simulate 10 consecutive 1-minute candles
    for i in range(10):
        # Create realistic OHLCV data with some price movement
        candle = {
            'timestamp': base_timestamp + (i * 60 * 1000),  # 1 minute apart
            'open': base_price + i * 0.1 + (i % 3) * 0.05,
            'high': base_price + i * 0.1 + 0.3 + (i % 2) * 0.1,
            'low': base_price + i * 0.1 - 0.2 - (i % 4) * 0.05,
            'close': base_price + i * 0.1 + 0.1 + (i % 5) * 0.03,
            'volume': 10000 + i * 1000 + (i % 3) * 500
        }
        
        minute = i + 1
        timestamp_dt = pd.Timestamp(candle['timestamp'], unit='ms', tz='US/Eastern')
        print(f"Minute {minute:2d}: {timestamp_dt.strftime('%H:%M')} - O={candle['open']:.2f} H={candle['high']:.2f} L={candle['low']:.2f} C={candle['close']:.2f} V={candle['volume']}")
        
        # Process the candle (this will add to buffer and potentially trigger aggregation)
        client.process_new_candle(symbol, candle)
        
        # Check buffer size
        if symbol in client.candle_buffers:
            buffer_size = len(client.candle_buffers[symbol])
            print(f"   Buffer size: {buffer_size}")
        
        # Small delay to simulate real-time
        time.sleep(0.1)
    
    print("\n📊 Checking aggregation results...")
    
    # Check if higher timeframe files were created
    timeframes_to_check = ['5m', '10m']  # We only have 10 minutes of data
    
    for timeframe in timeframes_to_check:
        csv_file = f"data/{timeframe}/{symbol}.csv"
        try:
            if pd.io.common.file_exists(csv_file):
                df = pd.read_csv(csv_file)
                print(f"✅ {timeframe} aggregation successful: {len(df)} candles created")
                if len(df) > 0:
                    for idx, row in df.iterrows():
                        timestamp_dt = pd.Timestamp(row['timestamp'], unit='ms', tz='US/Eastern')
                        print(f"   {timeframe} Candle {idx+1}: {timestamp_dt.strftime('%H:%M')} - O={row['open']:.2f} H={row['high']:.2f} L={row['low']:.2f} C={row['close']:.2f} V={row['volume']}")
            else:
                print(f"⚠️  {timeframe} aggregation file not found")
        except Exception as e:
            print(f"❌ Error checking {timeframe} aggregation: {e}")
    
    # Check inverse symbol aggregation
    inverse_symbol = f"{symbol}_inverse"
    print(f"\n📊 Checking inverse symbol ({inverse_symbol}) aggregation...")
    
    for timeframe in timeframes_to_check:
        csv_file = f"data/{timeframe}/{inverse_symbol}.csv"
        try:
            if pd.io.common.file_exists(csv_file):
                df = pd.read_csv(csv_file)
                print(f"✅ {timeframe} inverse aggregation successful: {len(df)} candles created")
            else:
                print(f"⚠️  {timeframe} inverse aggregation file not found")
        except Exception as e:
            print(f"❌ Error checking {timeframe} inverse aggregation: {e}")

def test_aggregation_timing():
    """Test aggregation timing logic"""
    print("\n🧪 Testing aggregation timing logic...")
    
    client = SchwabStreamerClient(debug=True)
    symbol = 'TIMING_TEST'
    
    # Test different minute boundaries
    test_minutes = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
    base_timestamp = 1735828200000  # 09:30:00
    
    for minute in test_minutes:
        # Calculate timestamp for this minute
        timestamp = base_timestamp + (minute * 60 * 1000)
        
        # Check if aggregation should happen for each timeframe
        print(f"Minute {minute:2d}:")
        for timeframe in ['5m', '10m', '15m', '30m']:
            should_agg = client.should_aggregate(symbol, timeframe, timestamp)
            status = "✅ AGGREGATE" if should_agg else "⏸️  WAIT"
            print(f"   {timeframe}: {status}")

if __name__ == "__main__":
    print("🚀 Starting Candle Aggregation Tests")
    print("=" * 50)
    
    try:
        # Test 1: Candle aggregation functionality
        test_candle_aggregation()
        
        # Test 2: Aggregation timing logic
        test_aggregation_timing()
        
        print("\n🎉 All aggregation tests completed!")
        
    except KeyboardInterrupt:
        print("\n👋 Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc() 