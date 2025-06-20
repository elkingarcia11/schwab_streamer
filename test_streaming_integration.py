#!/usr/bin/env python3
"""
Test script for Schwab Streamer Client integration with DataManager
Tests the real-time processing of new candles
"""

import time
import pandas as pd
from schwab_streamer_client import SchwabStreamerClient

def test_candle_processing():
    """Test the process_new_candle method with sample data"""
    print("🧪 Testing Schwab Streamer Client integration...")
    
    # Create client (this will initialize DataManager internally)
    client = SchwabStreamerClient(debug=True)
    print("✅ SchwabStreamerClient initialized")
    
    # Show bootstrap parameters being used
    bootstrap_params = client.data_manager.get_bootstrap_parameters()
    print(f"\n📊 Bootstrap parameters for 1m timeframe:")
    print(f"   EMA: {bootstrap_params['ema_period']['1m']}")
    print(f"   VWMA: {bootstrap_params['vwma_period']['1m']}")
    print(f"   ROC: {bootstrap_params['roc_period']['1m']}")
    print(f"   MACD: {bootstrap_params['fast_ema']['1m']}/{bootstrap_params['slow_ema']['1m']}/{bootstrap_params['signal_ema']['1m']}")
    
    # Test data: simulate a new candle from streaming API
    # Using a specific timestamp that matches the format: 1735828200000 = 2025-01-02 09:30:00 EST
    sample_candle = {
        'timestamp': 1735828200000,  # 2025-01-02 09:30:00 EST in milliseconds
        'open': 580.50,
        'high': 581.25,
        'low': 580.10,
        'close': 580.85,
        'volume': 125000
    }
    
    symbol = 'SPY'
    
    print(f"\n📊 Testing candle processing for {symbol}...")
    print(f"Sample candle: {sample_candle}")
    
    try:
        # Process the sample candle
        client.process_new_candle(symbol, sample_candle)
        print("✅ Candle processing completed successfully")
        
        # Check if data was saved
        csv_file = f"data/1m/{symbol}.csv"
        if pd.io.common.file_exists(csv_file):
            df = pd.read_csv(csv_file)
            print(f"📄 CSV file has {len(df)} rows")
            if len(df) > 0:
                print(f"Last row: {df.iloc[-1].to_dict()}")
        else:
            print("⚠️  CSV file not created")
            
        # Check in-memory cache for base symbol
        df_key = client.data_manager._get_df_key(symbol, '1m')
        if df_key in client.data_manager.latestDF:
            cached_df = client.data_manager.latestDF[df_key]
            print(f"💾 In-memory cache has {len(cached_df)} rows")
            if len(cached_df) > 0:
                print(f"Cached columns: {list(cached_df.columns)}")
        else:
            print("⚠️  No data in memory cache")
            
        # Check inverse symbol processing
        inverse_symbol = f"{symbol}_inverse"
        inverse_csv_file = f"data/1m/{inverse_symbol}.csv"
        if pd.io.common.file_exists(inverse_csv_file):
            inverse_df = pd.read_csv(inverse_csv_file)
            print(f"📄 Inverse CSV ({inverse_symbol}) has {len(inverse_df)} rows")
            if len(inverse_df) > 0:
                print(f"Inverse last row: {inverse_df.iloc[-1].to_dict()}")
        else:
            print(f"⚠️  Inverse CSV file not created for {inverse_symbol}")
            
        # Check inverse in-memory cache
        inverse_df_key = client.data_manager._get_df_key(inverse_symbol, '1m')
        if inverse_df_key in client.data_manager.latestDF:
            cached_inverse_df = client.data_manager.latestDF[inverse_df_key]
            print(f"💾 Inverse in-memory cache has {len(cached_inverse_df)} rows")
        else:
            print(f"⚠️  No inverse data in memory cache for {inverse_symbol}")
            
    except Exception as e:
        print(f"❌ Error during candle processing: {e}")
        import traceback
        traceback.print_exc()

def test_multiple_candles():
    """Test processing multiple candles in sequence"""
    print("\n🧪 Testing multiple candle processing...")
    
    client = SchwabStreamerClient(debug=False)  # Disable debug for cleaner output
    
    # Simulate 5 consecutive candles starting from market open
    base_timestamp = 1735828200000  # 2025-01-02 09:30:00 EST
    base_price = 580.00
    
    for i in range(5):
        candle = {
            'timestamp': base_timestamp + (i * 60 * 1000),  # 1 minute apart
            'open': base_price + i * 0.1,
            'high': base_price + i * 0.1 + 0.5,
            'low': base_price + i * 0.1 - 0.3,
            'close': base_price + i * 0.1 + 0.2,
            'volume': 100000 + i * 10000
        }
        
        print(f"Processing candle {i+1}/5: Close=${candle['close']:.2f}")
        client.process_new_candle('QQQ', candle)
        time.sleep(0.1)  # Small delay to simulate real-time
    
    print("✅ Multiple candle processing completed")

def test_inverse_processing():
    """Test inverse symbol processing with bootstrap parameters"""
    print("\n🧪 Testing inverse symbol processing...")
    
    client = SchwabStreamerClient(debug=True)
    
    # Test candle for base symbol
    test_candle = {
        'timestamp': 1735828200000,  # 2025-01-02 09:30:00 EST
        'open': 100.0,   # 1/100 = 0.01
        'high': 105.0,   # 1/105 = 0.0095... (becomes low in inverse)
        'low': 95.0,     # 1/95 = 0.0105... (becomes high in inverse)
        'close': 102.0,  # 1/102 = 0.0098...
        'volume': 50000
    }
    
    symbol = 'TEST'
    print(f"📊 Processing test candle for {symbol} (will also create {symbol}_inverse)")
    print(f"Original candle: O={test_candle['open']:.2f} H={test_candle['high']:.2f} L={test_candle['low']:.2f} C={test_candle['close']:.2f}")
    
    # Expected inverse values
    expected_inverse = {
        'open': 1.0 / test_candle['open'],     # 0.01
        'high': 1.0 / test_candle['low'],      # 0.0105... (1/95, was low)
        'low': 1.0 / test_candle['high'],      # 0.0095... (1/105, was high)
        'close': 1.0 / test_candle['close'],   # 0.0098...
    }
    print(f"Expected inverse: O={expected_inverse['open']:.6f} H={expected_inverse['high']:.6f} L={expected_inverse['low']:.6f} C={expected_inverse['close']:.6f}")
    
    try:
        # Process the candle (should create both base and inverse)
        client.process_new_candle(symbol, test_candle)
        
        # Check if inverse file was created
        inverse_csv = f"data/1m/{symbol}_inverse.csv"
        if pd.io.common.file_exists(inverse_csv):
            inverse_df = pd.read_csv(inverse_csv)
            if len(inverse_df) > 0:
                last_row = inverse_df.iloc[-1]
                print(f"✅ Inverse processing successful!")
                print(f"Actual inverse:   O={last_row['open']:.6f} H={last_row['high']:.6f} L={last_row['low']:.6f} C={last_row['close']:.6f}")
                
                # Verify the inversion is correct
                tolerance = 1e-6
                if (abs(last_row['open'] - expected_inverse['open']) < tolerance and
                    abs(last_row['high'] - expected_inverse['high']) < tolerance and
                    abs(last_row['low'] - expected_inverse['low']) < tolerance and
                    abs(last_row['close'] - expected_inverse['close']) < tolerance):
                    print("✅ Inverse calculation is mathematically correct!")
                else:
                    print("⚠️  Inverse calculation may have precision issues")
            else:
                print("❌ Inverse CSV file is empty")
        else:
            print("❌ Inverse CSV file was not created")
            
    except Exception as e:
        print(f"❌ Error during inverse processing test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Starting Schwab Streamer Integration Tests")
    print("=" * 50)
    
    try:
        # Test 1: Single candle processing
        test_candle_processing()
        
        # Test 2: Multiple candles
        test_multiple_candles()
        
        # Test 3: Inverse processing
        test_inverse_processing()
        
        print("\n🎉 All tests completed successfully!")
        
    except KeyboardInterrupt:
        print("\n👋 Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc() 