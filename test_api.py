#!/usr/bin/env python3

from schwab_auth import SchwabAuth
import requests
from datetime import datetime, timedelta

def test_schwab_api():
    """Simple test to check if Schwab API is working"""
    
    # Initialize auth
    auth = SchwabAuth()
    
    # Validate credentials
    if not auth.validate_credentials():
        print("❌ Schwab credentials validation failed")
        return False
        
    # Get headers
    headers = auth.get_auth_headers()
    if not headers:
        print("❌ No valid authentication headers")
        return False
        
    print("✅ Authentication successful")
    
    # Try a simple quote request first (this should work)
    print("\n🧪 Testing quotes API...")
    quote_url = "https://api.schwabapi.com/marketdata/v1/quotes?symbols=SPY"
    
    try:
        response = requests.get(quote_url, headers=headers, timeout=30)
        print(f"Quote API status: {response.status_code}")
        if response.status_code == 200:
            print("✅ Quotes API working")
        else:
            print(f"❌ Quotes API failed: {response.text[:200]}")
    except Exception as e:
        print(f"❌ Quotes API error: {e}")
    
    # Try price history with very simple parameters
    print("\n🧪 Testing price history API...")
    history_url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
    
    # Simple parameters - just last 5 days
    params = {
        'symbol': 'SPY',
        'periodType': 'day',
        'period': 5,
        'frequencyType': 'minute',
        'frequency': 5,
        'needExtendedHoursData': 'false',
        'needPreviousClose': 'false'
    }
    
    print(f"Parameters: {params}")
    
    try:
        response = requests.get(history_url, headers=headers, params=params, timeout=30)
        print(f"Price history status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        print(f"Response text: {response.text[:500]}")
        
        if response.status_code == 200:
            data = response.json()
            if 'candles' in data:
                print(f"✅ Got {len(data['candles'])} candles")
            else:
                print("⚠️ No candles in response")
        else:
            print(f"❌ Price history failed")
            
    except Exception as e:
        print(f"❌ Price history error: {e}")

if __name__ == "__main__":
    test_schwab_api() 