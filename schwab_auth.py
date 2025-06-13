"""
Schwab API Authentication Module
Handles all authentication, token management, and API credential functionality
"""

import os
import json
import base64
import requests
import time as time_module
from datetime import datetime, timedelta
from typing import Optional, Tuple

class SchwabAuth:
    def __init__(self):
        self.last_token_refresh = None
        self.token_refresh_interval = 20 * 60  # 20 minutes in seconds
        
    def load_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        """Load Schwab API credentials from environment file"""
        credentials_file = 'schwab_credentials.env'
        
        if os.path.exists(credentials_file):
            try:
                with open(credentials_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            os.environ[key.strip()] = value.strip()
            except Exception as e:
                print(f"⚠️  Error loading credentials file: {e}")
        
        app_key = os.getenv('SCHWAB_APP_KEY')
        app_secret = os.getenv('SCHWAB_APP_SECRET')
        
        if not app_key or not app_secret:
            print("❌ Missing SCHWAB_APP_KEY or SCHWAB_APP_SECRET")
            return None, None
            
        return app_key, app_secret

    def is_token_valid(self) -> bool:
        """Check if current access token is still valid"""
        try:
            with open('schwab_access_token.txt', 'r') as f:
                token_data = json.load(f)
            
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            # Consider token expired if it expires within 5 minutes
            buffer_time = datetime.now() + timedelta(minutes=5)
            
            return buffer_time < expires_at
            
        except (FileNotFoundError, KeyError, ValueError):
            return False

    def should_refresh_token_proactively(self) -> bool:
        """Check if we should proactively refresh token (every 20 minutes)"""
        if self.last_token_refresh is None:
            return True
            
        time_since_refresh = time_module.time() - self.last_token_refresh
        return time_since_refresh >= self.token_refresh_interval

    def get_access_token(self) -> Optional[str]:
        """Get current access token, refresh if needed"""
        try:
            # Check if we should proactively refresh
            if self.should_refresh_token_proactively():
                print("🕒 Proactive token refresh (20-minute interval)")
                if self.refresh_access_token():
                    self.last_token_refresh = time_module.time()
                else:
                    print("⚠️  Proactive token refresh failed")
            
            # Check if token is still valid
            if not self.is_token_valid():
                print("🔄 Access token expired, refreshing...")
                if self.refresh_access_token():
                    self.last_token_refresh = time_module.time()
                else:
                    return None
            
            with open('schwab_access_token.txt', 'r') as f:
                token_data = json.load(f)
            return token_data['access_token']
                
        except FileNotFoundError:
            print("❌ Access token file not found")
            return None
        except Exception as e:
            print(f"❌ Error loading access token: {e}")
            return None

    def refresh_access_token(self) -> bool:
        """Refresh the access token using refresh token"""
        print("🔄 Refreshing access token...")
        
        app_key, app_secret = self.load_credentials()
        if not app_key or not app_secret:
            return False
        
        try:
            with open('schwab_refresh_token.txt', 'r') as f:
                refresh_token = f.read().strip()
        except Exception as e:
            print(f"❌ Failed to load refresh token: {e}")
            return False
        
        token_url = "https://api.schwabapi.com/v1/oauth/token"
        credentials = f"{app_key}:{app_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token
        }
        
        try:
            response = requests.post(token_url, headers=headers, data=data)
            
            if response.status_code == 200:
                token_data = response.json()
                current_time = datetime.now()
                expires_in = token_data.get('expires_in', 1800)
                expires_at = current_time.timestamp() + expires_in
                
                token_info = {
                    'access_token': token_data['access_token'],
                    'created_at': current_time.isoformat(),
                    'expires_at': datetime.fromtimestamp(expires_at).isoformat(),
                    'expires_in': expires_in
                }
                
                with open('schwab_access_token.txt', 'w') as f:
                    json.dump(token_info, f)
                
                if 'refresh_token' in token_data:
                    with open('schwab_refresh_token.txt', 'w') as f:
                        f.write(token_data['refresh_token'])
                
                print("✅ Access token refreshed successfully")
                return True
            else:
                print(f"❌ Token refresh failed: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error during token refresh: {e}")
            return False

    def get_token_info(self) -> dict:
        """Get information about current token status"""
        try:
            with open('schwab_access_token.txt', 'r') as f:
                token_data = json.load(f)
            
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            current_time = datetime.now()
            time_remaining = expires_at - current_time
            
            return {
                'valid': self.is_token_valid(),
                'expires_at': expires_at,
                'time_remaining': time_remaining,
                'seconds_remaining': time_remaining.total_seconds(),
                'created_at': datetime.fromisoformat(token_data['created_at'])
            }
            
        except (FileNotFoundError, KeyError, ValueError) as e:
            return {
                'valid': False,
                'error': str(e),
                'expires_at': None,
                'time_remaining': None,
                'seconds_remaining': 0,
                'created_at': None
            }

    def validate_credentials(self) -> bool:
        """Validate that all required credential files exist and are accessible"""
        issues = []
        
        # Check credentials file
        if not os.path.exists('schwab_credentials.env'):
            issues.append("Missing schwab_credentials.env file")
        else:
            app_key, app_secret = self.load_credentials()
            if not app_key:
                issues.append("Missing SCHWAB_APP_KEY in credentials file")
            if not app_secret:
                issues.append("Missing SCHWAB_APP_SECRET in credentials file")
        
        # Check refresh token file
        if not os.path.exists('schwab_refresh_token.txt'):
            issues.append("Missing schwab_refresh_token.txt file")
        else:
            try:
                with open('schwab_refresh_token.txt', 'r') as f:
                    refresh_token = f.read().strip()
                if not refresh_token:
                    issues.append("Empty schwab_refresh_token.txt file")
            except Exception as e:
                issues.append(f"Cannot read schwab_refresh_token.txt: {e}")
        
        if issues:
            print("❌ Credential validation failed:")
            for issue in issues:
                print(f"   - {issue}")
            return False
        
        print("✅ All credentials validated successfully")
        return True

    def test_token_refresh(self) -> bool:
        """Test token refresh functionality"""
        print("🧪 Testing token refresh functionality...")
        
        if not self.validate_credentials():
            return False
        
        # Force a token refresh to test the process
        original_refresh_time = self.last_token_refresh
        self.last_token_refresh = None  # Force refresh
        
        success = self.refresh_access_token()
        
        if success:
            self.last_token_refresh = time_module.time()
            print("✅ Token refresh test successful")
        else:
            self.last_token_refresh = original_refresh_time
            print("❌ Token refresh test failed")
        
        return success

    def get_auth_headers(self) -> dict:
        """Get authentication headers for API requests"""
        access_token = self.get_access_token()
        if not access_token:
            return {}
        
        return {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }

    def is_authenticated(self) -> bool:
        """Check if we have valid authentication"""
        return self.get_access_token() is not None 