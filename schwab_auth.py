"""
Schwab API Authentication Module
Handles all authentication, token management, and API credential functionality
"""

import os
import json
import base64
import requests
import logging
import time as time_module
from datetime import datetime, timedelta
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchwabAuth:
    def __init__(self, access_token_filepath: str = 'schwab_access_token.txt', refresh_token_filepath: str = 'schwab_refresh_token.txt', credentials_filepath: str = 'schwab_credentials.env', APP_KEY_ENV_VAR: str = 'SCHWAB_APP_KEY', APP_SECRET_ENV_VAR: str = 'SCHWAB_APP_SECRET'):
        self.access_token_filepath = access_token_filepath
        self.refresh_token_filepath = refresh_token_filepath
        self.credentials_file = credentials_filepath
        self.APP_KEY_ENV_VAR = APP_KEY_ENV_VAR
        self.APP_SECRET_ENV_VAR = APP_SECRET_ENV_VAR

        # Last token refresh time
        self.last_token_refresh = None
        # Token refresh interval
        self.token_refresh_interval = 20 * 60  # 20 minutes in seconds
        # Minimum token buffer before expiration
        self.min_token_buffer = 5 * 60  # 5 minutes buffer before expiration
        # Maximum retries
        self.max_retries = 3
        # Retry delay
        self.retry_delay = 2  # seconds
        
    def load_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        """Load Schwab API credentials from environment file"""
        if not os.path.exists(self.credentials_file):
            logger.error(f"Credentials file not found: {self.credentials_file}")
            return None, None
            
        try:
            with open(self.credentials_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
                        
            app_key = os.getenv(self.APP_KEY_ENV_VAR)
            app_secret = os.getenv(self.APP_SECRET_ENV_VAR)
            
            if not app_key or not app_secret:
                logger.error(f"Missing {self.APP_KEY_ENV_VAR} or {self.APP_SECRET_ENV_VAR}")
                return None, None
                
            return app_key, app_secret
            
        except Exception as e:
            logger.error(f"Error loading credentials file: {e}")
            return None, None

    def is_token_valid(self) -> bool:
        """Check if current access token is still valid"""
        try:
            with open(self.access_token_filepath, 'r') as f:
                token_data = json.load(f)
            
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            buffer_time = datetime.now() + timedelta(seconds=self.min_token_buffer)
            
            return buffer_time < expires_at
            
        except (FileNotFoundError, KeyError, ValueError) as e:
            logger.error(f"Token validation error: {e}")
            return False

    def should_refresh_token_proactively(self) -> bool:
        """Check if we should proactively refresh the token based on the last refresh time"""
        if not self.last_token_refresh:
            return True
        # If time since last token refresh is greater than 20 minutes, refresh
        time_since_refresh = time_module.time() - self.last_token_refresh
        return time_since_refresh >= self.token_refresh_interval

    def get_access_token(self) -> Optional[str]:
        """Get current access token, refresh if needed"""
        try:
            # Check if we should proactively refresh
            if self.should_refresh_token_proactively():
                logger.info("Proactive token refresh (20-minute interval)")
                if self.refresh_access_token():
                    self.last_token_refresh = time_module.time()
                else:
                    logger.warning("Proactive token refresh failed")
            
            # Check if token is still valid
            if not self.is_token_valid():
                logger.info("Access token expired, refreshing...")
                if self.refresh_access_token():
                    self.last_token_refresh = time_module.time()
                else:
                    return None
            
            with open(self.access_token_filepath, 'r') as f:
                token_data = json.load(f)
            return token_data['access_token']
                
        except FileNotFoundError:
            logger.error("Access token file not found")
            return None
        except Exception as e:
            logger.error(f"Error loading access token: {e}")
            return None

    def refresh_access_token(self) -> bool:
        try:
            logger.info("Refreshing access token...")
            
            app_key, app_secret = self.load_credentials()
            if not app_key or not app_secret:
                return False
            
            # Load refresh token
            with open(self.refresh_token_filepath, 'r') as f:
                refresh_token = f.read().strip()
            
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
            
            for attempt in range(self.max_retries):
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
                        
                        with open(self.access_token_filepath, 'w') as f:
                            json.dump(token_info, f)
                        
                        if 'refresh_token' in token_data:
                            with open('schwab_refresh_token.txt', 'w') as f:
                                f.write(token_data['refresh_token'])
                        
                        logger.info("Access token refreshed successfully")
                        return True
                    else:
                        logger.error(f"Token refresh failed (attempt {attempt + 1}/{self.max_retries}): {response.status_code}")
                        logger.error(f"Response: {response.text}")
                        
                        if attempt < self.max_retries - 1:
                            time_module.sleep(self.retry_delay)
                            continue
                        return False
                        
                except requests.exceptions.RequestException as e:
                    logger.error(f"Network error during token refresh (attempt {attempt + 1}/{self.max_retries}): {e}")
                    if attempt < self.max_retries - 1:
                        time_module.sleep(self.retry_delay)
                        continue
                    return False
            
            return False
            
        except Exception as e:
            logger.error(f"Error during token refresh: {e}")
            return False