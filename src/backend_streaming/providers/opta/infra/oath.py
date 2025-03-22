# Directory: src/backend_streaming/providers/opta/oath.py
import hashlib
import requests
import time
import json
import traceback
from pathlib import Path
from backend_streaming.providers.opta.constants import OUTLET_AUTH_KEY, SECRET_KEY, PATH_TO_CREDENTIALS

def generate_auth_credentials():
    post_url = f"https://oauth.performgroup.com/oauth/token/{OUTLET_AUTH_KEY}?_fmt=json&_rt=b"
    
    # Generate authentication parameters
    timestamp = int(round(time.time() * 1000))
    key = str.encode(OUTLET_AUTH_KEY + str(timestamp) + SECRET_KEY)
    unique_hash = hashlib.sha512(key).hexdigest()
    
    # Request headers and body
    auth_headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {unique_hash}',
        'Timestamp': str(timestamp)
    }
    auth_body = {
        'grant_type': 'client_credentials',
        'scope': 'b2b-feeds-auth'
    }
    
    try:
        # Get access token
        response = requests.post(post_url, data=auth_body, headers=auth_headers)
        response.raise_for_status()
        
        access_token = response.json()['access_token']
        
        # Create API headers with bearer token
        api_headers = {'Authorization': f'Bearer {access_token}'}
        
        # Save credentials to file
        credentials = {
            'access_token': access_token,
            'headers': api_headers,
            'generated_at': timestamp,
            'expires_in': response.json().get('expires_in', 3600)  # typically 1 hour
        }
        # Create directory if it doesn't exist
        cred_path = Path(__file__).parent / 'credentials'
        cred_path.mkdir(parents=True, exist_ok=True)
        with open(cred_path / 'opta_auth.json', 'w') as f:
            json.dump(credentials, f, indent=4)
            
        return credentials
        
    except requests.exceptions.RequestException as e:
        traceback.print_exc()
        print(f"Error generating authentication: {e}")
        return None

def get_auth_headers():
    """Get existing headers or generate new ones if expired"""
    try:
        # Check if credentials file exists and is valid
        cred_file = Path(PATH_TO_CREDENTIALS) / 'opta_auth.json'
        if cred_file.exists():
            with open(cred_file) as f:
                credentials = json.load(f)
            
            # Check if token is expired (with 5 min buffer)
            generated_at = credentials['generated_at']
            expires_in = credentials['expires_in']
            current_time = int(round(time.time() * 1000))
            
            if (current_time - generated_at) < (int(expires_in) * 1000):  # 5 min buffer
                return credentials['headers']
        
        # Generate new credentials if file doesn't exist or token is expired
        new_credentials = generate_auth_credentials()
        return new_credentials['headers'] if new_credentials else None
        
    except Exception as e:
        traceback.print_exc()
        print(f"Error getting authentication headers: {e}")
        return None
    

if __name__ == "__main__":
    print(get_auth_headers())