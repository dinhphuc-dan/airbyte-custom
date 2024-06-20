from datetime import datetime, timedelta
import requests
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import logging
import json

logger = logging.getLogger('airbyte')

class SnapchatAdvertiseAuthenticator(requests.auth.AuthBase):
    auth_url = 'https://accounts.snapchat.com/login/oauth2/access_token'
    auth_method = 'POST'

    def __init__(self, config: dict, access_token_as_dict: dict =None):
        self.client_id = config["client_id"]
        self.client_secret = config["client_secret"]
        self.refresh_token = config["refresh_token"]
        self._access_token_and_expire_time = access_token_as_dict
    
    def _get_access_token(self):
        request_params = {
            'client_id':self.client_id,
            'client_secret':self.client_secret,
            'refresh_token': self.refresh_token,
            'grant_type': 'refresh_token',
        }

        try:
            call_authentication_api = requests.request(
                method=self.auth_method,
                url=self.auth_url,
                params=request_params
            )
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error getting access token: {e}") from e
        
        access_token = call_authentication_api.json().get('access_token')
        access_token_expire_time = datetime.now() + timedelta(seconds=3590)
        access_token_as_dict = {}
        access_token_as_dict.update({
            'access_token': access_token,
            'access_token_expire_time': access_token_expire_time
        })

        return access_token_as_dict
    
    def _handel_access_token_expire_time(self):
        if not self._access_token_and_expire_time or self._access_token_and_expire_time["access_token_expire_time"] < datetime.now():
            # logger.info("happen in auth if not")
            self._access_token_and_expire_time = self._get_access_token()
        else:
            # logger.info("happen in auth else")
            self._access_token_and_expire_time
        return self._access_token_and_expire_time

    def __call__(self, request: requests.Request) -> requests.Request:
        r = request
        access_token_and_expire_time = self._handel_access_token_expire_time()
        access_token = access_token_and_expire_time['access_token']
        r.headers["Authorization"] = f"Bearer {access_token}"
        return r