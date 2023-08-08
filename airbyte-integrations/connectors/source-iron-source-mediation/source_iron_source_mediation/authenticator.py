from datetime import datetime, timedelta
import requests
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import json
import logging

logger = logging.getLogger('airbyte')

class IronSourceMediationAuthenticator(requests.auth.AuthBase):
    auth_url = 'https://platform.ironsrc.com/partners/publisher/auth'
    auth_method = 'GET'

    def __init__(self, config: dict, access_token_as_dict: dict =None):
        self.secret_key = config["secret_key"]
        self.refresh_token = config["refresh_token"]
        self._access_token_and_expire_time = access_token_as_dict
    
    def _get_access_token(self):
        request_header = {
            'secretkey':self.secret_key,
            'refreshToken': self.refresh_token,
        }

        try:
            call_authentication_api = requests.request(
                method=self.auth_method,
                url=self.auth_url,
                headers=request_header
            )
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error getting access token: {e}") from e
        
        access_token = call_authentication_api.json()
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
    


# with open(r"D:\airbyte_custom\airbyte-integrations\connectors\source-iron-source-mediation\secrets\config.json", "r" ) as config_file:
#     config = json.load(config_file)

# test_api = IronSourceMediationAuthenticator(config=config)
# test_access_token = test_api._get_access_token()
# print(test_access_token)
# print(type(test_access_token))
# x = test_api._handel_access_token_expire_time()
# print(x)