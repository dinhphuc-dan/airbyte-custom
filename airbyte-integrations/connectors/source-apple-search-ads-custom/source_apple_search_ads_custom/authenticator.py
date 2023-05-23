#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import time
import gzip
from datetime import datetime, timedelta
import json
import jwt
import requests
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

class AppleSearchAdsAPIAuthenticator(requests.auth.AuthBase):
    _jwt_encode_algorithm = "ES256"

    def __init__(self, config: Mapping[str, Any], access_token=None):
        self._private_key = config["private_key"]
        self._client_id = config["client_id"]
        self._team_id = config["team_id"]
        self._key_id = config["key_id"]
        self._access_token = access_token
    
    def _get_jwt_header(self) -> dict:
        header = {}
        # header.update({"alg":self._jwt_encode_algorithm})
        header.update({"kid":self._key_id})
        return header

    def _get_jwt_payload(self) -> dict:
        payload = {}
        payload.update({"iss": self._team_id})

        timestamp_now = datetime.now()
        token_start_time = int(time.mktime(timestamp_now.timetuple()))
        token_expire_time = int(time.mktime((timestamp_now + timedelta(seconds=3590)).timetuple()))
        
        payload.update({"iat": token_start_time})
        payload.update({"exp": token_expire_time})
        payload.update({"aud": "https://appleid.apple.com"})
        payload.update({"sub": self._client_id})
        return payload
    
    def _get_signed_token(self) -> dict:
        payload = self._get_jwt_payload()
        header = self._get_jwt_header()
        signed_jwt_token = jwt.encode(payload=payload, key=self._private_key,algorithm=self._jwt_encode_algorithm,headers=header)
        return signed_jwt_token
        
    def _get_access_token(self) -> dict:
        client_secret = self._get_signed_token()
        request_method = "POST"
        request_url = 'https://appleid.apple.com/auth/oauth2/token'
        request_header = {
            "Host": "appleid.apple.com",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        request_params = {
            "client_id": self._client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": "searchadsorg"
        }
        try: 
            call_api = requests.request(method=request_method, url=request_url,headers=request_header, params=request_params)
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error getting access token: {e}") from e
        request_response: dict = call_api.json()
        # timestamp_now = datetime.now()
        # access_token_expire_time = int(time.mktime((timestamp_now + timedelta(seconds=3590)).timetuple()))
        access_token_expire_time = datetime.now() + timedelta(seconds=3590)
        request_response.update({"access_token_expire_time": access_token_expire_time})

        return request_response

    def _handel_access_token_expire_time(self):
        if not self._access_token:
            print("happen in auth if not")
            self._access_token = self._get_access_token()
        elif self._access_token["access_token_expire_time"] < datetime.now():
            print("happen in auth elif")
            self._access_token = self._get_access_token()
        else:
            print("happen in auth else")
            self._access_token
        return self._access_token

    def __call__(self, r: requests.Request) -> requests.Request:
        access_token_in_dict: dict = self._handel_access_token_expire_time()
        access_token = access_token_in_dict['access_token']
        r.headers["Authorization"] = f"Bearer {access_token}"
        return r



# config_file = open(r"D:\airbyte_custom\airbyte-integrations\connectors\source-apple-search-ads-custom\secrets\config.json" )
# config = json.load(config_file)
# config_file.close()
# foo = AppleSearchAdsAPIAuthenticator(config=config)
# print(foo._private_key)
# access_token_in_dict = foo._handel_access_token_expire_time()
# access_token = access_token_in_dict['access_token']
# org_id = 3724370
# print(access_token)
# header = {}
# header.update({"Authorization": f"Bearer {access_token}"})
# header.update({"X-AP-Context": f"orgId={org_id}"})
# body_json = {}
# body_json.update({
#     'startTime': "2023-05-16",
#     'endTime': "2023-05-16",
# })
# body_json.update({"granularity": "DAILY"})
# body_json.update({ "selector": {
#     "orderBy": [
#       {
#         "field": "campaignId",
#         "sortOrder": "DESCENDING"
#       }
#     ],
#     "conditions": [
#     ],
#     "pagination": {
#       "offset": 0,
#       "limit": 1000
#     }
#   }})
# body_json.update({"groupBy": ["countryOrRegion"]})
# body_json.update({  "timeZone": "UTC",
#   "returnRecordsWithNoMetrics": False,
#   "returnRowTotals": False,
#   "returnGrandTotals": False
# })




# call_api_campaign = requests.request(
#     method="POST", 
#     url="https://api.searchads.apple.com/api/v4/reports/campaigns",
#     headers=header, 
#     json=body_json
# )

# result = call_api_campaign.json()
# print(f"{result}")