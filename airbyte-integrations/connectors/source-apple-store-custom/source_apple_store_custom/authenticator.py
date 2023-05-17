#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import time
from datetime import datetime, timedelta
import json
import jwt
import requests
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

class AppleStoreConnectAPIAuthenticator(requests.auth.AuthBase):
    _jwt_encode_algorithm = "ES256"

    def __init__(self, config: Mapping[str, Any]):
        self._issuer_id = config["issuer_id"]
        self._key_id = config["key_id"]
        self._private_key = config["private_key"]
        self._vendor_id = config["vendor_id"]
    
    def _get_jwt_header(self) -> dict:
        header = {}
        # header.update({"alg":self._jwt_encode_algorithm})
        header.update({"kid":self._key_id})
        header.update({"typ":"JWT"})
        return header

    def _get_jwt_payload(self) -> dict:
        payload = {}
        payload.update({"iss": self._issuer_id})

        timestamp_now = datetime.now()
        token_start_time = int(time.mktime(timestamp_now.timetuple()))
        token_expire_time = int(time.mktime((timestamp_now + timedelta(minutes=15)).timetuple()))
        
        payload.update({"iat": token_start_time})
        payload.update({"exp": token_expire_time})
        payload.update({"aud": "appstoreconnect-v1"})
        return payload
    
    def _get_signed_token(self) -> dict:
        payload = self._get_jwt_payload()
        header = self._get_jwt_header()
        signed_jwt_token = jwt.encode(payload=payload, key=self._private_key,algorithm=self._jwt_encode_algorithm,headers=header)
        return signed_jwt_token

    def __call__(self, r: requests.Request) -> requests.Request:
        signed_token = self._get_signed_token()
        r.headers["Authorization"] = f"Bearer {signed_token}"
        return r



# config_file = open(r"D:\airbyte_custom\airbyte-integrations\connectors\source-apple-store-custom\secrets\config.json" )
# config = json.load(config_file)
# config_file.close()
# foo = AppleStoreConnectAPIAuthenticator(config=config)
# print(foo._private_key)
# signed_token = foo._get_signed_token()
# headers = {"Authorization": f"Bearer {signed_token}"}
# url_base = "https://api.appstoreconnect.apple.com/v1/apps"
# print(url_base)
# print(headers)
# r = requests.request(method="GET", url=url_base, headers=headers)
# x = r.json()
# print(x)