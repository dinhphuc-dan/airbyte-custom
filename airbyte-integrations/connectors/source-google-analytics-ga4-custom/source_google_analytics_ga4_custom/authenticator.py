#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import json
import pendulum
import jwt
import requests
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

class GoogleServiceKeyAuthenticator(requests.auth.AuthBase):
    _google_oauth2_token_endpoint = "https://oauth2.googleapis.com/token"
    _google_oauth2_scope_endpoint = "https://www.googleapis.com/auth/analytics.readonly"
    _google_oauth2_grant_type_urn = "urn:ietf:params:oauth:grant-type:jwt-bearer"

    _default_token_lifetime_secs = 3600
    _jwt_encode_algorithm = "RS256"

    def __init__(self, credentials: dict):
        self._client_email = credentials["client_email"]
        self._client_secret = credentials["private_key"]
        self._client_id = credentials["client_id"]

        self._token: dict = {}
    
    def _get_claims(self) -> dict:
        now = pendulum.now()
        expiry = now.add(seconds=self._default_token_lifetime_secs)

        return {
            "iss": self._client_email,
            "scope": self._google_oauth2_scope_endpoint,
            "aud": self._google_oauth2_token_endpoint,
            "exp": expiry.int_timestamp,
            "iat": now.int_timestamp,
        }

    def _get_signed_payload(self) -> dict:
        claims = self._get_claims()
        assertion = jwt.encode(claims, self._client_secret, algorithm=self._jwt_encode_algorithm)
        return {"grant_type": self._google_oauth2_grant_type_urn, "assertion": str(assertion)}

    def _token_expired(self):
        if not self._token:
            return True
        return self._token["expires_at"] < pendulum.now().int_timestamp
    
    
    def _rotate(self):
        if self._token_expired():
            try:
                response = requests.request(method="POST", url=self._google_oauth2_token_endpoint, params=self._get_signed_payload()).json()
            except requests.exceptions.RequestException as e:
                raise Exception(f"Error refreshing access token: {e}") from e
            self._token = dict(
                **response,
                expires_at=pendulum.now().add(seconds=response["expires_in"]).int_timestamp,
            )

    def __call__(self, r: requests.Request) -> requests.Request:
        self._rotate()

        r.headers["Authorization"] = f"Bearer {self._token['access_token']}"
        return r

# config_file = open(r"G:\airbyte_custom\airbyte-integrations\connectors\source-google-analytics-GA4-custom\secrets\config.json" )
# config = json.load(config_file)
# config_file.close()
# cre = json.loads(config["credentials"]["credentials_json"])
# foo = GoogleServiceKeyAuthenticator(credentials = cre)
# foo._rotate()
# e = foo._token
# f = e['access_token']
# headers = {"Authorization": f"Bearer {f}", "Accept":"application/json"}
# url_base = f"https://analyticsdata.googleapis.com/v1beta/properties/{secret['property_id']}/metadata"
# print(url_base)
# print(headers)
# r = requests.request(method="GET", url=url_base, headers=headers)
# x = r.json()
# print(x)
