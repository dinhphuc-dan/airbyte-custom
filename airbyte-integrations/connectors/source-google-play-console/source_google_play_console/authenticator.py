#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import json
import datetime
import jwt
import requests
from source_google_play_console import utils

config_file = open(r"G:\airbyte_custom\airbyte-integrations\connectors\source-google-play-console\secrets\config.json" )
secret = json.load(config_file)
config_file.close()

class GoogleServiceKeyAuthenticator(requests.auth.AuthBase):
    _google_oauth2_token_endpoint = "https://oauth2.googleapis.com/token"
    _google_oauth2_scope_endpoint = "https://www.googleapis.com/auth/androidpublisher"
    _google_oauth2_grant_type_urn = "urn:ietf:params:oauth:grant-type:jwt-bearer"

    _default_token_lifetime_secs = 3600
    _jwt_encode_algorithm = "RS256"

    def __init__(self, credentials: dict):
        print("something happens")
        self._client_email = credentials["client_email"]
        self._client_secret = credentials["private_key"]
        self._client_id = credentials["client_id"]
        self._token: dict = {}

    def _get_claims(self) -> dict:
        now = datetime.datetime.utcnow()
        expiry = now + datetime.timedelta(seconds=self._default_token_lifetime_secs)

        return {
            "iss": self._client_email,
            "scope": self._google_oauth2_scope_endpoint,
            "aud": self._google_oauth2_token_endpoint,
            "exp": utils.datetime_to_timestamp(expiry),
            "iat": utils.datetime_to_timestamp(now),
        }

    def _get_headers(self):
        headers = {}
        if self._client_id:
            headers["kid"] = self._client_id
        return headers

    def _get_signed_payload(self) -> dict:
        claims = self._get_claims()
        headers = self._get_headers()
        assertion = jwt.encode(claims, self._client_secret, algorithm=self._jwt_encode_algorithm)
        return {"grant_type": self._google_oauth2_grant_type_urn, "assertion": str(assertion)}

    def _token_expired(self):
        print("it start here")
        if not self._token:
            print("alo alo")
            return True
        print("no iffff")
        return self._token["expires_at"] < utils.datetime_to_timestamp(datetime.datetime.utcnow())

    def _rotate(self):
        if self._token_expired():
            try:
                response = requests.request(method="POST", url=self._google_oauth2_token_endpoint, params=self._get_signed_payload()).json()
            except requests.exceptions.RequestException as e:
                raise Exception(f"Error refreshing access token: {e}") from e
            self._token = dict(
                **response,
                expires_at=utils.datetime_to_timestamp(datetime.datetime.utcnow() + datetime.timedelta(seconds=response["expires_in"])),
            )
        return self._token

    def __call__(self, r: requests.Request) -> requests.Request:
        print("it running")
        self._rotate()

        r.headers["Authorization"] = f"Bearer {self._token['access_token']}"
        print(r)
        return r

# print(secret["credentials"]["credentials_json"])
cre = json.loads(secret["credentials"]["credentials_json"])
foo = GoogleServiceKeyAuthenticator(credentials = cre)
foo._rotate()
e = foo._token
f = e['access_token']
headers = {"Authorization": f"Bearer {f}", "Accept":"application/json"}
url_base = f"https://androidpublisher.googleapis.com/androidpublisher/v3/applications/{secret['package_id']}/subscriptions"
print(url_base)
print(headers)
r = requests.request(method="GET", url=url_base, headers=headers)
x = r.json()
print(x)
