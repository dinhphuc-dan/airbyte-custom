
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import time
import hashlib
import requests


class MintegralTokenAuthenticator(requests.auth.AuthBase):
    def __init__(self, config: Mapping[str, Any]):
        self._access_key = config["access_key"]
        self._api_key = config["api_key"]
    
    ''' adding token and other required info into requests headers, the below headers is correct one
        headers={
            "access-key": access_key,
            "token": token,
            "timestamp": str(now)
        } '''
    def __call__(self, r: requests.Request) -> requests.Request:
        now: int = int(time.time())
        token: str = hashlib.md5((self._api_key + hashlib.md5(str(now).encode()).hexdigest()).encode()).hexdigest()
        r.headers.update({
            "access-key": self._access_key, 
            "token": token, 
            "timestamp": str(now)
        })
        return r