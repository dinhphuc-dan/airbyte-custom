from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
import pendulum
from urllib.parse import urlparse, parse_qs


# Base Stream
class AppsflyerCustomStream(HttpStream, ABC):
    url_base = "https://hq1.appsflyer.com/api/"
    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)  # Method Resolution Order to solve Mutiple Inheritance
        self.config = config

    @property
    def http_method(self) -> str:
        """Override if needed. Default by airbyte is GET"""
        return "GET"

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


class AppsFlyerCheckConnectionStream(AppsflyerCustomStream):
    limit = 100

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "mng/apps"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response = response.json()
        if response.get("links") and response["links"].get("next"):
            next_page_link = response["links"]["next"]
            next_page_params = parse_qs(urlparse(next_page_link).query)
            return next_page_params["offset"][0]
        else:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"capabilities": "cost", "limit": self.limit}

        if next_page_token:
            params.update({"offset": next_page_token})

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        list_app_info = []
        for item in response_json["data"]:
            list_app_info.append(item["id"])
            yield item["id"]
