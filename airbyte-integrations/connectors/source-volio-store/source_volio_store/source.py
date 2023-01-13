#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import json
import pendulum
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class VolioStoreStream(HttpStream, ABC):
    url_base = "https://store.volio.vn/api/v3/public/analyst"

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config: dict = config
        self.store_token: str = self.config["store_token"]
        self.dict_app_name_and_id: dict = json.loads(self.config["app_id"])

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}

class BaseStream(VolioStoreStream):
    primary_key = None
    
    @property
    def http_method(self) -> str:
        return "GET"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        path = f"?token={self.store_token}"
        return path

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield response_json

class AppItemsIDStream(BaseStream):

    def __init__(self, app_name:str = None, app_id: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app_name = app_name
        self.app_id = app_id
    
    @property
    def name(self) -> str:
        postfix = "_items_catergory"
        stream_name = self.app_name + postfix
        return stream_name

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        path = f"?token={self.store_token}&app_id={self.app_id}"
        return path

    def get_json_schema(self) -> Mapping[str, Any]:
        schema: dict[str, Any] = {
             "$schema": "http://json-schema.org/draft-07/schema#",
            "type": ["null", "object"],
            "additionalProperties": True,
            "properties": {
                "app_name":{"type": ["null", "string"]},
                "app_id":{"type": ["null", "number"]},
                "module_name":{"type": ["null", "string"]},
                "module_id":{"type": ["null", "number"]},
                "categories_name":{"type": ["null", "string"]},
                "categories_id":{"type": ["null", "number"]},
                "item_name":{"type": ["null", "string"]},
                "item_id":{"type": ["null", "number"]},
                "item_image_link":{"type": ["null", "string"]},
            },
        }
        return schema
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        result = {}
        response_json = response.json()
        for data in response_json["data"]["data"]:
            record = data
        for category in record["categories"]:
            for item in category["items"]:
                result.update({"app_name": self.app_name})
                result.update({"app_id": record['app_id']})
                result.update({"module_name": record['name']})
                result.update({"module_id": record['id']})
                result.update({"categories_name": category['name']})
                result.update({"categories_id": category['id']})
                result.update({"item_name": item['name']})
                result.update({"item_id": item['id']})
                result.update({"item_image_link": item['icon']})
                yield result


# Source
class SourceVolioStore(AbstractSource):

    def _get_list_stream(self, config) -> list[Any]:
        app_name_and_id = BaseStream(config=config).dict_app_name_and_id
        for name, id in app_name_and_id.items():
            yield AppItemsIDStream(app_name= name, app_id= id, config=config)

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            stream = BaseStream(config=config)
            logger.info(f'stream is {stream}')
            stream_record = stream.read_records(sync_mode = 'full_refresh')
            logger.info(f'stream record is {stream_record}')
            record = next(stream_record)
            logger.info(f'record is {record}')
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = []
        streams.extend(self._get_list_stream(config=config))
        return streams
