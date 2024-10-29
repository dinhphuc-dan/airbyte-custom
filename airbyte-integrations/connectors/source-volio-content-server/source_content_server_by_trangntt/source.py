from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


# Basic full refresh stream
class ContentServer(HttpStream, ABC):

    url_base = "https://stores.volio.vn/stores/api/v5.0/data-analysis"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
    primary_key = None

    page_size = 500

    def __init__(self, config: Mapping[str, Any], stream_name, package_name, **kwargs):
        super().__init__()
        self.access_token = config["access_token"]
        self.stream_name = stream_name
        self.package_name = package_name


    @property 
    def name(self) -> str: 
        return self.stream_name

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None


    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {'Authorization': 'Bearer ' + self.access_token}

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,) -> Optional[Mapping[str, Any]]:
        params = {
            "package_name": self.package_name,
            "page_size": self.page_size
        }
        if next_page_token:
            page = next_page_token
            params.update({"page": page})
        return params


    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        
        result = response.json()
        for record in result['data']:
            yield record
    

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        total_pages = response_json['paging']['total_pages']
        current_page = response_json['paging']['current_page']
        next_page = response_json['paging']['next_page']
        if total_pages == 1: 
            return None
        elif current_page == total_pages:
            return None  
        else: 
            next_page
            return next_page


    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None
    

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["null","string"]},
                "item_old_id": {"type": ["null","string"]},
                "name": {"type": ["null","string"]},
                "category_id": {"type": ["null","string"]},
                "category_old_id": {"type": ["null","string"]},
                "category_name": {"type": ["null","string"]},
                "url": {"type": ["null","string"]}
            }
        }
        return full_schema

# Check connection 

class ContentServerCheckConnection(ContentServer):

    # request_params to read 1 record per package name 
    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,) -> Optional[Mapping[str, Any]]:
        params = {
            "package_name": self.package_name,
            "page_size": 1,
            "page": 1
        }
        return params
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

# Source
class SourceContentServerByTrangntt(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]: 
        try: 
            auth = TokenAuthenticator(token=config["access_token"])
            streams = []
            for package_name in config["list_package_name"]:
                stream_name = package_name.replace(".","_")
                streams.extend([ContentServerCheckConnection(authenticator=auth, config=config, stream_name=stream_name, package_name=package_name)])
            # def check_connection_inner():
            logger.info(f"There are {len(streams)} streams")
            for stream in streams:
                stream_records = stream.read_records(sync_mode='full_refresh')
                logger.info(f"Prepare to connect to package: {stream.package_name}")
                record = next(stream_records)
                logger.info(f"Successfully read records this package name and There is one of records: {record}")
            # check_connection_inner()
            return True, None
        except Exception as e:
            return False, e
                 

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["access_token"]) 
        streams = []  
        list_package_name = config["list_package_name"]
        for package_name in list_package_name:
            stream_name = package_name.replace(".","_")
            # streams.extend([ContentServer(authenticator=auth, config=config, stream_name=stream_name, package_name=package_name )])
            streams.extend([ContentServer(authenticator=auth, config=config, stream_name=stream_name, package_name=package_name )])
        return streams
        
