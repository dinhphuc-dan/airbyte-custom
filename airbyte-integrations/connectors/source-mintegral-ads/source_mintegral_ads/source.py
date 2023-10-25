#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
import pendulum
import datetime
from source_mintegral_ads.authenticator import MintegralTokenAuthenticator


# Basic full refresh stream
class MintegralAdsStream(HttpStream, ABC):
    primary_key = None
    url_base = "https://ss-api.mintegral.com/api/"

    """
    Override HttpSteam contructor method, add config params to read config value from config setup by users
    **kwargs for authentication part of HttStream --> go to check connection method
    """
    def __init__(self,config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}
    
    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None


class MintegralCheckConnection(MintegralAdsStream):
    max_limit_per_page = 2

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "open/v1/campaign"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        total = response_json['data']['total']
        limit = response_json['data']['limit']
        page = response_json['data']['page']
        if page*limit > total:
            return None
        else:
            return page
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {}
        params.update({"limit": self.max_limit_per_page, "page": 1})
        if next_page_token:
            next_page = next_page_token + 1
            params.update({"page": next_page})
        print(f"request params AFTER IF, params as {params}")
        return params
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        list_campaign_as_dict = {}
        response_json = response.json()
        for record in response_json['data']['list']:
            list_campaign_as_dict.update({'campaign_id': record['campaign_id'], 'campaign_name': record['campaign_name']})
            yield list_campaign_as_dict

class MintegralAdsReport(MintegralAdsStream, IncrementalMixin):
    chunk_date_range = 6
    def __init__(self,list_campaign_as_dict,**kwargs):
        super().__init__(**kwargs)
        self._list_campaign_as_dict: dict = list_campaign_as_dict
        self._cursor_value = None
        self.number_days_backward: int = self.config.get("number_days_backward", 7)
        self.timezone: str  = self.config.get("timezone", "UTC")
        self.utc_offset: str  = str(self.config.get("utc_offset", 0))
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v1/reports/data"

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name """
        stream_name = "Campaign_Offer_Country"
        return stream_name

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

    @property
    def state(self) -> Mapping[str, Any]:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        
        # data_available_date is the date that the newest data can be accessed
        data_avaliable_date : datetime.date = pendulum.today(self.timezone).date()

        if stream_state:
            ''' this code for incremental run, the stream will start with the last date of record minus number_days_backward'''
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        else: 
            '''' this code for the first time run or full refresh run, the stream will start with the start date in config'''
            start_date: datetime.date = pendulum.parse(self.config["start_date"]).date()
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        while start_date <= data_avaliable_date:
            start_date_as_str: str = start_date.to_date_string()
            if (data_avaliable_date - start_date).days >= self.chunk_date_range:
                end_date: datetime.date = start_date.add(days=self.chunk_date_range)
                end_date_as_str: str = end_date.to_date_string()
                slice.append({
                    'start_date': start_date_as_str,
                    'end_date': end_date_as_str,
                })
            else:
                end_date: datetime.date = data_avaliable_date
                end_date_as_str: str = end_date.to_date_string()
                slice.append({
                    'start_date': start_date_as_str,
                    'end_date': end_date_as_str,
                })
            start_date: datetime.date = end_date.add(days=1)

        return slice or [None]
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        for campaign in self._list_campaign_as_dict:
            request_params.update(stream_slice)
            request_params.update({
                "utc":self.utc_offset,
                "dimension": "location",
                "campaign_id": str(campaign['campaign_id']),
                "not_empty_field": "click,install,impression,spend"
            })
            request_params.update({"format":"json"})
            self.logger.info(f" stream slice date {stream_slice} for campaign_id {campaign['campaign_id']}")
            return request_params
    
    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        if not stream_slice:
            return []
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        for record in records:
            # record_cursor_value = pendulum.parse(record[self.cursor_field]).date()
            # self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record with ELSE, record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield response_json
        # results = response_json.get('results')
        # for record in results:
        #     yield record
    
    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
            "date": {"type": ["null", "string"]},
            "package_name": {"type": ["null", "string"]},
            "platform": {"type": ["null", "string"]},
            "location": {"type": ["null", "string"]},
            "geo": {"type": ["null", "string"]},
            "currency": {"type": ["null", "string"]},
            "utc": {"type": ["null", "string"]},
            "campaign_id": {"type": ["null", "string"]},
            "campaign_name": {"type": ["null", "string"]},
            "offer_id": {"type": ["null", "string"]},
            "offer_name": {"type": ["null", "string"]},
            "impression": {"type": ["null", "number"]},
            "click": {"type": ["null", "number"]},
            "install": {"type": ["null", "number"]},
            "spend": {"type": ["null", "number"]},
            }
        }
        return full_schema

# Source
class SourceMintegralAds(AbstractSource):
    def _get_campaign_list(self, config):
        auth = MintegralTokenAuthenticator(config=config)
        campaign_list_generator = MintegralCheckConnection(authenticator=auth,config=config)
        read_records_generator = campaign_list_generator.read_records(sync_mode="full_refresh")
        # yield from read_records_generator generate each campaign from parse response function
        yield from read_records_generator

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            check_campaign_list = self._get_campaign_list(config)
            if next(check_campaign_list) is not None:
                return True, None
        except Exception as e:
            return False,

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = MintegralTokenAuthenticator(config=config)
        campaign_list = self._get_campaign_list(config)
        streams = [MintegralAdsReport(authenticator=auth,config=config, list_campaign_as_dict= campaign_list)]
        return streams
