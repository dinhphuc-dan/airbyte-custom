#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
import pendulum
import datetime
import time
from io import StringIO


class ApplovinAdsBaseStream(HttpStream, IncrementalMixin, ABC):
    primary_key = None
    _cursor_value = None
    url_base = "https://r.applovin.com/"
    chunk_date_range = 44

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.number_days_backward: int = self.config.get("number_days_backward", 7)
        self.timezone: str = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "api_key": self.config["api_key"],
            "format": "json",
            "report_type": "advertiser",
            "sort_day": "ASC",
        }
        return params
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_json = response.json()
        yield response_json
    
    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "day"

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
        data_avaliable_date: datetime.date = pendulum.today(self.timezone).date()

        if self.get_last_X_days:
            """' this code for all kind of run, such as: the first time run or full refresh or incremental run, the stream will start with today date minus number_days_backward"""
            start_date: datetime.date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        elif stream_state:
            """this code for incremental run and get_last_X_days is false, the stream will start with the last date of stream state minus number_days_backward"""
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        else:
            """' this code for the first time run or full refresh run, the stream will start with the start date in config"""
            start_date: datetime.date = pendulum.parse(self.config["start_date"]).date()
            # self.logger.info(f"stream slice start date in ELSE {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        while start_date <= data_avaliable_date:
            start_date_as_str: str = start_date.to_date_string()
            if (data_avaliable_date - start_date).days >= self.chunk_date_range:
                end_date: datetime.date = start_date.add(days=self.chunk_date_range)
                end_date_as_str: str = end_date.to_date_string()
                slice.append(
                    {
                        "start": start_date_as_str,
                        "end": end_date_as_str,
                    }
                )
            else:
                end_date: datetime.date = data_avaliable_date
                end_date_as_str: str = end_date.to_date_string()
                slice.append(
                    {   
                        "start": start_date_as_str,
                        "end": end_date_as_str,
                    }
                )
            start_date: datetime.date = end_date.add(days=1)
        
        # self.logger.info(f"slice {slice}")
        return slice or [None]

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "day": {"type": ["null", "string"]},
                "ad": {"type": ["null", "string"]},
                "ad_id": {"type": ["null", "string"]},
                "ad_creative_type": {"type": ["null", "string"]},
                "campaign": {"type": ["null", "string"]},
                "campaign_ad_type": {"type": ["null", "string"]},
                "campaign_bid_goal": {"type": ["null", "string"]},
                "campaign_package_name": {"type": ["null", "string"]},
                "campaign_store_id": {"type": ["null", "string"]},
                "campaign_type": {"type": ["null", "string"]},
                "clicks": {"type": ["null", "number"]},
                "conversions": {"type": ["null", "number"]},
                "cost": {"type": ["null", "number"]},
                "creative_set": {"type": ["null", "string"]},
                "creative_set_id": {"type": ["null", "string"]},
                "impressions": {"type": ["null", "number"]},
                "placement_type": {"type": ["null", "string"]},
                "platform": {"type": ["null", "string"]},
            },
        }
        return full_schema

class ApplovinAdsCheckConnection(ApplovinAdsBaseStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/report"
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        additional_params = {
            "start": pendulum.today(self.timezone).subtract(days=1).date().to_date_string(),
            "end": pendulum.today(self.timezone).subtract(days=1).date().to_date_string(),
            "columns": "day,campaign",
        }
        params.update(additional_params)
        return params

class ApplovinAdsReport(ApplovinAdsBaseStream):

    @property
    def name(self) -> str:
        stream_name = "Applovin_Ads_Report"
        return stream_name
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/report"
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)

        additional_params = {
            "start": stream_slice["start"],
            "end": stream_slice["end"],
            "columns": "day,ad,ad_id,ad_creative_type,campaign,campaign_ad_type,campaign_bid_goal,campaign_package_name,campaign_store_id,campaign_type,clicks,conversions,cost,creative_set,creative_set_id,impressions,placement_type,platform",
            
        }
        params.update(additional_params)
        self.logger.info(f"Params {stream_slice['start']} - {stream_slice['end']}")
        return params
    
    
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
            record_cursor_value: datetime.date = pendulum.parse(record[self.cursor_field]).date()
            self._cursor_value: datetime.date = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            yield record
        
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_json = response.json()
        results = response_json.get("results")
        for record in results:
            yield record


# Source
class SourceApplovinAds(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try: 
            check_connection_steam = ApplovinAdsCheckConnection(config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {next(check_connection_records)}")
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = [ApplovinAdsReport(config=config) ]
        return streams