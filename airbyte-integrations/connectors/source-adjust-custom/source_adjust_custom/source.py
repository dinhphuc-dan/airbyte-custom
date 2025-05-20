#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import (
    TokenAuthenticator, 
    MultipleTokenAuthenticator, 
    BasicHttpAuthenticator, 
    Oauth2Authenticator, 
    SingleUseRefreshTokenOauth2Authenticator
)

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
import pendulum
import datetime
import time
from io import StringIO


class AdjustCustomBaseStream(HttpStream, IncrementalMixin, ABC):
    primary_key = None
    _cursor_value = None

    url_base = "https://automate.adjust.com/reports-service/"
    _chunk_date_range = 7
    _custom_backoff = False
    _raise_on_http_errors = True

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.number_days_backward: int = self.config.get("number_days_backward", 7)
        self.timezone: str = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
        self.chunk_date_range = self.config.get("chunk_date_range", self._chunk_date_range)

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None
    
    @property
    def name(self) -> str:
        stream_name = "AdjustCustom"
        return stream_name

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # self.logger.info(f"Status code in Parse Response {response.status_code}")
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

        return slice or [None]

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                # "campaign_id": {"type": ["null", "number"]},
                # "campaign_name": {"type": ["null", "string"]},
            },
        }
        return full_schema

class AdjustCustomCheckConnection(AdjustCustomBaseStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "report"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "date_period": "yesterday",
            "utc_offset": ("+0" + str(self.config["utc_offset"]) + ":00") if self.config["utc_offset"] >= 0 else ("-0" + str(abs(self.config["utc_offset"])) + ":00"),
            "format_dates": True,
            "dimensions": "day",
            "metrics": "cost,installs,ad_revenue",
        }
        self.logger.info(f"Request params {params}")
        return params 
		

class AdjustCustomDailyReportStream(AdjustCustomBaseStream):

    dimensions_default = [
        "day", 
        "app", 
        "store_id"
    ]

    metrics_default = [
        "cost", 
        "installs", 
        "ad_revenue"
    ]

    @property
    def name(self) -> str:
        stream_name = "Daily_Report"
        return stream_name
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "report"
    
    @property
    def _finalized_dimensions(self) -> List[str]:
        if self.config.get("custom_report_dimensions"):
            dimensions = [item.strip() for item in self.config["custom_report_dimensions"].split(",")] + self.dimensions_default
            dimensions = list(set(dimensions))  # remove duplicates
        else:
            dimensions = self.dimensions_default
        return dimensions
    
    @property
    def _finalized_metrics(self) -> List[str]:
        if self.config.get("custom_report_metrics"):
            metrics = [item.strip() for item in self.config["custom_report_metrics"].split(",")] + self.metrics_default
            metrics = list(set(metrics)) # remove duplicates
        else:
            metrics = self.metrics_default
        return metrics
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        
        date_period = stream_slice["start"] + ":" + stream_slice["end"]

        dimensions: str = ",".join(self._finalized_dimensions)

        metrics: str = ",".join(self._finalized_metrics)

        params = {
            "date_period": date_period,
            "utc_offset": ("+0" + str(self.config["utc_offset"]) + ":00") if self.config["utc_offset"] >= 0 else ("-0" + str(abs(self.config["utc_offset"])) + ":00"),
            "format_dates": True,
            "sort":"day",
            "dimensions": dimensions,
            "metrics": metrics,
        }

        self.logger.info(f"Request params {params}")
        return params 
    
    
    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)

        for record in records:
            record_cursor_value: datetime.date = pendulum.parse(record[self.cursor_field]).date()
            self._cursor_value: datetime.date = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            yield record
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        data_warnings = response_json["data_warnings"]
        if response_json.get("rows"):
            for record in response_json["rows"]:
                record.update({"data_warnings": data_warnings})
                yield record
        else:
            yield {}
    
    def get_json_schema(self) -> Mapping[str, Any]:
        properties = {
            "data_warnings": {"type": ["null", "string"]},
        }

        for dimension in self._finalized_dimensions:
            properties.update(
                {dimension: {"type": ["null", "string"]}}
            )

        for metric in self._finalized_metrics:
            properties.update(
                {metric: {"type": ["null", "number"]}}
            )
            
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": properties,
        }
        return full_schema


# Source
class SourceAdjustCustom(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try: 
            auth = TokenAuthenticator(token=config["api_token"])
            # logger.info(f"load auth {auth}")
            check_connection_steam = AdjustCustomCheckConnection(authenticator = auth, config=config) 
            # logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            record = next(check_connection_records)
            logger.info(f"Successfully check token, status code {record}")
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_token"])
        streams = [AdjustCustomDailyReportStream(authenticator=auth, config=config)]
        return streams