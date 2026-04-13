#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
import datetime
import pendulum
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
import pandas as pd
import numpy as np


""" Base Stream """


class ApplovinMaxStream(HttpStream, ABC):
    url_base = "https://r.applovin.com/"

    """ 
    Override HttpSteam contructor method, add config params to read config value from config setup by users
    **kwargs for authentication part of HttStream --> go to check connection method
    """

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
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
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None


""" Check connection Stream"""


class ApplovinMaxCheckConnection(ApplovinMaxStream):
    primary_key = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "maxReport"
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        today: datetime.date = datetime.date.today()
        start_date: datetime.date = today - datetime.timedelta(days=45)
        request_params.update({"start": start_date})
        request_params.update({"end": today})
        request_params.update({"api_key": self.config["api_key"]})
        request_params.update({"format": "json"})
        request_params.update({"columns": "package_name,platform"})
        return request_params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        list_package_name = []
        # for record in response_json["results"]:
        #     for key, package_name_value in record.items():
        #         list_package_name.append(package_name_value)
        # yield list_package_name

        for record in response_json["results"]:
            list_package_name.append(list(record.values()))
        yield list_package_name


        """ use when to check raw response from API"""
        # yield response_json


""" Incremental Stream """


class ApplovinMaxFullReport(ApplovinMaxStream, IncrementalMixin):
    primary_key = None
    daily_columns = "day,package_name,platform,application,ad_format,ad_unit_waterfall_name,country,custom_network_name,device_type,has_idfa,max_ad_unit,max_ad_unit_id,max_ad_unit_test,network,network_placement,attempts,responses,impressions,estimated_revenue"
    hourly_columns = "hour,day,package_name,platform,application,ad_format,ad_unit_waterfall_name,country,custom_network_name,device_type,has_idfa,max_ad_unit,max_ad_unit_id,max_ad_unit_test,network,network_placement,attempts,responses,impressions,estimated_revenue"

    def __init__(self, list_package_name, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
        self.get_hourly_data = self.config.get("get_hourly_data", False)
        self.list_package_name = list_package_name
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "maxReport"

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name"""
        stream_name = "Full_Report"
        return stream_name

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
            if start_date.month == data_avaliable_date.month:
                end_date_as_str: str = data_avaliable_date.to_date_string()
                for package_name in self.list_package_name:
                    slice.append({"start": start_date_as_str, "end": end_date_as_str, "filter_package_name": package_name})
            else:
                end_date_as_str: str = start_date.end_of("month").to_date_string()
                for package_name in self.list_package_name:
                    slice.append({"start": start_date_as_str, "end": end_date_as_str, "filter_package_name": package_name})
            start_date: datetime.date = start_date.add(months=1).start_of("month")

        # self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        request_params.update(stream_slice)
        request_params.update({"api_key": self.config["api_key"]})
        request_params.update({"format": "json"})
        if not self.get_hourly_data:
            request_params.update({"columns": self.daily_columns})
        else:
            request_params.update({"columns": self.hourly_columns})
        self.logger.info(f" stream slice date {stream_slice}")
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
            record_cursor_value = pendulum.parse(record[self.cursor_field]).date()
            self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record with ELSE, record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_json = response.json()
        results = response_json.get("results")
        for record in results:
            yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "day": {"type": ["null", "string"]},
                "hour": {"type": ["null", "string"]},
                "package_name": {"type": ["null", "string"]},
                "platform": {"type": ["null", "string"]},
                "application": {"type": ["null", "string"]},
                "ad_format": {"type": ["null", "string"]},
                "ad_unit_waterfall_name": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "custom_network_name": {"type": ["null", "string"]},
                "device_type": {"type": ["null", "string"]},
                "has_idfa": {"type": ["null", "string"]},
                "max_ad_unit": {"type": ["null", "string"]},
                "max_ad_unit_id": {"type": ["null", "string"]},
                "max_ad_unit_test": {"type": ["null", "string"]},
                "network": {"type": ["null", "string"]},
                "network_placement": {"type": ["null", "string"]},
                "attempts": {"type": ["null", "number"]},
                "responses": {"type": ["null", "number"]},
                "impressions": {"type": ["null", "number"]},
                "estimated_revenue": {"type": ["null", "number"]},
            },
        }
        return full_schema


""" Realtime Stream """


class ApplovinMaxCustomReport(ApplovinMaxFullReport):

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name"""
        stream_name = "Custom_Report"
        return stream_name

    def get_dimensions(self) -> list:
        required_dimensions = ["day", "package_name", "platform", "application"]
        if self.config.get("custom_report_dimensions"):
            return required_dimensions + self.config.get("custom_report_dimensions")
        else:
            return required_dimensions

    def get_metrics(self) -> list:
        if self.config.get("custom_report_metrics"):
            return self.config.get("custom_report_metrics")

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        request_params.update(stream_slice)
        request_params.update({"api_key": self.config["api_key"]})
        request_params.update({"format": "json"})
        request_params.update({"sort_day": "ASC"})
        if not self.get_hourly_data:
            list_of_dimensions_and_metrics = self.get_dimensions() + self.get_metrics()
        else:
            list_of_dimensions_and_metrics = self.get_dimensions() + self.get_metrics() + ["hour"]
        colums = ",".join(list_of_dimensions_and_metrics)
        request_params.update({"columns": colums})
        self.logger.info(f" stream slice date {stream_slice}")
        return request_params

    def get_json_schema(self) -> Mapping[str, Any]:
        dynamic_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": {
                "day": {"type": ["null", "string"]},
                "hour": {"type": ["null", "string"]},
                "package_name": {"type": ["null", "string"]},
                "platform": {"type": ["null", "string"]},
                "application": {"type": ["null", "string"]},
            },
        }
        dynamic_schema["properties"].update({d: {"type": ["null", "string"]} for d in self.get_dimensions()})
        dynamic_schema["properties"].update({m: {"type": ["null", "number"]} for m in self.get_metrics()})
        return dynamic_schema

""" User-Level Ad Impression Report"""
class ApplovinMaxUserLevelAdImpressionReport(ApplovinMaxStream, IncrementalMixin):
    primary_key = None

    def __init__(self, list_package_name_n_platform, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
        self.get_hourly_data = self.config.get("get_hourly_data", False)
        self.list_package_name_n_platform: list[list[str]] = list_package_name_n_platform
        self._raise_on_http_errors = True


    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "max/userAdRevenueReport"

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name"""
        stream_name = "User_Level_Ad_Impression_Report"
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
            for app in self.list_package_name_n_platform:
                slice.append({"date": start_date_as_str, "store_id": app[0], "platform": app[1]})
            start_date: datetime.date = start_date.add(days=1)

        # self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        request_params.update(stream_slice)
        request_params.update({"api_key": self.config["api_key"]})
        request_params.update({"aggregated": False})
        # request_params.update({"platform": "android"})
        self.logger.info(f" stream slice date {stream_slice}")
        return request_params

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        for record in records:
            record_cursor_value = pendulum.parse(record[self.cursor_field]).date()
            self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record with ELSE, record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Applovin return error 404, when there is no reports. 
        We turn it into a record as {} instead of stopping sync
        """
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        if response.status_code == 404:
            return {}
        else:
            response_json = response.json()
            # response contain a s3 csv link, so we need to get the link then load it into pandas csv
            # if report does not have Meta Bidding data, it show only url key-value
            ad_revenue_report_url = response_json.get("ad_revenue_report_url") or response_json.get("url")

            # we use chunk here to make sure if the file is too large, our RAM dont crash
            for chunk in pd.read_csv(ad_revenue_report_url, chunksize=100000):
                chunk.rename(columns=lambda x: x.replace(" ", "_").lower(), inplace=True)
                chunk.replace([np.nan, np.inf, -np.inf], None, inplace=True)
                for record in chunk.to_dict(orient="records"):
                    yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "ad_unit_id": {"type": ["null", "string"]},
                "ad_unit_name": {"type": ["null", "string"]},
                "waterfall": {"type": ["null", "string"]},
                "ad_format": {"type": ["null", "string"]},
                "placement": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "device_type": {"type": ["null", "string"]},
                "idfa": {"type": ["null", "string"]},
                "idfv": {"type": ["null", "string"]},
                "user_id": {"type": ["null", "string"]},
                "network": {"type": ["null", "string"]},
                "ad_placement": {"type": ["null", "string"]},
                "custom_data": {"type": ["null", "string"]},
                "revenue": {"type": ["null", "number"]}
            },
        }
        return full_schema
    
    @property
    def raise_on_http_errors(self) -> bool:
        """
        Default is True. If set to False, allows opting-out of raising HTTP code exception.
        """
        return self._raise_on_http_errors

    @raise_on_http_errors.setter
    def raise_on_http_errors(self, value: bool):
        self._raise_on_http_errors = value
    
    def should_retry(self, response: requests.Response) -> bool:
        """
        Applovin return error 404, when there is no reports. 
        We turn it into a record as {} instead of stopping sync
        """
        if response.status_code == 404:
            setattr(self, "raise_on_http_errors", False)
            return False
        return super().should_retry(response=response)

# Source
class SourceApplovinMax(AbstractSource):
    def _get_list_package_name(self, config):
        check_connection_steam = ApplovinMaxCheckConnection(config=config)
        check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
        list_package_name_n_platform: list[list[str]] = next(check_connection_records)
        list_package_name = list(set(app[0] for app in list_package_name_n_platform))
        return list_package_name, list_package_name_n_platform

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            list_package_name, list_package_name_n_platform = self._get_list_package_name(config)
            logger.info(f"List package and platform: {list_package_name_n_platform}")
            logger.info(f"List package: {list_package_name}")
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = []
        list_package_name, list_package_name_n_platform = self._get_list_package_name(config)
        streams.append(ApplovinMaxFullReport(config=config, list_package_name=list_package_name))
        streams.append(ApplovinMaxUserLevelAdImpressionReport(config=config, list_package_name_n_platform=list_package_name_n_platform))

        if config.get("custom_report_metrics"):
            streams.append(ApplovinMaxCustomReport(config=config, list_package_name=list_package_name))

        return streams
