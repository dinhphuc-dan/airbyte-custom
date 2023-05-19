#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth
from airbyte_cdk.models import SyncMode
from source_applovin_max import utils


''' Base Stream '''
class ApplovinMaxStream(HttpStream, ABC):
    url_base = "https://r.applovin.com/maxReport"

    """ 
    Override HttpSteam contructor method, add config params to read config value from config setup by users
    **kwargs for authentication part of HttStream --> go to check connection method
    """
    def __init__(self,config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}

''' Check connection Stream'''
class ApplovinMaxCheckConnection(ApplovinMaxStream):
    primary_key = None

    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        today: datetime.date = datetime.date.today()
        start_date: datetime.date = today - datetime.timedelta(days=45)
        request_params.update({"start":start_date})
        request_params.update({"end":today})
        request_params.update({"api_key":self.config["api_key"]})
        request_params.update({"format":"json"})
        request_params.update({"columns":"package_name"})
        return request_params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        list_package_name = []
        for record in response_json['results']:
            for key, package_name_value in record.items():
                list_package_name.append(package_name_value)
        return list_package_name

        ''' use when to check raw response from API'''
        # yield response_json

''' Base stream'''
class ApplovinMaxReportBase(ApplovinMaxStream):
    primary_key = None

    def __init__(self, package_name: str, **kwargs):
        """override __init__ to add package name"""
        super().__init__(**kwargs)
        self.package_name = package_name
        self._cursor_value = None
    
    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name """
        prefix = "FullReport_"
        stream_name = prefix + self.package_name
        return stream_name

    def path(self, **kwargs) -> str:
        return None

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
            "day": {"type": ["null", "string"]},
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
            }
        }
        return full_schema

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        results = response_json.get('results')
        for record in results:
            yield record


''' Incremental Stream '''
class ApplovinMaxFullReport(ApplovinMaxReportBase,IncrementalMixin):
    number_days_backward_default = 7
    _record_date_format = "%Y-%m-%d"


    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
    
    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "day"
    
    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            self.logger.info(f"Cursor Getter with IF {self._cursor_value}")
            return {self.cursor_field: self._cursor_value}
        else:
            self.logger.info(f"Cursor Getter with ELSE {self._cursor_value}")
            return {self.cursor_field: utils.string_to_date(self.config["start_date"])}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = utils.string_to_date(value[self.cursor_field]) + datetime.timedelta(days=1)
        self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        today: datetime.date = datetime.date.today()
        number_days_backward: int = int(next(filter(None,[self.config.get('number_days_backward')]),self.number_days_backward_default))
        start_date: datetime.date = self.state[self.cursor_field] - datetime.timedelta(days=number_days_backward)

        while start_date < today:
            end_date: datetime.date = start_date 
            slice.append(
                {
                    'start': utils.date_to_string(start_date),
                    'end': utils.date_to_string(end_date),
                }
            )
            start_date: datetime.date = end_date + datetime.timedelta(days=1)

        self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        request_params.update(stream_slice)
        request_params.update({"api_key":self.config["api_key"]})
        request_params.update({"format":"json"})
        request_params.update({"filter_package_name":self.package_name})
        # request_params.update({"columns":"day,package_name,application,estimated_revenue"})
        request_params.update({"columns":"day,package_name,platform,application,ad_format,ad_unit_waterfall_name,country,custom_network_name,device_type,has_idfa,max_ad_unit,max_ad_unit_id,max_ad_unit_test,network,network_placement,attempts,responses,impressions,estimated_revenue"})
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
            record_cursor_value = utils.string_to_date(record[self.cursor_field], self._record_date_format)
            self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record with ELSE, record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record

''' Realtime Stream '''
class ApplovinMaxCustomReport(ApplovinMaxFullReport):

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name """
        prefix = "CustomReport_"
        stream_name = prefix + self.package_name
        return stream_name
    
    def get_dimensions(self) ->list:
        required_dimensions = ['day','package_name','platform', 'application']
        if self.config.get("custom_report_dimensions"):
            return required_dimensions + self.config.get("custom_report_dimensions") 
        else:
            return required_dimensions 
    
    def get_metrics(self) ->list:
        if self.config.get("custom_report_metrics"):
            return self.config.get("custom_report_metrics")

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        today: datetime.date = datetime.date.today()
        start_date: datetime.date = self.state[self.cursor_field]

        slice.append(
            {
                'start': utils.date_to_string(start_date),
                'end': utils.date_to_string(today),
            }
        )

        self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        request_params.update(stream_slice)
        request_params.update({"api_key":self.config["api_key"]})
        request_params.update({"format":"json"})
        request_params.update({"filter_package_name":self.package_name})
        # request_params.update({"columns":"day,package_name,application,estimated_revenue"})
        list_of_dimensions_and_metrics = self.get_dimensions() + self.get_metrics()
        colums = ','.join(list_of_dimensions_and_metrics)
        request_params.update({"columns":colums})
        return request_params

    def get_json_schema(self) -> Mapping[str, Any]:
        dynamic_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": {
            "day": {"type": ["null", "string"]},
            "package_name": {"type": ["null", "string"]},
            "platform": {"type": ["null", "string"]},
            "application": {"type": ["null", "string"]},
            }
        }
        dynamic_schema["properties"].update({d: {"type": ["null", "string"]} for d in self.get_dimensions()})
        dynamic_schema["properties"].update({m: {"type": ["null", "number"]} for m in self.get_metrics()})
        return dynamic_schema
    

    

# Source
class SourceApplovinMax(AbstractSource):

    def _get_package_name(self,config) -> list:
        list_package_name = []
        list_package_stream = ApplovinMaxCheckConnection(config=config) 
        list_package_record: list = list_package_stream.read_records(sync_mode="full_refresh")
        list_package_name.extend(list_package_record)
        return list_package_name

    def _generate_applovin_max_full_report_stream(self, config) -> list[Stream]:
        list_package_name = self._get_package_name(config=config)
        for package_name in list_package_name:
            yield ApplovinMaxFullReport(
                package_name=package_name,
                config=config
            )
    
    def _generate_applovin_max_custom_report_stream(self, config) -> list[Stream]:
        list_package_name = self._get_package_name(config=config)
        for package_name in list_package_name:
            yield ApplovinMaxCustomReport(
                package_name=package_name,
                config=config
            )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            logger.info(f"load API key {config.get('api_key')}")
            logger.info(f"load Package {config.get('package_name')}")
            logger.info(f"load start_date {config.get('start_date')}")
            check_connection_steam = ApplovinMaxCheckConnection(config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            record = next(check_connection_records)
            logger.info(f"There is one of records: {record}")
            return True, None
        except Exception as e:
            return False,

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = []

        applovin_max_report_stream = self._generate_applovin_max_full_report_stream(config=config)
        streams.extend(applovin_max_report_stream)

        if  config.get("custom_report_metrics"):
            custom_streams = self._generate_applovin_max_custom_report_stream(config=config)
            streams.extend(custom_streams)

        return streams
