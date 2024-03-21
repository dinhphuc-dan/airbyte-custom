#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from source_iron_source_advertise.authenticator import IronSourceAdvertiseAuthenticator
import pendulum
import datetime
import re

# Base stream
class IronSourceAdvertiseBaseStream(HttpStream, IncrementalMixin, ABC):
    url_base = "https://api.ironsrc.com/advertisers/"
    _cursor_value = None
    primary_key = None

    def __init__(self, config ,*args ,**kwargs):
        super().__init__(*args ,**kwargs)
        self.config = config
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone  = self.config.get("timezone", "UTC")
    
    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None
    
    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None
    
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

    '''
    For this api, if reponse request are paginated, the response json will contain a next link in a `paging` key, in that link contains a param's name  as cursor
    We will use cursor value to get the next page
    Example: {'data': [...], 'paging': {'next': '...&cursor=XXXXXXXXXXXX=='}}
    '''
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response = response.json()
        if response.get("paging", None):
            next_link = response["paging"]["next"]
            cursor_param = re.search(r'cursor=[a-zA-Z0-9]+',next_link)
            cursor_param_value = cursor_param.group().split('=')[1]
            return cursor_param_value
        else:
            return None

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice: list = []
        
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

        ''' 
        this loop is for checking if start_date and data_avaliable_date are in same month
        if not, then create at least 2 slices in which start_date -> end_of_month and end_of_month -> data_avaliable_date
        else, create 1 slice in which start_date -> data_avaliable_date
        '''
        while start_date <= data_avaliable_date:
            start_date_as_str: str = start_date.to_date_string()
            if start_date.month == data_avaliable_date.month:
                end_date_as_str: str = data_avaliable_date.to_date_string()
                slice.append({
                    "startDate": start_date_as_str,
                    "endDate": end_date_as_str
                    }
                )
            else:
                end_date_as_str: str = start_date.end_of('month').to_date_string()
                slice.append({
                    "startDate": start_date_as_str,
                    "endDate": end_date_as_str
                    }
                )
            start_date: datetime.date = start_date.add(months=1).start_of('month')

        self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    ''' this API response json always has same format as {'data': [...], 'paging': {'next': '...&cursor=XXXXXXXXXXXX=='}}'''
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        for row in response_json['data']:
            yield row

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        if stream_slice:
            for record in records:
                record_cursor_value: datetime.date = pendulum.parse(record[self.cursor_field]).date()
                self._cursor_value: datetime.date = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
                # self.logger.info(f"read record; record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
                yield record
        else:
            yield from records

class IronSourceAdvertiseCheckConnnectionStream(IronSourceAdvertiseBaseStream):
    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v4/reports/cost"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        start_date :str = pendulum.today(self.timezone).subtract(days=1).to_date_string()
        end_date :str = pendulum.today(self.timezone).to_date_string()
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "breakdowns": "day",
            "metrics": ['installs','billable_spend'],
            "format": "json",
            "count": 1,
        }
        return params
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield response_json

class IronSourceAdvertiseCostAPI(IronSourceAdvertiseBaseStream):
    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v4/reports/cost"

    @property
    def name(self) -> str:
        """Override method to get stream name """
        stream_name = 'Cost_API'
        return stream_name

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "campaignId": {"type": ["null", "number"]},
                "campaignName": {"type": ["null", "string"]},
                "titleName": {"type": ["null", "string"]},
                "titleBundleId": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "os": {"type": ["null", "string"]},
                'impressions': {"type": ["null", "number"]},
                'clicks': {"type": ["null", "number"]},
                'installs': {"type": ["null", "number"]},
                "billableSpend": {"type": ["null", "number"]},
            }
        }
        return full_schema

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        param = {
            "breakdowns": ["day",'campaign','title','os','country'],
            "metrics": ['impressions','clicks','installs','billable_spend'],
            "format": "json",
            "count": 250000,
            "order":"day",
            "direction":"asc"
        }

        # we pass cursor param value into cursor param
        if next_page_token:
            param['cursor'] = next_page_token

        self.logger.info(f"Slice in params {stream_slice}")
        param.update(stream_slice)
        return param


class IronSourceAdvertiseReportingAPI(IronSourceAdvertiseBaseStream):
    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v2/reports"

    @property
    def name(self) -> str:
        """Override method to get stream name """
        stream_name = 'Reporting_API'
        return stream_name

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "campaignId": {"type": ["null", "number"]},
                "campaignName": {"type": ["null", "string"]},
                "titleName": {"type": ["null", "string"]},
                "titleBundleId": {"type": ["null", "string"]},
                "applicationId": {"type": ["null", "number"]},
                "creativeId": {"type": ["null", "number"]},
                "creativeName": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "deviceType": {"type": ["null", "string"]},
                "os": {"type": ["null", "string"]},
                "adUnitName": {"type": ["null", "string"]},
                "impressions": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]},
                "completions": {"type": ["null", "number"]},
                "installs": {"type": ["null", "number"]},
                "spend": {"type": ["null", "number"]},
                }
        }
        return full_schema

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        param = {
            "breakdowns": ["day",'campaign','title','application','os','country','deviceType', 'creative', 'adUnit'],
            "metrics": ['impressions','clicks','installs','completions','spend'],
            "format": "json",
            "count": 250000,
            "order":"day",
            "direction":"asc"
        }

        # we pass cursor param value into cursor param
        if next_page_token:
            param['cursor'] = next_page_token

        self.logger.info(f"Slice in params {stream_slice}")
        param.update(stream_slice)
        return param

# Source
class SourceIronSourceAdvertise(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = IronSourceAdvertiseAuthenticator(config=config)
            logger.info(f"load auth {auth}")
            check_connection_steam = IronSourceAdvertiseCheckConnnectionStream(authenticator = auth, config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            record = next(check_connection_records)
            logger.info(f"There is one of records: {record}")
            return True, None
        except Exception as e:
            return False

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = IronSourceAdvertiseAuthenticator(config=config)
        stream_1 = IronSourceAdvertiseCostAPI(authenticator = auth, config=config)
        stream_2 = IronSourceAdvertiseReportingAPI(authenticator = auth, config=config)
        return [stream_1, stream_2]
