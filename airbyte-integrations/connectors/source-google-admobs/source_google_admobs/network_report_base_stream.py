#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import uuid
import datetime
import pendulum
import re
from abc import ABC
import string
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from source_google_admobs import utils
from source_google_admobs import schemas

token_refresh_endpoint = "https://oauth2.googleapis.com/token"

# Basic stream
class GoogleAdmobsStream(HttpStream, ABC):
    url_base = "https://admob.googleapis.com/v1beta/accounts/"

    """ 
    Override HttpSteam contructor method, add config params to read publisher_id  from config setup by users --> go to path method in each stream
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

class AListApps(GoogleAdmobsStream):
    """full refresh, no incremental, get ID of all apps"""
    primary_key = None

    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        #Here is how the config params get the publisher id from user's config
        publisher_id = self.config["publisher_id"]
        path = f"{publisher_id}/apps"
        return path
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """ if have more than 20,000 apps, modify this part with pageSize parameters"""
        return {}
    
    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Override get_json_schema CDK method to retrieve the schema information for GoogleAnalyticsV4 Object dynamically.
        """
        schema: dict[str, Any] = schemas.list_apps_schema()

        return schema
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """ The items API returns records inside a list dict with name as "name", each contain an appID """
        list_app_dict = {}
        response_json = response.json()
        # response_json = response.status_code 

        """use to check the orgirinal form of reponse from API when use check fuction """
        # yield response_json
        for name in response_json.get('apps'):
            for key, value in name.items():
                if "appId" in key:
                    app_id_full = value
                    app_id_trim = re.search(r'(~)[0-9]+',value).group()
                if "linkedAppInfo" in key:
                    display_name = name.get('linkedAppInfo').get("displayName")
                    display_name_cap = string.capwords(display_name)
                    """remove all special character in display name to make app_name in camel case"""
                    app_name = re.sub(r'\W+', '',display_name_cap)
                    list_app_dict.update({"app_name": app_name, "app_id_full": app_id_full, "app_id": app_id_trim})
            yield list_app_dict

class NetworkReportBase(GoogleAdmobsStream):
    """
    Base class for incremental stream and custom network report stream
    Due to Admobs API limit 100k row reponse per request, and it also does not support pagination, so I create dynamic stream for each app
    First, I call list app API to get app_name and app_id. Then convert to name of each stream
    Uuid is added so that later on I can deduplicate records in data warehouse
    """
    primary_key = "uuid"

    def __init__(self, app_name: str = None, app_id: str = None, **kwargs):
        """override __init__ to add app_name and app_id"""
        super().__init__(**kwargs)
        self.app_name = app_name
        self.app_id = app_id

    @property
    def name(self) -> str:
        """Override method to get stream name according to each app name with prefix NP (network report) """
        prefix = "NP"
        stream_name = prefix + self.app_id
        return stream_name

    @property
    def http_method(self) -> str:
       """ Override because using POST. Default by airbyte is GET """
       return "POST"

    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """Here is how I get the publisher id from user's config and add it to the path to call API"""
        publisher_id = self.config["publisher_id"]
        path = f"{publisher_id}/networkReport:generate"
        return path
    
    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Override get_json_schema CDK method to retrieve the schema of network report
        """
        schema: dict[str, Any] = schemas.network_report_schema()

        return schema
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """ The items API returns records inside a list dict with the first item is header, the next to before last item is row, and the last item is footer """
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_json = response.json()

        """ use to check the orgirinal form of reponse from API when use check fuction """
        # yield response_json 
        """ use to check the number of record when use read fuction """
        # return response_json 

        """ use to check return row item only, exclude header and footer item """
        # for reponse_item in response_json:
        #     if 'row' in reponse_item:
        #         yield reponse_item

        """ turn a row item to a dict without keyword row, then add uuid and also transform the format of API response"""
        row = (response_item.get('row') for response_item in response_json if 'row' in response_item)
        for a_dict in row:
            result = {"uuid": str(uuid.uuid4())}
            if 'dimensionValues' in a_dict:
                for key, value in a_dict.get('dimensionValues').items():
                    if 'value' and 'displayLabel' in value.keys():
                        result.update({key: value.get('value')})
                        result.update({key + '_NAME': value.get('displayLabel')})
                    else:
                        result.update({key: value.get('value','null')})
            if 'metricValues' in a_dict:
                for key, value in a_dict.get('metricValues').items():
                    """  
                    filter(None, it) removes all Falsy values such as [], {}, 0, False, set(), '', None, etc.
                    filter() return a generator so need use next() to get item
                    """
                    result.update({key: next(filter(None, [value.get('microsValue'), value.get('integerValue'),value.get('doubleValue')]),0)})
            yield result

class NetworkReport(NetworkReportBase,IncrementalMixin):
    """Incremental stream for network report api. Just adding some new function to get incremental"""

    def __init__(self, *args, **kwargs):
        """Due to multiple inheritance, so need MRO"""
        super(NetworkReport, self).__init__(*args, **kwargs)
        self._cursor_value = None
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone  = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
  
    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "DATE"
    
    @property
    def state(self) -> Mapping[str, Any]:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        # self.logger.info(f"Cursor Setter BEFORE {value}")
        ''' 
        in case the first run does not return any record, the second incremental run will get value of cursor_field of stream state as null
        which in turn will cause error. Thus, we add if value of cursor_field is None then getting the config start_date instead
        '''
        if value[self.cursor_field] is None:
            self._cursor_value = pendulum.parse(self.config["start_date"]).date()
        else:
            self._cursor_value = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []

        # data_available_date is the date that the newest data can be accessed
        data_avaliable_date : datetime.date = pendulum.today(self.timezone).date()

        if self.get_last_X_days:
            '''' this code for all kind of run, such as: the first time run or full refresh or incremental run, the stream will start with today date minus number_days_backward'''
            start_date: datetime.date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        elif stream_state:
            ''' this code for incremental run and get_last_X_days is false, the stream will start with the last date of stream state minus number_days_backward'''
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        else: 
            '''' this code for the first time run or full refresh run, the stream will start with the start date in config'''
            start_date: datetime.date = pendulum.parse(self.config["start_date"]).date()
            # self.logger.info(f"stream slice start date in ELSE {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        while start_date <= data_avaliable_date:
            end_date: datetime.date = start_date 
            slice.append(
                {
                    'startDate': utils.turn_date_to_dict(start_date),
                    'endDate': utils.turn_date_to_dict(end_date),
                }
            )
            start_date: datetime.date = end_date + datetime.timedelta(days=1)

        # self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        """ 
        POST method needs a body json. 
        Json body according to Google Admobs API: https://developers.google.com/admob/api/v1/reference/rest/v1/accounts.networkReport/generate 
        """
        date_range = stream_slice

        app_id = self.app_id

        dimensions = ['DATE','AD_UNIT','APP','COUNTRY','FORMAT','PLATFORM','APP_VERSION_NAME']

        metrics = ['AD_REQUESTS','MATCHED_REQUESTS','CLICKS','ESTIMATED_EARNINGS','IMPRESSIONS']

        sort_conditions = [{'dimension': 'DATE', 'order': 'DESCENDING'}]

        dimension_filters = {'dimension': 'APP','matches_any': {'values': app_id}}

        report_spec = {
            'dateRange': date_range,
            'dimensions': dimensions,
            'metrics': metrics,
            'sortConditions': sort_conditions,
            'dimensionFilters': dimension_filters
        }

        body_json = {"reportSpec": report_spec}
        self.logger.info(f"stream slice date {stream_slice['startDate']} - {stream_slice['endDate']}")
        return body_json
    
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



