#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import gzip
import requests
import datetime
import pendulum

from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from source_apple_store_custom import schema
from io import StringIO
import pandas as pd
import numpy as np


# Basic full refresh stream
class AppleStoreCustomStream(HttpStream, ABC):
    url_base = "https://api.appstoreconnect.apple.com/v1/"

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

''' check connection stream'''
class AppleStoreCheckConnectionStream(AppleStoreCustomStream):
    primary_key = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "apps"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # response_json = response.json()
        response_json = response.status_code 
        ''' use when to check raw response from API'''
        yield response_json

class AppleStoreReport(AppleStoreCustomStream, IncrementalMixin):
    primary_key = None

    def __init__(self, report_name: str ,report_type: str, report_frequency: str , report_sub_type:str ,report_version: str, cursor_field: str, cursor_format: str,**kwargs):
        """override __init__ to add stream name and resolve MRO"""
        super().__init__(**kwargs)
        self.report_name = report_name
        self.report_type = report_type
        self.report_frequency = report_frequency
        self.report_sub_type = report_sub_type
        self.report_version = report_version
        self.vendor_number = self.config["vendor_id"]
        self._cursor_value = None
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone  = self.config.get("timezone", "UTC")
        self.cf = cursor_field
        self.cursor_format = cursor_format
        
    @property
    def name(self) -> str:
        """Override method to get stream name """
        stream_name = self.report_type
        return stream_name
    
    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return self.cf
    
    @property
    def state(self) -> Mapping[str, Any]:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}


    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "salesReports"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        '''Apple Store Sale Report API response back as gzip compress type, so we need to decompress then decode'''
        response_in_gzip = response.content
        response_as_bytes: bytes = gzip.decompress(response_in_gzip)
        response_as_string: str = response_as_bytes.decode('utf-8')

        '''Then, we covert response string to a file object using StringIO
        After that, we load file object to pandas frame and rename column
        Finally, we yield records by using to_dict() of pandas data frame
        '''
        # load response to pandas data frame, need to use sep variable because we have special '\t 'charater
        df = pd.read_csv(StringIO(response_as_string), sep='\\t', engine='python')
        # rename column
        df.rename(columns=lambda x: x.replace(' ','_'), inplace=True)
        df.rename(columns=lambda x: x.replace('-','_'), inplace=True)
        # replace Nan value as None
        df.replace(np.nan, None, inplace=True)
        for record in df.to_dict(orient='records'):
            yield record
        
        
        # ''' response from Apple Store contain \t and \n so we need to convert them to json'''
        # response_as_list: list = response_as_string.split('\n')
        # list_column_name = response_as_list[0].split('\t')
        # number_column = len(list_column_name)
        # result = {}
        # for number in  range(1,len(response_as_list)):
        #     k = (response_as_list[number].split('\t'))
        #     if number_column == len(k):
        #         for no in range(0, number_column):
        #             result.update({(list_column_name[no]).replace(" ", "_") : k[no]})
        #         yield result
    
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []

        # data_available_date is the date that the newest data can be accessed
        data_avaliable_date : datetime.date = pendulum.yesterday(self.timezone).date()
        
        if stream_state:
            # print(f' stream slice, stream state in IF {stream_state}, {self._cursor_value}')
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
        else:
            # print(f' stream slice, stream state in ELSE {stream_state}, {self._cursor_value}')
            start_date: datetime.date = pendulum.parse(self.config["start_date"]).date()
        
        while start_date < data_avaliable_date:
            start_date_as_str: str = start_date.to_date_string() 
            slice.append(
                {
                    'filter[reportDate]': start_date_as_str,
                }
            )
            start_date: datetime.date = start_date.add(days=1) 

        # self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        request_params.update({"filter[frequency]":self.report_frequency})
        request_params.update({"filter[reportSubType]":self.report_sub_type})
        request_params.update({"filter[reportType]":self.report_type})
        request_params.update({"filter[vendorNumber]":self.vendor_number})
        request_params.update({"filter[version]":self.report_version})
        request_params.update(stream_slice)
        self.logger.info(f"Slice in params {stream_slice}")

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
            record_cursor_value = pendulum.from_format(record[self.cursor_field], self.cursor_format).date()
            self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record with ELSE, record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record
    
    def get_json_schema(self) -> Mapping[str, Any]:
        function_name = self.report_name + "Schema"
        get_function =getattr(schema, function_name)
        full_schema =get_function()
        return full_schema
