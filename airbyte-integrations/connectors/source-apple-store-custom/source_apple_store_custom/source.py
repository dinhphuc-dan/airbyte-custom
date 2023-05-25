#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import gzip
import requests
import datetime

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from source_apple_store_custom.authenticator import AppleStoreConnectAPIAuthenticator
from source_apple_store_custom import utils


# Basic full refresh stream
class AppleStoreCustomStream(HttpStream, ABC):
    url_base = "https://api.appstoreconnect.apple.com/v1/"

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

class AppleStoreSaleReportBaseStream(AppleStoreCustomStream):
    primary_key = None

    def __init__(self, report_type: str, report_frequency: str , report_sub_type:str ,report_version: str, **kwargs):
        """override __init__ to add stream name and resolve MRO"""
        super().__init__(**kwargs)
        self.report_type = report_type
        self.report_frequency = report_frequency
        self.report_sub_type = report_sub_type
        self.report_version = report_version
        self.vendor_number = self.config["vendor_id"]
        
    @property
    def name(self) -> str:
        """Override method to get stream name """
        stream_name = self.report_type
        return stream_name

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
        response_as_list: list = response_as_string.split('\n')
        
        ''' response from Apple Store contain \t and \n so we need to convert them to json'''
        list_column_name = response_as_list[0].split('\t')
        number_column = len(list_column_name)
        result = {}
        for number in  range(1,len(response_as_list)):
            k = (response_as_list[number].split('\t'))
            if number_column == len(k):
                for no in range(0, number_column):
                    result.update({(list_column_name[no]).replace(" ", "_") : k[no]})
                yield result


# Basic incremental stream
class AppleStoreSaleReportStream(AppleStoreSaleReportBaseStream, IncrementalMixin):
    number_days_backward_default = 7
    _record_date_format = "%m/%d/%Y"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None

    
    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "Begin_Date"
    
    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            # self.logger.info(f"Cursor Getter with IF {self._cursor_value}")
            return {self.cursor_field: self._cursor_value}
        else:
            # self.logger.info(f"Cursor Getter with ELSE {self._cursor_value}")
            return {self.cursor_field: utils.string_to_date(self.config["start_date"])}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = utils.string_to_date(value[self.cursor_field]) + datetime.timedelta(days=1)
        self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        yesterday: datetime.date = datetime.date.today() - datetime.timedelta(days=1)
        number_days_backward: int = int(next(filter(None,[self.config.get('number_days_backward')]),self.number_days_backward_default))
        start_date: datetime.date = self.state[self.cursor_field] - datetime.timedelta(days=number_days_backward)
        while start_date < yesterday:
            end_date: datetime.date = start_date 
            slice.append(
                {
                    'filter[reportDate]': utils.date_to_string(start_date),
                }
            )
            start_date: datetime.date = end_date + datetime.timedelta(days=1)

        self.logger.info(f"stream slice {slice}")
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
        self.logger.info(f"Sending slice date {stream_slice['filter[reportDate]']}")

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


# Source
class SourceAppleStoreCustom(AbstractSource):

    def _get_list_sale_report_stream(self, config) -> list[Stream]:
        auth = AppleStoreConnectAPIAuthenticator(config=config)
        dict_sale_reports = {"SALES" : {"reportType":"SALES", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_0"}}
        # dict_sale_reports = {
        #     "SALES" : {"reportType":"SALES", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_0"}, 
        #     "SUBSCRIPTION": {"reportType":"SUBSCRIPTION", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_3"}, 
        #     "SUBSCRIPTION_EVENT": {"reportType":"SUBSCRIPTION_EVENT", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_3"}, 
        #     "SUBSCRIPTION_OFFER_CODE_REDEMPTION": {"reportType":"SUBSCRIPTION_OFFER_CODE_REDEMPTION", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_0"},
        #     "SUBSCRIBER":  {"reportType":"SUBSCRIBER", "reportSubType":"DETAILED", "frequency":"DAILY", "version":"1_3"},
        #     "NEWSSTAND": {"reportType":"NEWSSTAND", "reportSubType":"DETAILED", "frequency":"DAILY", "version":"1_0"},
        #     "PRE_ORDER": {"reportType":"PRE_ORDER", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_0"}
        # }

        for key,value in dict_sale_reports.items():
            yield AppleStoreSaleReportStream(
                report_type=value["reportType"],
                report_frequency=value["frequency"], 
                report_sub_type=value["reportSubType"],
                report_version=value["version"],
                authenticator=auth, 
                config=config
            )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = AppleStoreConnectAPIAuthenticator(config=config)
            logger.info(f"load auth {auth}")
            check_connection_steam = AppleStoreCheckConnectionStream(authenticator=auth, config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            logger.info(f"Happend before next fuction OMGGGG")
            record = next(check_connection_records)
            logger.info(f"There is one of records: {record}")
            return True, None
        except Exception as e:
            return False

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = []
        streams.extend(self._get_list_sale_report_stream(config=config))
        return streams
