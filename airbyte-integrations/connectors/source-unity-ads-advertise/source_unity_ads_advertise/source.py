#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import BasicHttpAuthenticator
from airbyte_cdk.models import SyncMode
import pendulum
import datetime
from io import StringIO
import csv
import pandas as pd
import numpy as np
import base64


# Basic full refresh stream
class UnityAdsAdvertiseStream(HttpStream, ABC):
    url_base = "https://services.api.unity.com/advertise/stats/v2/organizations/"
    primary_key = None

    def __init__(self,config:Mapping[str,any] ,*arg,**kwargs):
        super().__init__(*arg, **kwargs)
        self.config = config
    
    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    @property
    def http_method(self) -> str:
        """
        Override if needed. See get_request_data/get_request_json if using POST/PUT/PATCH.
        """
        return "GET"

    @property
    def name(self) -> str:
        """
        :return: Stream name. By default this is the implementing class name, but it can be overridden as needed.
        because this is a @property decorator so we can't use super().name()
        """
        return super().name

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        organization_id = self.config["organization_id"]
        return f"{organization_id}/reports/acquisitions"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


class UnityAdsAdvertiseCheckConnectionStream(UnityAdsAdvertiseStream):

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        start_date: str = pendulum.today().subtract(days=1).to_date_string()
        end_date: str = pendulum.today().to_date_string()
        params = {"start": start_date, "end": end_date, "scale": "day", "metrics": "spend"}
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.status_code


class UnityAdsAdvertiseIncrementalStream(UnityAdsAdvertiseStream, IncrementalMixin):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone  = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)

    @property
    def name(self) -> str:
        """Override method to get stream name """
        stream_name = 'Acquisitions_Report'
        return stream_name

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "timestamp"
    
    @property
    def state(self) -> Mapping[str, Any]:
        if self.cursor_field:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
            return {self.cursor_field: self._cursor_value}
        else: 
            return {}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        if value[self.cursor_field] is None:
            self._cursor_value = pendulum.parse(self.config["start_date"]).date()
        else:
            self._cursor_value: datetime.date = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        # self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "timestamp": {"type": ["null", "string"],"format": "date-time","airbyte_type": "timestamp_with_timezone"},
                "app_id": {"type": ["null", "string"]},
                "app_name": {"type": ["null", "string"]},
                "campaign_id": {"type": ["null", "string"]},
                "campaign_name": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "creative_pack_id": {"type": ["null", "string"]},
                "creative_pack_name": {"type": ["null", "string"]},
                "creative_pack_type": {"type": ["null", "string"]},
                "os_version": {"type": ["null", "string"]},
                "platform": {"type": ["null", "string"]},
                "store": {"type": ["null", "string"]},
                "target_id": {"type": ["null", "string"]},
                "target_name": {"type": ["null", "string"]},
                "target_store_id": {"type": ["null", "string"]},
                "starts": {"type": ["null", "number"]},
                "views": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]},
                "installs": {"type": ["null", "number"]},
                "spend": {"type": ["null", "number"]}
            }
        }
        return full_schema
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice: list = []

        # data_available_date is the date that the newest data can be accessed
        data_avaliable_date : datetime.date = pendulum.today(self.timezone).date()
        
        if self.get_last_X_days:
            start: datetime.date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
        elif stream_state:
            start: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
        else: 
            start: datetime.date = pendulum.parse(self.config["start_date"]).date()
        
        while start < data_avaliable_date:
            start_as_str: str = start.to_date_string()
            if start.month == data_avaliable_date.month:
                end_as_str: str = data_avaliable_date.to_date_string()
                slice.append({
                    "start": start_as_str,
                    "end": end_as_str
                })
            else:
                end_as_str: str = start.end_of("month").add(days=1).to_date_string()
                slice.append({
                    "start": start_as_str,
                    "end": end_as_str
                })
            start: datetime.date = start.add(months=1).start_of('month') 

        return slice or [None]
    
    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "scale": "day",
            "metrics": "starts,views,clicks,installs,spend",
            "breakdowns": "app,campaign,country,creativePack,creativePackType,osVersion,platform,store,targetGame"
        }
        
        params.update(stream_slice)
        self.logger.info(f"Slice in params {stream_slice}")
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
    
    # def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
    #     '''
    #     First we decode the response into string.
    #     The string then can convert into a CSV file by using StringIO,
    #     In addition, we take the header of the CSV file and replace all white space by character "_"
    #     '''
    #     response_as_string: str = response.content.decode('utf-8-sig')
    #     response_as_list: list = response_as_string.split('\n')
    #     list_column_name: list = response_as_list[0].replace(' ','_').split(',') # get the correct column name
    #     df = csv.DictReader(StringIO(response_as_string), fieldnames = list_column_name)
    #     next(df, None) # skip the header before loop for
    #     for record in df:
    #         yield record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_as_string: str = response.content.decode('utf-8-sig')
        '''
        First, we convert response text to string by decoding utf-8-sig
        Then, we covert response string to a file object using StringIO
        After that, we load file object to pandas frame and rename column
        Finally, we yield records by using to_dict() of pandas data frame
        '''
        if response_as_string: 
            # load response to pandas data frame
            df = pd.read_csv(StringIO(response_as_string))
            # rename column
            df.rename(columns=lambda x: x.replace(' ','_'), inplace=True)
            # replace Nan value, Infinity as None
            df.replace([np.nan,np.inf,-np.inf], None, inplace=True)

            for record in df.to_dict(orient='records'):
                yield record
        else:
            return {}

# Source
class SourceUnityAdsAdvertise(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = BasicHttpAuthenticator(username=config["key_id"], password=config["secret_key"])
            logger.info(f"load auth {auth}")
            check_connection_steam = UnityAdsAdvertiseCheckConnectionStream(authenticator = auth, config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            record = next(check_connection_records)
            logger.info(f"There is one of records: {record}")
            record_2 = next(check_connection_records,"")
            logger.info(f"There is 2nd records: {record_2}")
            return True, None
        except Exception as e:
            return False

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = BasicHttpAuthenticator(username=config["key_id"], password=config["secret_key"])
        stream = UnityAdsAdvertiseIncrementalStream(authenticator = auth, config=config)
        return [stream]
