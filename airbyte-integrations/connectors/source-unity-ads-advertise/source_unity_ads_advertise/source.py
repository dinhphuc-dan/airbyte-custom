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
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.models import SyncMode
import pendulum
import datetime
from io import StringIO
import pandas as pd
import numpy as np


# Basic full refresh stream
class UnityAdsAdvertiseStream(HttpStream, ABC):
    url_base = "https://stats.unityads.unity3d.com/organizations/"

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
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


class UnityAdsAdvertiseCheckConnectionStream(UnityAdsAdvertiseStream):
    primary_key = None

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        organization_id = self.config["organization_id"]
        return f"{organization_id}/reports/acquisitions"

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        start_date: str = pendulum.today().subtract(days=1).to_date_string()
        end_date: str = pendulum.today().to_date_string()
        params = {"start": start_date, "end": end_date, "scale": "day"}

        # params = {
        #     "start": start_date, 
        #     "end": end_date, 
        #     "scale": "day",
        #     # "splitBy": "campaignSet,creativePack,adType,campaign,target,sourceAppId,store,country,platform,osVersion,skadConversionValue"
        #     "splitBy": "creativePack,adType,campaign,target,store,country,platform,osVersion,skadConversionValue",
        #     # "campaigns": "64c21e1a01a041430d59345f",
        #     "creativePacks": "656986e46ceb4ea199dedb26",
        #     # "countries": "BR"

        # }
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_as_string = response.content.decode('utf-8-sig').split('\n')
        yield {"response": response_as_string}

class UnityAdsAdvertiseIncrementalStream(UnityAdsAdvertiseStream, IncrementalMixin):
    primary_key = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone  = self.config.get("timezone", "UTC")

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        organization_id = self.config["organization_id"]
        return f"{organization_id}/reports/acquisitions"

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
        '''airbyte always starts syncing by checking stream availability, then sets cursor value as your logic at read_records() fucntion''' 
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        # self.logger.info(f"Cursor Setter Value {value}")
        self._cursor_value: datetime.date = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        # self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "timestamp": {"type": ["null", "string"],"format": "date-time","airbyte_type": "timestamp_with_timezone"},
                "target_id": {"type": ["null", "string"]},
                "target_store_id": {"type": ["null", "string"]},
                "target_name": {"type": ["null", "string"]},
                "creative_pack_id": {"type": ["null", "string"]},
                "creative_pack_name": {"type": ["null", "string"]},
                "ad_type": {"type": ["null", "string"]},
                "campaign_id": {"type": ["null", "string"]},
                "campaign_name": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "platform": {"type": ["null", "string"]},
                "os_version": {"type": ["null", "string"]},
                "store": {"type": ["null", "string"]},
                "SKAd_conversion_value": {"type": ["null", "string"]},
                "starts": {"type": ["null", "number"]},
                "views": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]},
                "installs": {"type": ["null", "number"]},
                "spend": {"type": ["null", "number"]},
            }
        }
        return full_schema
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice: list = []

        # data_available_date is the date that the newest data can be accessed
        data_avaliable_date : datetime.date = pendulum.today(self.timezone).date()
        
        # if stream has stream_state which means it has been run before, so start_date will be subtract X days backwards from last time run
        if stream_state:
            # print(f' stream slice, stream state in IF {stream_state}, {self._cursor_value}')
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
        else:
            # print(f' stream slice, stream state in ELSE {stream_state}, {self._cursor_value}')
            start_date: datetime.date = pendulum.parse(self.config["start_date"]).date()

        while start_date <= data_avaliable_date:
            start_date_as_str: str = start_date.to_date_string()
            if start_date.month == data_avaliable_date.month:
                end_date_as_str: str = data_avaliable_date.to_date_string()
                slice.append({
                    "start": start_date_as_str,
                    "end": end_date_as_str
                    }
                )
            else:
                end_date_as_str: str = start_date.end_of('month').to_date_string()
                slice.append({
                    "start": start_date_as_str,
                    "end": end_date_as_str
                    }
                )
            start_date: datetime.date = start_date.add(months=1).start_of('month')

        return slice or [None]
    
    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "scale": "day",
            "splitBy": "creativePack,adType,campaign,target,store,country,platform,osVersion,skadConversionValue"
        }
        self.logger.info(f"Slice in params {stream_slice}")
        params.update(stream_slice)
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
        response_as_string: str = response.content.decode('utf-8-sig')
        '''
        First, we convert response text to string by decoding utf-8-sig
        Then, we covert response string to a file object using StringIO
        After that, we load file object to pandas frame and rename column
        Finally, we yield records by using to_dict() of pandas data frame
        '''
        # load response to pandas data frame
        df = pd.read_csv(StringIO(response_as_string))
        # rename column
        df.rename(columns=lambda x: x.replace(' ','_'), inplace=True)
        # replace Nan value as None
        df.replace(np.nan, None, inplace=True)
        for record in df.to_dict(orient='records'):
            yield record

# Source
class SourceUnityAdsAdvertise(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = TokenAuthenticator(token=config["api_key"])
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
        auth = TokenAuthenticator(token=config["api_key"])
        stream = UnityAdsAdvertiseIncrementalStream(authenticator = auth, config=config)
        return [stream]
