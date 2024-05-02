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
import json


class TiktokTokenAuthenticator(TokenAuthenticator):
    def __init__(self, token: str, **kwargs):
        super().__init__(token, **kwargs)
        self.access_token = token

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"Access-Token": self.access_token}


# Basic full refresh stream
class TiktokAdsCustomStream(HttpStream, ABC):
    url_base = "https://business-api.tiktok.com/open_api/v1.3/"

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

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}

    def should_retry(self, response: requests.Response) -> bool:
        """
        Tiktok Ads API returns HTTP status codes 200, but still can be an unsuccessful call
        Docs: https://business-api.tiktok.com/portal/docs?id=1737172488964097
        Once the rate limit is met, the server returns "code": 40016, 40100, 40133
        Retry 50002 as well - it's a server error.
        """

        if response.status_code == 429 or 500 <= response.status_code < 600:
            return True
        elif response.status_code == 200:
            data = response.json()
            if data["code"] in (40016, 40100, 40133, 50002):
                return True
        else:
            return False

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """
        The system uses a second call limit for each developer app. The set limit varies according to the app's call limit level.
        """
        # Basic: 	10/sec
        # Advanced: 	20/sec
        # Premium: 	30/sec
        # All apps are set to basic call limit level by default.
        # Returns maximum possible delay
        return 0.6

class TiktokAdsTestConnection(TiktokAdsCustomStream):
    primary_key = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "advertiser/info/"
    
    def request_body_json(
            self, stream_state: Optional[Mapping[str, Any]], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None,
        ) -> Optional[Mapping[str, Any]]:
        list_advertiser_ids: list = self.config.get("advertiser_ids").split(",")
        body = {"advertiser_ids": list_advertiser_ids}
        return body
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Tiktok Ads API returns HTTP status codes 200, but still can be an unsuccessful call
        We need to compare code in the response body as well
        Docs: https://business-api.tiktok.com/portal/docs?id=1737172488964097
        All responses have the similar structure:
        {
            "message": "<OK or ERROR>",
            "code": <code>, # 0 if error else error unique code
            "request_id": "<unique_request_id>"
            "data": {
                "page_info": {
                    "total_number": <total_item_count>,
                    "page": <current_page_number>,
                    "page_size": <page_size>,
                    "total_page": <total_page_count>
                },
                "list": [
                    <list_item>
                ]
           }
        }
        """
        data = response.json()
        if data["code"]: # if code is NOT equal to 0, 1 or None 
            self.logger.error(f"{data['message']}")
            raise Exception()
        for record in data["data"]["list"]:
            yield record

class TiktokAdsAdGroupReport(TiktokAdsCustomStream, IncrementalMixin):
    primary_key = None
    page_size = 1000
    maximum_date_range = 30

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.list_advertiser_ids: list = self.config.get("advertiser_ids").split(",")
        self.number_days_backward: int = self.config.get("number_days_backward", 7)
        self.timezone: str  = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
        self.include_deleted: bool = self.config.get("include_deleted", False)

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "report/integrated/get/"
    
    @property
    def name(self) -> str:
        """Override method to get stream name """
        stream_name = 'AdGroup_Report'
        return stream_name
    
    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "stat_time_day"
    
    @property
    def state(self) -> Mapping[str, Any]:
        '''sets cursor value as your logic at read_records() fucntion''' 
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice: list = []

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
            start_date_as_str: str = start_date.to_date_string()
            if (data_avaliable_date - start_date).days >= self.maximum_date_range:
                end_date: datetime.date = start_date.add(days=self.maximum_date_range)
                end_date_as_str: str = end_date.to_date_string()
                for id in self.list_advertiser_ids:
                    slice.append({
                        "start_date": start_date_as_str,
                        "end_date": end_date_as_str,
                        "advertiser_id": id
                        }
                    )
            else:
                end_date: datetime.date = data_avaliable_date
                end_date_as_str: str = end_date.to_date_string()
                for id in self.list_advertiser_ids:
                    slice.append({
                        "start_date": start_date_as_str,
                        "end_date": end_date_as_str,
                        "advertiser_id": id
                        }
                    )
            start_date: datetime.date = end_date.add(days=1)

        return slice or [None]
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        total_page = response_json['data']['page_info']['total_page']
        page = response_json['data']['page_info']['page']
        if page == total_page or total_page == 0:
            return None
        else:
            return page
    
    def request_body_json(
            self, stream_state: Optional[Mapping[str, Any]], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None,
        ) -> Optional[Mapping[str, Any]]:
        # default body params
        body = {
            "report_type": "BASIC",
            "data_level": "AUCTION_ADGROUP",
            "dimensions": ["adgroup_id", "stat_time_day","country_code"],
            "metrics": ["promotion_type","placement_type","adgroup_name","campaign_name","campaign_id", "objective_type","spend", "impressions", "clicks", "reach", "conversion","real_time_conversion", "result","real_time_result","currency"],
            "page_size": self.page_size
        }

        if next_page_token:
            next_page = next_page_token + 1
            body.update({"page": next_page})

        if self.include_deleted:
            body.update({"filtering": [{"filter_value": json.dumps(["STATUS_ALL"]), "field_name": "adgroup_status", "filter_type": "IN"}]})

        body.update(stream_slice)
        self.logger.info(f" Request Params {stream_slice['start_date'], stream_slice['end_date'], stream_slice['advertiser_id']}")
        return body

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

            # because response does not contain advertiser_id, so we add it here
            if record is not None:
                record.update({"advertiser_id": stream_slice["advertiser_id"]})
            yield record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Tiktok Ads API returns HTTP status codes 200, but still can be an unsuccessful call
        We need to compare code in the response body as well
        Docs: https://business-api.tiktok.com/portal/docs?id=1737172488964097
        All responses have the similar structure:
        {
            "message": "<OK or ERROR>",
            "code": <code>, # 0 if error else error unique code
            "request_id": "<unique_request_id>"
            "data": {
                "page_info": {
                    "total_number": <total_item_count>,
                    "page": <current_page_number>,
                    "page_size": <page_size>,
                    "total_page": <total_page_count>
                },
                "list": [
                    <list_item>
                ]
           }
        }
        """
        result = response.json()
        self.logger.info(f"Status code in Parse Response {response.status_code} and Tiktok code {result['code']}")
        if result["code"]: # if code is NOT equal to 0, 1 or None 
            self.logger.error(f"{result['message']}")
            raise Exception()
        for item in result['data']['list']:
            record = {}
            record.update(item['dimensions'])
            record.update(item['metrics'])
            yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "stat_time_day": {"type": ["null", "string"]},
                "advertiser_id": {"type": ["null", "string"]},
                "country_code": {"type": ["null", "string"]},
                "adgroup_id": {"type": ["null", "string"]},
                "adgroup_name": {"type": ["null", "string"]},
                "campaign_id": {"type": ["null", "string"]},
                "campaign_name": {"type": ["null", "string"]},
                "placement_type": {"type": ["null", "string"]},
                "objective_type": {"type": ["null", "string"]},
                "promotion_type": {"type": ["null", "string"]},
                "currency": {"type": ["null", "string"]},
                "spend": {"type": ["null", "number"]},
                "impressions": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]},
                "conversion": {"type": ["null", "number"]},
                "real_time_conversion": {"type": ["null", "number"]},
                "result": {"type": ["null", "number"]},
                "real_time_result": {"type": ["null", "number"]},
                "reach": {"type": ["null", "number"]},
            }
        }
        return full_schema


# Source
class SourceTiktokAdsCustom(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try: 
            token=config.get("access_token")
            auth = TiktokTokenAuthenticator(token=token)
            logger.info(f"load auth {auth}")
            check_connection_steam = TiktokAdsTestConnection(authenticator = auth, config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            record = next(check_connection_records)
            logger.info(f"There is one of records: {record}")
            return True, None
        except Exception as e:
            return False


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        token=config.get("access_token")
        auth = TiktokTokenAuthenticator(token=token)
        streams = [TiktokAdsAdGroupReport(authenticator = auth, config=config)]
        return streams
