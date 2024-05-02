from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import requests
import datetime
import json
import pendulum

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode


# Basic base abstract stream
class AppleSearchAdsCustomStream(HttpStream, ABC):
    url_base = "https://api.searchads.apple.com/api/v4/"

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


class AppleSearchAdsCheckConnectionStream(AppleSearchAdsCustomStream):
    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "campaigns"

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        header = {}
        header.update({"X-AP-Context": f"orgId={self.config['org_id']}"})
        return header
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {'limit': 1000}
        return params
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        result = {}
        for record in response_json['data']:
            result.update({record['name']: record['id']})
        yield result

        # ''' use when to check raw response from API'''
        # yield response_json

class AppleSearchAdsCampaignBaseStream(AppleSearchAdsCustomStream):
    primary_key = None

    def __init__(self, **kwargs):
        """override __init__ to add stream name and resolve MRO"""
        super().__init__(**kwargs)
    
    @property
    def http_method(self) -> str:
       """ Override because using POST. Default by airbyte is GET """
       return "POST"
    
    @property
    def name(self) -> str:
        """Override method to get stream name """
        stream_name = 'Campain_Level_Report'
        return stream_name

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "reports/campaigns"

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        header = {}
        header.update({"X-AP-Context": f"orgId={self.config['org_id']}"})
        return header

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_json = response.json()
        result = {}
        for row in response_json['data']['reportingDataResponse']['row']:

            '''dimesions'''
            result.update({"modificationTime": row['metadata']['modificationTime']})
            result.update({"campaignId": row['metadata']['campaignId']})
            result.update({"campaignName": row['metadata']['campaignName']})
            result.update({"campaignStatus": row['metadata']['campaignStatus']})
            result.update({"appName": row['metadata']['app']['appName']})
            result.update({"appID": row['metadata']['app']['adamId']})
            result.update({"servingStatus": row['metadata']['servingStatus']})
            result.update({"servingStateReasons": row['metadata']['servingStateReasons'] if row['metadata']['servingStateReasons'] is None else ','.join(row['metadata']['servingStateReasons']) })
            result.update({"countriesOrRegions":  row['metadata']['countriesOrRegions'] if row['metadata']['countriesOrRegions'] is None else ','.join(row['metadata']['countriesOrRegions'])})
            result.update({"countryCode": row['metadata']['countryCode']})
            result.update({"totalBudget": row['metadata']['totalBudget'] if row['metadata']['totalBudget'] is None else row['metadata']['totalBudget']['amount']} )
            result.update({"dailyBudget": row['metadata']['dailyBudget'] if row['metadata']['dailyBudget'] is None else row['metadata']['dailyBudget']['amount']})
            result.update({"displayStatus": row['metadata']['displayStatus']})
            result.update({"supplySources": row['metadata']['supplySources'] if row['metadata']['supplySources'] is None else ','.join(row['metadata']['supplySources'])})
            result.update({"adChannelType": row['metadata']['adChannelType']})
            result.update({"orgId": row['metadata']['orgId']})
            result.update({"countryOrRegionServingStateReasons": row['metadata']['countryOrRegionServingStateReasons'] if row['metadata']['countryOrRegionServingStateReasons'] is None else ','.join(row['metadata']['countryOrRegionServingStateReasons'])})
            result.update({"billingEvent": row['metadata']['billingEvent']})
            
            '''metrics'''
            for item in row['granularity']:
                result.update({"date": item['date']})
                result.update({"impressions": item['impressions']})
                result.update({"taps": item['taps']})
                result.update({"installs": item['installs']})
                result.update({"newDownloads": item['newDownloads']})
                result.update({"redownloads": item['redownloads']})
                result.update({"latOnInstalls": item['latOnInstalls']})
                result.update({"latOffInstalls": item['latOffInstalls']})
                result.update({"ttr": item['ttr']})
                result.update({"avgCPA": item['avgCPA']['amount']})
                result.update({"avgCPT": item['avgCPT']['amount']})
                result.update({"avgCPM": item['avgCPM']['amount']})
                result.update({"localSpend": item['localSpend']['amount']})
                result.update({"conversionRate": item['conversionRate']})
                yield result
        
        # yield response_json


# Basic incremental stream
class AppleSearchAdsCampaignStream(AppleSearchAdsCampaignBaseStream, IncrementalMixin):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone  = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)

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
    
    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "modificationTime": {"type": ["null", "string"]},
                "campaignId": {"type": ["null", "integer"]},
                "campaignName": {"type": ["null", "string"]},
                "campaignStatus": {"type": ["null", "string"]},
                "appName": {"type": ["null", "string"]},
                "appID": {"type": ["null", "integer"]},
                "servingStatus": {"type": ["null", "string"]},
                "servingStateReasons": {"type": ["null", "string"]},
                "countriesOrRegions": {"type": ["null", "string"]},
                "countryCode": {"type": ["null", "string"]},
                "totalBudget": {"type": ["null", "number"]},
                "dailyBudget": {"type": ["null", "number"]},
                "displayStatus": {"type": ["null", "string"]},
                "supplySources": {"type": ["null", "string"]},
                "adChannelType": {"type": ["null", "string"]},
                "orgId": {"type": ["null", "integer"]},
                "countryOrRegionServingStateReasons": {"type": ["null", "string"]},
                "billingEvent": {"type": ["null", "string"]},
                "impressions": {"type": ["null", "number"]},
                "taps": {"type": ["null", "number"]},
                "installs": {"type": ["null", "number"]},
                "newDownloads": {"type": ["null", "number"]},
                "redownloads": {"type": ["null", "number"]},
                "latOnInstalls": {"type": ["null", "number"]},
                "latOffInstalls": {"type": ["null", "number"]},
                "ttr": {"type": ["null", "number"]},
                "avgCPA": {"type": ["null", "number"]},
                "avgCPT": {"type": ["null", "number"]},
                "avgCPM": {"type": ["null", "number"]},
                "localSpend": {"type": ["null", "number"]},
                "conversionRate": {"type": ["null", "number"]},
            }
        }
        return full_schema

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
            start_date_as_str: str = start_date.to_date_string()
            if start_date.month == data_avaliable_date.month:
                end_date_as_str: str = data_avaliable_date.to_date_string()
                slice.append(
                    {
                        'startTime': start_date_as_str,
                        'endTime': end_date_as_str,
                    }
                )
            else:
                end_date_as_str: str = start_date.end_of('month').to_date_string()
                slice.append(
                    {
                        'startTime': start_date_as_str,
                        'endTime': end_date_as_str,
                    }
                )
            start_date: datetime.date = start_date.add(months=1).start_of('month')

        # self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        """ 
        API needs a body json. 
        """
        body_json = {}
        body_json.update(stream_slice)
        body_json.update({"granularity": "DAILY"})
        body_json.update({
            "selector":{
                "conditions":[],
                "orderBy":[{
                    "field": "campaignId",
                    "sortOrder": "ASCENDING"
                }]
            }
        })
        body_json.update({"groupBy": ["countryCode"]})
        body_json.update({  "timeZone": "ORTZ",
        "returnRecordsWithNoMetrics": False,
        "returnRowTotals": False,
        "returnGrandTotals": False
        })
        self.logger.info(f" stream slice date {stream_slice}")
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
            # self.logger.info(f"read record; record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record