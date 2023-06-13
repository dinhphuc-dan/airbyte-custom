from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import requests
import datetime
import json
import pendulum

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from source_apple_search_ads_custom import utils

# Basic base abstract stream
class AppleSearchAdsCustomStream(HttpStream, ABC):
    url_base = "https://api.searchads.apple.com/api/v4/"

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
        response_json = response.json()
        result = {}
        for row in response_json['data']['reportingDataResponse']['row']:

            '''dimesions'''
            result.update({"date": row['granularity'][0]['date']})
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
            
            
            ''' metrics'''
            result.update({"impressions": row['granularity'][0]['impressions']})
            result.update({"taps": row['granularity'][0]['taps']})
            result.update({"installs": row['granularity'][0]['installs']})
            result.update({"newDownloads": row['granularity'][0]['newDownloads']})
            result.update({"redownloads": row['granularity'][0]['redownloads']})
            result.update({"latOnInstalls": row['granularity'][0]['latOnInstalls']})
            result.update({"latOffInstalls": row['granularity'][0]['latOffInstalls']})
            result.update({"ttr": row['granularity'][0]['ttr']})
            result.update({"avgCPA": row['granularity'][0]['avgCPA']['amount']})
            result.update({"avgCPT": row['granularity'][0]['avgCPT']['amount']})
            result.update({"avgCPM": row['granularity'][0]['avgCPM']['amount']})
            result.update({"localSpend": row['granularity'][0]['localSpend']['amount']})
            result.update({"conversionRate": row['granularity'][0]['conversionRate']})
            yield result
        
        # yield response_json


# Basic incremental stream
class AppleSearchAdsCampaignStream(AppleSearchAdsCampaignBaseStream, IncrementalMixin):
    number_days_backward_default = 7
    _record_date_format = "%Y-%m-%d"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"
    
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
        # today: datetime.date = datetime.date.today()
        if self.config.get('time_zone'):
            today = pendulum.today(self.config['time_zone']).date()
        else:
            today = pendulum.today().date()
        number_days_backward: int = int(next(filter(None,[self.config.get('number_days_backward')]),self.number_days_backward_default))
        start_date: datetime.date = self.state[self.cursor_field] - datetime.timedelta(days=number_days_backward)
        while start_date <= today:
            end_date: datetime.date = start_date 
            slice.append(
                {
                    'startTime': utils.date_to_string(start_date),
                    'endTime': utils.date_to_string(start_date),
                }
            )
            start_date: datetime.date = end_date + datetime.timedelta(days=1)

        self.logger.info(f"stream slice {slice}")
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
        self.logger.info(f" stream slice date {stream_slice['startTime']}")
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
            record_cursor_value = utils.string_to_date(record[self.cursor_field], self._record_date_format)
            self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record; record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record