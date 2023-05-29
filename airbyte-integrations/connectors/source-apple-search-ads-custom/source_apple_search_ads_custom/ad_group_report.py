from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
import requests
import datetime
import json

from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.models import SyncMode
from source_apple_search_ads_custom import utils
from source_apple_search_ads_custom.campaign_report import AppleSearchAdsCustomStream



class AppleSearchAdsAdGroupBaseStream(AppleSearchAdsCustomStream):
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
        stream_name = 'AdGroup_Level_Report'
        return stream_name

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        campaign_id = stream_slice['campaign_id']
        return f"reports/campaigns/{campaign_id}/adgroups"

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        header = {}
        header.update({"X-AP-Context": f"orgId={self.config['org_id']}"})
        return header

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        result = {}
        if response_json['data']['reportingDataResponse']['row'] is None:
            return None
        else:
            for row in response_json['data']['reportingDataResponse']['row']:
                '''dimesions'''
                result.update({"date": row['granularity'][0]['date']})
                result.update({"modificationTime": row['metadata']['modificationTime']})
                result.update({"campaignId": row['metadata']['campaignId']})
                result.update({"adGroupId": row['metadata']['adGroupId']})
                result.update({"adGroupName": row['metadata']['adGroupName']})
                result.update({"cpaGoal": row['metadata']['cpaGoal']})
                result.update({"adGroupStatus": row['metadata']['adGroupStatus']})
                result.update({"adGroupServingStatus": row['metadata']['adGroupServingStatus']})
                result.update({"adGroupServingStateReasons": row['metadata']['adGroupServingStateReasons']})
                result.update({"automatedKeywordsOptIn": row['metadata'].get('automatedKeywordsOptIn')})
                result.update({"adGroupDisplayStatus": row['metadata']['adGroupDisplayStatus']})
                result.update({"orgId": row['metadata']['orgId']})
                result.update({"pricingModel": row['metadata']['pricingModel']})
                result.update({"defaultBidAmount": 0 if row['metadata']['defaultBidAmount'] is None else row['metadata']['defaultBidAmount']['amount']})
                result.update({"countryCode": row['metadata']['countryCode']})

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
class AppleSearchAdsAdGroupStream(AppleSearchAdsAdGroupBaseStream, IncrementalMixin):
    number_days_backward_default = 7
    _record_date_format = "%Y-%m-%d"

    def __init__(self, list_campaign: dict, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self._list_campaign = list_campaign

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
    
    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "modificationTime": {"type": ["null", "string"]},
                "campaignId": {"type": ["null", "integer"]},
                "adGroupId": {"type": ["null", "integer"]},
                "adGroupName": {"type": ["null", "string"]},
                "cpaGoal": {"type": ["null", "string"]},
                "adGroupStatus": {"type": ["null", "string"]},
                "adGroupServingStatus": {"type": ["null", "string"]},
                "adGroupServingStateReasons": {"type": ["null", "string"]},
                "automatedKeywordsOptIn": {"type": ["null", "boolean"]},
                "adGroupDisplayStatus": {"type": ["null", "string"]},
                "orgId": {"type": ["null", "integer"]},
                "pricingModel": {"type": ["null", "string"]},
                "defaultBidAmount": {"type": ["null", "number"]},
                "countryCode": {"type": ["null", "string"]},
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

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = utils.string_to_date(value[self.cursor_field]) + datetime.timedelta(days=1)
        self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        today: datetime.date = datetime.date.today()
        number_days_backward: int = int(next(filter(None,[self.config.get('number_days_backward')]),self.number_days_backward_default))
        # test_day: datetime.date = datetime.date.today() - datetime.timedelta(days=number_days_backward) + datetime.timedelta(days=1)
        start_date: datetime.date = self.state[self.cursor_field] - datetime.timedelta(days=number_days_backward)
        # self.logger.info(f"self._list_campaign items {self._list_campaign.items()}")
        
        while start_date <= today:
            for name, id in self._list_campaign.items():
                end_date: datetime.date = start_date 
                slice.append(
                    {
                        'startTime': utils.date_to_string(start_date),
                        'endTime': utils.date_to_string(start_date),
                        'campaign_id': id
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
        body_json.update({'startTime' :stream_slice['startTime']})
        body_json.update({'endTime' :stream_slice['endTime']})
        body_json.update({"granularity": "DAILY"})
        body_json.update({
            "selector":{
                "conditions":[],
                "orderBy":[{
                    "field": "adGroupId",
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
        self.logger.info(f" stream slice date {stream_slice['startTime']} for campaign {stream_slice['campaign_id']}")
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