#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.models import SyncMode
import pendulum
import datetime


from requests_oauthlib import OAuth1
from urllib.parse import urlparse
import logging
from io import BytesIO
import gzip
import json

logger = logging.getLogger('airbyte')

class XAdsTwitterCustomStream(HttpStream, IncrementalMixin, ABC):
    url_base = "https://ads-api.twitter.com/12/"
    _cursor_value = None
    primary_key = None

    def __init__(self, config ,*args ,**kwargs):
        super().__init__(*args ,**kwargs)
        self.config = config
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone  = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
    
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
        if self.cursor_field:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
            return {self.cursor_field: self._cursor_value}
        else: 
            return {}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    '''
    For this api, if reponse request are paginated, the response json will contain `next_cursor` key with value as a cursor
    We will use that cursor value to get the next page
    Example: {"data": [], "next_cursor": "xxx"}
    '''
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response = response.json()
        if response.get("next_cursor", None):
            cursor_value = response["next_cursor"]
            return cursor_value
        else:
            return None

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

        ''' 
        this loop is for checking if start_date and data_avaliable_date are in same month
        if not, then create at least 2 slices in which start_date -> end_of_month and end_of_month -> data_avaliable_date
        else, create 1 slice in which start_date -> data_avaliable_date
        '''
        while start_date < data_avaliable_date:
            start_date_as_str: str = start_date.to_date_string()
            if start_date.month == data_avaliable_date.month:
                end_date_as_str: str = data_avaliable_date.to_date_string()
                slice.append({
                    "start_time": start_date_as_str,
                    "end_time": end_date_as_str
                    }
                )
            else:
                end_date_as_str: str = start_date.end_of('month').to_date_string()
                slice.append({
                    "start_time": start_date_as_str,
                    "end_time": end_date_as_str
                    }
                )
            start_date: datetime.date = start_date.add(months=1).start_of('month')

        # self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    ''' this API response json always has same format as {'data': [...], 'paging': {'next': '...&cursor=XXXXXXXXXXXX=='}}'''
    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_json = response.json()
        if response_json.get('data'):
            for row in response_json['data']:
                yield row
        else:
            yield {}
        # yield response_json

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
    
    def should_retry(self, response: requests.Response) -> bool:
        """
            this api endpoint is limited per 15-minute windown. 
            More info: https://developer.x.com/en/docs/twitter-ads-api/rate-limiting
            When you reach the limit, you will receive a 429 error, so we then need to pause for 15 minutes.
        """
        
        if response.status_code == 429:
            self._back_off_time = 900
            self._error_message = f"API reached limit. Pause for {self._back_off_time} second"
            return True
        else: 
            return super().should_retry(response=response)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if self._back_off_time:
            return self._back_off_time
        return super().backoff_time(response=response)
    
    def error_message(self, response: requests.Response) -> str:
        if self._error_message:
            return self._error_message
        return super().error_message(response=response)

class XAdsTwitterCustomCheckConnection(XAdsTwitterCustomStream):
    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return 'accounts'
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        account_ids = ','.join(self.config['ad_account_id'])
        params = {'account_ids': account_ids}
        return params

class XAdsTwitterLocationInfo(XAdsTwitterCustomStream): 
    count = 1000
    cursor_field = []

    @property
    def name(self) -> str:
        """Override method to get stream name """
        return 'geographical_location_info'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.location_type = self.config.get('location_type', 'COUNTRIES')

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return 'targeting_criteria/locations'  

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            'count': self.count,
            'location_type': self.location_type
        }
        return params
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        return [None]

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": {
                "name": {"type": ["null", "string"]},
                "country_code": {"type": ["null", "string"]},
                "location_type": {"type": ["null", "string"]},
                "targeting_value": {"type": ["null", "string"]},
                "targeting_type": {"type": ["null", "string"]},
            }
        }
        return full_schema

class XAdsTwitterPrepareEntityInfo(XAdsTwitterCustomStream):
    '''
    this class support 2 purposes:
    1. getting adgroup id and its infor for each ad account in config
    2. prepare request slice based on the 1 above as an input to register job api of Asynchronous Analytics
    https://developer.x.com/en/docs/twitter-ads-api/analytics/api-reference/asynchronous
    '''
    number_entities_in_one_job = 20

    def __init__(self, account_id ,entity_name ,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.account_id = account_id
        self.entity_name = entity_name
    
    # for getting adgroup id and its infor for each ad account in config
    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return f'accounts/{self.account_id}/{self.entity_name}'
    
    # for registering job
    def _grouping_entity_ids(self, line_item_object):
        '''
        line_item_object = 
                [
                    'ad_account_id': xxx,
                    'line_items': [{
                      'adgroup_name': xxx, 
                      'product_type': xxx, 
                      'adgroup_id': xxx, 
                      'android_app_store_identifier': xxx, 
                      'ios_app_store_identifier': xxx, 
                      'campaign_id': xxx, 
                      'campaign_name': xxx
                    }],
                ]
        we then convert it to: [{'ad_account_id': xxx, 'entity_ids': ['HERE***','KKK']}]
        Length of each 'entity_ids' list is equal to number_entities_in_one_job
        More info: https://developer.x.com/en/docs/twitter-ads-api/analytics/api-reference/asynchronous (find keyword entity_ids in docs)
        '''
        groups = []
        for item in line_item_object:
            n = 0 
            while n <= self.number_entities_in_one_job * (len(item['line_items']) // self.number_entities_in_one_job):
                list_id_in_one_group = [i['adgroup_id'] for i in item['line_items'][ n : n + self.number_entities_in_one_job]]
                groups.append({
                'entity_ids_as_list':list_id_in_one_group,
                'ad_account_id': item['ad_account_id'],
                'line_items': [i for i in item['line_items'][ n : n + self.number_entities_in_one_job]],
                })
                n = n + self.number_entities_in_one_job
        return groups

    # for registering job
    def _stream_slices(self, line_item_object, **kwargs) -> Iterable[Mapping[str, Any] | None]:
        '''here we try to add date slice into each entity group under an account id'''
        date_slice = super().stream_slices(stream_state= None,**kwargs)
        slice = []
        for date in date_slice:
            for group in self._grouping_entity_ids(line_item_object):
                # **date means passing the dictionary to the function as a **kwargs
                slice.append({
                    'entity_ids': (',').join(group['entity_ids_as_list']), 
                    'ad_account_id': group['ad_account_id'],
                    'line_items': group['line_items'],
                    **date})
        return slice
    
class XAdsTwitterLineItemsCreateAsynchronousJobs(XAdsTwitterCustomStream):
    '''
    This class purpose is to register and return the job id of Asynchronous Analytics
    https://developer.x.com/en/docs/twitter-ads-api/analytics/api-reference/asynchronous
    '''
    entity = 'LINE_ITEM'

    def __init__(self, request_slice ,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.request_slice = request_slice

    @property
    def http_method(self) -> str:
        return "POST"

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return f'stats/jobs/accounts/{self.request_slice["ad_account_id"]}'
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            'entity': self.entity,
            'entity_ids': self.request_slice['entity_ids'],
            'start_time': self.request_slice['start_time'],
            'end_time': self.request_slice['end_time'],
            'granularity': 'DAY',
            'metric_groups': 'ENGAGEMENT,BILLING,VIDEO,MEDIA',
            'segmentation_type': 'LOCATIONS',
            'placement': self.config['placement'],
        }
        return params
    
    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        ''' rewrite to get job id '''
        self.logger.info(f"Complete Post Asynchronous Jobs, account {self.request_slice['ad_account_id']} and job id as {response.json()['data']['id']}")
        response_json = response.json()
        if response_json.get('data'):
            yield response_json.get('data')
        else:
            yield {}


class XAdsTwitterHandleJobStatus(XAdsTwitterLineItemsCreateAsynchronousJobs):
    '''
    This class purpose is to checking job id status
    If the job is done, we get the download link
    If not, we pause for 10s and then try again
    '''

    @property
    def http_method(self) -> str:
        return "GET"
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            'job_ids': self.request_slice['job_ids'],
        }
        return params
    
    def should_retry(self, response: requests.Response) -> bool:
        """
            Job ID Status has 2 possible status: 'SUCCESS' and 'PROCESSING'.
            More info: https://devcommunity.x.com/t/twitter-async-job-returning-all-null-values/175665
            If job status is 'PROCESSING', we pause for 10s and then try again
        """
        self.logger.info(f"Checking status in Asynchronous Jobs, account {self.request_slice['ad_account_id']} and job id {self.request_slice['job_ids']}")
        if response.status_code == 200 and response.json().get('data')[0].get('status') == 'PROCESSING':
            self._back_off_time = 10
            self._error_message = f"Job {self.request_slice['job_ids']} has not completed. Pause for {self._back_off_time} second"
            return True
        else: 
            return super().should_retry(response=response)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if self._back_off_time:
            return self._back_off_time
        return super().backoff_time(response=response)
    
    def error_message(self, response: requests.Response) -> str:
        if self._error_message:
            return self._error_message
        return super().error_message(response=response)
    
    @property
    def max_retries(self) -> Union[int, None]:
        """ Override if needed. Specifies maximum amount of retries for backoff policy. Return None for no limit."""
        return None

    @property
    def max_time(self) -> Union[int, None]:
        """ Override if needed. Specifies maximum total waiting time (in seconds) for backoff policy. Return None for no limit."""
        return None
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        ''' rewrite to get job id '''
        return XAdsTwitterCustomStream.parse_response(self, response, **kwargs)

class XAdsTwitterAdGroupsReport(XAdsTwitterCustomStream):
    url_base = 'https://ton.twimg.com'

    def __init__(self, request_slices, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.request_slices = request_slices

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any] | None]:
        slice = [request_slice for request_slice in self.request_slices]
        return slice
    
    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        url = stream_slice['url']
        path = urlparse(url=url).path
        return path

    @property
    def name(self) -> str:
        """Override method to get stream name """
        return 'adgroups_location_daily_report'
    
    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        self.logger.info(f"Status code in AdGroups Report {response.status_code}")
        ''' 
        rewrite due to response is gzip file
        we first get the response content as gzip and then convert it to json
        '''
        file_gzip = BytesIO(response.content)
        file = gzip.GzipFile(fileobj=file_gzip)
        response_json = json.load(file)

        start_time = pendulum.parse(stream_slice['start_time'])
        end_time = pendulum.parse(stream_slice['end_time'])
        date_diff = (end_time - start_time).days

        line_items = stream_slice['line_items']
        
        if response_json.get('data'): 
            for entity_id in response_json['data']:
                result = {}
                
                # getting extra info about adgroup's name, its campaign or accounts
                for item in line_items:
                    if item['adgroup_id'] == entity_id['id']:
                        result.update(**item)

                # getting segment data
                for segment in entity_id['id_data']:
                    # getting dimension as segment
                    result.update(**segment['segment'])
                    # getting metrics
                    for n in range(0, date_diff):
                        for k,v in segment['metrics'].items():
                            if v:
                                result.update({
                                    'date': start_time.add(days=n).to_date_string(),
                                    k: v[n]
                                })
                            else:
                                result.update({
                                    'date': start_time.add(days=n).to_date_string(),
                                    k: v
                                })
                            

                        yield result

        # yield response_json
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": {
                "adgroup_name": {"type": ["null", "string"]},
                "product_type": {"type": ["null", "string"]},
                "adgroup_id": {"type": ["null", "string"]},
                "android_app_store_identifier": {"type": ["null", "string"]},
                "ios_app_store_identifier": {"type": ["null", "string"]},
                "campaign_id": {"type": ["null", "string"]},
                "campaign_name": {"type": ["null", "string"]},
                "segment_name": {"type": ["null", "string"]},
                "segment_value": {"type": ["null", "string"]},
                "date": {"type": ["null", "string"]},
                "video_views_50": {"type": ["null", "number"]},
                "impressions": {"type": ["null", "number"]},
                "tweets_send": {"type": ["null", "number"]},
                "billed_charge_local_micro": {"type": ["null", "number"]},
                "qualified_impressions": {"type": ["null", "number"]},
                "video_views_75": {"type": ["null", "number"]},
                "media_engagements": {"type": ["null", "number"]},
                "follows": {"type": ["null", "number"]},
                "video_3s100pct_views": {"type": ["null", "number"]},
                "app_clicks": {"type": ["null", "number"]},
                "retweets": {"type": ["null", "number"]},
                "video_cta_clicks": {"type": ["null", "number"]},
                "unfollows": {"type": ["null", "number"]},
                "likes": {"type": ["null", "number"]},
                "video_content_starts": {"type": ["null", "number"]},
                "video_views_25": {"type": ["null", "number"]},
                "engagements": {"type": ["null", "number"]},
                "video_views_100": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]},
                "media_views": {"type": ["null", "number"]},
                "card_engagements": {"type": ["null", "number"]},
                "video_6s_views": {"type": ["null", "number"]},
                "poll_card_vote": {"type": ["null", "number"]},
                "replies": {"type": ["null", "number"]},
                "video_15s_views": {"type": ["null", "number"]},
                "url_clicks": {"type": ["null", "number"]},
                "billed_engagements": {"type": ["null", "number"]},
                "video_total_views": {"type": ["null", "number"]},
                "carousel_swipes": {"type": ["null", "number"]},
            }
        }
        return full_schema

# Source
class SourceXAdsTwitterCustom(AbstractSource):
    
    entity_list = ['campaigns', 'line_items']
    entity_info = []

    def _get_auth(self, config) -> OAuth1:
        return OAuth1(
            client_key = config['api_key'],
            client_secret= config['api_secret'],
            resource_owner_key=config['access_token'],
            resource_owner_secret=config['access_token_secret'],
        )

    def _get_line_item_object(self,config) -> List[dict]:
            auth = self._get_auth(config)
            
            list_object = []
            for id in config['ad_account_id']:

                list_campaigns = []
                campaigns_info_stream = XAdsTwitterPrepareEntityInfo(authenticator = auth, config=config, account_id = id, entity_name= self.entity_list[0]) 
                campaigns_info_data = campaigns_info_stream.read_records(sync_mode="full_refresh")
                logger.info(f"Get campaigns_info_data of account {id}")
                for record in campaigns_info_data:
                    if record:
                        info  = {}
                        for k,v in record.items():
                            if k in ('name', 'id') :
                                info.update({'campaign_' + k : v})
                        list_campaigns.append(info)

                list_adgroups = []
                adgroups_info_stream = XAdsTwitterPrepareEntityInfo(authenticator = auth, config=config, account_id = id, entity_name= self.entity_list[1]) 
                adgroups_info_data = adgroups_info_stream.read_records(sync_mode="full_refresh")
                logger.info(f"Get adgroups_info_data of account {id}")
                for record in adgroups_info_data:
                    if record:
                        info  = {}
                        for k,v in record.items():
                            if k in ('product_type','android_app_store_identifier', 'ios_app_store_identifier', 'campaign_id') : 
                                info.update({k:v})
                            if k in ('name', 'id'):
                                info.update({ 'adgroup_' + k : v})
                        list_adgroups.append(info)

                '''
                 list comprehension for update each ad_id with it's campaign, adsquad and account infomation in below format
                [
                    'ad_account_id': xxx,
                    'line_items': [{
                      'adgroup_name': xxx, 
                      'product_type': xxx, 
                      'adgroup_id': xxx, 
                      'android_app_store_identifier': xxx, 
                      'ios_app_store_identifier': xxx, 
                      'campaign_id': xxx, 
                      'campaign_name': xxx
                    }],
                ]
                ''' 
                
                [i.update(k) for i in list_adgroups for k in list_campaigns if i['campaign_id'] == k['campaign_id']]
                # getting only account that has line_items
                if list_adgroups:
                    list_object.append({'ad_account_id': id, 'line_items': list_adgroups})
            yield from list_object

    def _get_job_id_and_handle_job_status(self ,config) -> List[dict]:
        auth = self._get_auth(config)
        request_slices = XAdsTwitterPrepareEntityInfo(authenticator = auth, config=config, account_id = None, entity_name= None)._stream_slices(line_item_object=self._get_line_item_object(config=config))
        # get job id of each ad account with groups of adgroup ID (line_items)
        for slice in request_slices:
            register_job = XAdsTwitterLineItemsCreateAsynchronousJobs(
                authenticator = auth, 
                config=config, 
                request_slice = slice
            ) 

            # update job_ids in slice
            slice.update({'job_ids' : next(record['id'] for record in register_job.read_records(sync_mode="full_refresh"))})
            
            # handle job status from same slice
            check_job_status = XAdsTwitterHandleJobStatus(
                authenticator = auth, 
                config=config, 
                request_slice = slice
            )       

            # update url in slice
            slice.update({'url' : next(record['url'] for record in check_job_status.read_records(sync_mode="full_refresh"))})
        yield from request_slices

    

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = self._get_auth(config)
            logger.info(f"load auth {auth}")
            check_connection_steam = XAdsTwitterCustomCheckConnection(authenticator = auth, config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            record = next(check_connection_records)
            logger.info(f"There is one of records: {record}")


            # print(self._get_line_item_object(config))
            # print(len(self._get_line_item_object(config)))
            # print(self._get_job_id_and_handle_job_status(logger=logger,config=config))

            return True, None
        except Exception as e:
            return False

    def streams(self,config: Mapping[str, Any]) -> List[Stream]:
        auth = self._get_auth(config)
        streams = []
        streams.append(XAdsTwitterLocationInfo(authenticator = auth, config=config))

        request_slices = self._get_job_id_and_handle_job_status(config=config)
        streams.append(XAdsTwitterAdGroupsReport(
            authenticator = auth, 
            config=config, 
            request_slices = request_slices
        ))
        return streams
