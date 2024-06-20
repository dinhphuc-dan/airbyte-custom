#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.models import SyncMode
import pendulum
import datetime
import re
from source_snapchat_ads_custom.authenticator import SnapchatAdvertiseAuthenticator

from functools import lru_cache

# Base stream
class SnapchatAdsCustomBaseStream(HttpStream, IncrementalMixin ,ABC):
    url_base = "https://adsapi.snapchat.com/v1/"
    _cursor_value = None
    primary_key = None

    def __init__(self, 
                 config,
                 entity_name_in_path,
                 entity_id_in_path, 
                 parent_entity_name = None,
                 child_entity = None ,
                 *args ,**kwargs
        ):
        super().__init__(*args ,**kwargs)
        self.config: dict = config
        self.number_days_backward: int = self.config.get("number_days_backward", 7)
        self.timezone: str  = self.config.get("timezone", "UTC")
        self.get_last_X_days: int = self.config.get("get_last_X_days", False)

        # we try to get data of each entity
        self.entity_name_in_path: str = entity_name_in_path.replace("_", "") + "s"
        self.entity_id_in_path: str = entity_id_in_path
        self.parent_entity_name: str  = parent_entity_name
        self.child_entity: str = child_entity

    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None
    
    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None
    
    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "start_time"
    
    @property
    def state(self) -> Mapping[str, Any]:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    
    """
    For this api, if reponse request are paginated, the response json will contain a next link in a `paging` key, in that link contains a param"s name  as cursor
    We will use cursor value to get the next page
    Example: {"ENTIRY_NAME": [...], "paging": {"next_link": "...&cursor=XXXXXXXXXXXX=="}}
    """
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response = response.json()
        if response.get("paging"):
            next_link = response["paging"]["next_link"]
            cursor_param = re.search(r"cursor=[a-zA-Z0-9]+",next_link)
            cursor_param_value = cursor_param.group().split("=")[1]
            return cursor_param_value
        else:
            return None

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice: list = []
        
        # data_available_date is the date that the newest data can be accessed
        data_avaliable_date : datetime.date = pendulum.today(self.timezone).date() 

        if self.get_last_X_days:
            """" this code for all kind of run, such as: the first time run or full refresh or incremental run, the stream will start with today date minus number_days_backward"""
            start_date: datetime.date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        elif stream_state:
            """ this code for incremental run and get_last_X_days is false, the stream will start with the last date of stream state minus number_days_backward"""
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        else: 
            """" this code for the first time run or full refresh run, the stream will start with the start date in config"""
            start_date: datetime.date = pendulum.parse(self.config["start_date"]).date()
            # self.logger.info(f"stream slice start date in ELSE {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        """ 
        this loop is for checking if start_date and data_avaliable_date are in same month
        if not, then create at least 2 slices in which start_date -> end_of_month and end_of_month -> data_avaliable_date
        else, create 1 slice in which start_date -> data_avaliable_date
        """
        while start_date < data_avaliable_date:
            start_date_as_str: str = start_date.to_date_string()
            if start_date.month == data_avaliable_date.month:
                end_date_as_str: str = data_avaliable_date.to_date_string()
                slice.append({
                    "start_date": start_date_as_str,
                    "end_date": end_date_as_str
                    }
                )
            else:
                end_date_as_str: str = start_date.end_of("month").to_date_string()
                slice.append({
                    "start_date": start_date_as_str,
                    "end_date": end_date_as_str
                    }
                )
            start_date: datetime.date = start_date.add(months=1).start_of("month")

        # self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    """ return data as default response json """
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_json = response.json()
        yield response_json

class SnapchatAdsCheckConnection(SnapchatAdsCustomBaseStream):
    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """ this API follow this format https://adsapi.snapchat.com/v1/{entity_name}/{entity_id}"""
        return f"{self.entity_name_in_path}/{self.entity_id_in_path}"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.status_code

class SnapchatEntityUnderOneAdAccount(SnapchatAdsCustomBaseStream):

    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        
        child_entity_with_plural = self.child_entity.replace("_", "") + "s"
        path  = f"{self.entity_name_in_path}/{self.entity_id_in_path}/{child_entity_with_plural}"
        return path

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        
        params = {
            "read_deleted_entities": True,
        }
        if next_page_token:
            params.update({"cursor": next_page_token})
        return params

    """ this API response follow this format 
    {
        child_entity_with_plural:[{
            child_entity: {
                id: xxx,
                name: xxx,
                parent_entity_name_id: xxx
            }
        }]
    }
    """
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        entity_in_response = self.child_entity.replace("_", "")
        for record in response_json.get(entity_in_response + "s"):
            result = {}
            if record[entity_in_response]:
                result.update({
                    self.child_entity + "_id": record[entity_in_response]["id"],
                    self.child_entity + "_name": record[entity_in_response]["name"],
                    self.parent_entity_name + "_id" : record[entity_in_response][self.parent_entity_name + "_id"],
                })
            yield result

class SnapchatAdsAdStatsReport(SnapchatAdsCustomBaseStream):

    list_metrics = [
        "impressions","swipes","spend","total_impressions",
        "total_installs","android_installs","ios_installs",
        "screen_time_millis","paid_impressions","view_time_millis",
        "quartile_1","quartile_2","quartile_3","view_completion","video_views",
        "shares","saves","uniques","story_opens","story_completes",
        "custom_event_1","custom_event_2","custom_event_3","custom_event_4","custom_event_5",
        "attachment_uniques","attachment_view_completion","attachment_quartile_3","attachment_quartile_2","attachment_quartile_1","attachment_video_views",
        "conversion_rate","conversion_save","conversion_login","conversion_share","conversion_invite",
        "conversion_ad_view","conversion_reserve","conversion_ad_click","conversion_add_cart","conversion_searches",
        "conversion_app_opens","conversion_list_view","attachment_total_view_time_millis","conversion_complete_tutorial",
        "conversion_purchases_value","conversion_level_completes","conversion_add_to_wishlist","conversion_visit",
        "conversion_start_checkout","conversion_spend_credits","conversion_view_content","conversion_start_trial",
        "conversion_add_billing","conversion_page_views","conversion_subscribe","conversion_purchases","conversion_sign_ups","conversion_achievement_unlocked"]

    def __init__(self, entity_info , *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.entity_info: List[dict] = entity_info

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name """
        stream_name = "Ads_Stats_Report"
        return stream_name

    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None 
    ) -> str:
        
        entity_id_in_path = stream_slice.get(self.entity_name_in_path.rstrip("s") + "_id")
        self.logger.info(f"Sending request to account {stream_slice['ad_account_id']}, ad id {entity_id_in_path} with start_time {stream_slice['start_date']} and end_time {stream_slice['end_date']}")
        return f"{self.entity_name_in_path}/{entity_id_in_path}/stats"
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any] | None]:
        date_slice: List[dict] = super().stream_slices()
        for date in date_slice:
            for entity in self.entity_info:
                 entity.update(date)
        return self.entity_info
    
    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        
        stream_slice
        metrics  = ",".join(self.list_metrics)
        params = {
            "start_time": stream_slice["start_date"],
            "end_time": stream_slice["end_date"],
            "granularity": "DAY",
            "report_dimension": "country",
            "fields": f"{metrics}",
        }
        if next_page_token:
            params.update({"cursor": next_page_token})
        return params
    
    """ this API response follow this format 
    {
        "timeseries_stats":[{
            "timeseries_stat": {
                "timeseries": [{
                    {
                        "start_time": xxx,
                        "end_time": xxx,
                        "dimension_stats": []
                    }
                }]
            }
        }]
    }
    """
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        for data in response_json.get("timeseries_stats"):
            for record in data.get("timeseries_stat").get("timeseries"):
                result = {}
                if record.get("dimension_stats"):
                    result.update({
                        "start_time": record.get("start_time"),
                        "end_time": record.get("end_time"),

                    })
                    for item in record.get("dimension_stats"):
                        result.update(item)
                        yield result

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": {
                # dimensions
                "start_time": {"type": ["null", "string"]},
                "end_time": {"type": ["null", "string"]},
                "ad_id": {"type": ["null", "string"]},
                "ad_name": {"type": ["null", "string"]},
                "ad_squad_name": {"type": ["null", "string"]},
                "ad_squad_id": {"type": ["null", "string"]},
                "campaign_id": {"type": ["null", "string"]},
                "campaign_name": {"type": ["null", "string"]},
                "ad_account_id": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "campaignId": {"type": ["null", "number"]},
                "campaignName": {"type": ["null", "string"]},

                 # metrics
                "impressions":{"type":["null","number"]},
                "swipes":{"type":["null","number"]},
                "spend":{"type":["null","number"]},
                "total_impressions":{"type":["null","number"]},
                "total_installs":{"type":["null","number"]},
                "android_installs":{"type":["null","number"]},
                "ios_installs":{"type":["null","number"]},
                "screen_time_millis":{"type":["null","number"]},
                "paid_impressions":{"type":["null","number"]},
                "view_time_millis":{"type":["null","number"]},
                "quartile_1":{"type":["null","number"]},
                "quartile_2":{"type":["null","number"]},
                "quartile_3":{"type":["null","number"]},
                "view_completion":{"type":["null","number"]},
                "video_views":{"type":["null","number"]},
                "shares":{"type":["null","number"]},
                "saves":{"type":["null","number"]},
                "uniques":{"type":["null","number"]},
                "story_opens":{"type":["null","number"]},
                "story_completes":{"type":["null","number"]},
                "custom_event_1":{"type":["null","number"]},
                "custom_event_2":{"type":["null","number"]},
                "custom_event_3":{"type":["null","number"]},
                "custom_event_4":{"type":["null","number"]},
                "custom_event_5":{"type":["null","number"]},
                "attachment_uniques":{"type":["null","number"]},
                "attachment_view_completion":{"type":["null","number"]},
                "attachment_quartile_3":{"type":["null","number"]},
                "attachment_quartile_2":{"type":["null","number"]},
                "attachment_quartile_1":{"type":["null","number"]},
                "attachment_video_views":{"type":["null","number"]},
                "conversion_rate":{"type":["null","number"]},
                "conversion_save":{"type":["null","number"]},
                "conversion_login":{"type":["null","number"]},
                "conversion_share":{"type":["null","number"]},
                "conversion_invite":{"type":["null","number"]},
                "conversion_ad_view":{"type":["null","number"]},
                "conversion_reserve":{"type":["null","number"]},
                "conversion_ad_click":{"type":["null","number"]},
                "conversion_add_cart":{"type":["null","number"]},
                "conversion_searches":{"type":["null","number"]},
                "conversion_app_opens":{"type":["null","number"]},
                "conversion_list_view":{"type":["null","number"]},
                "attachment_total_view_time_millis":{"type":["null","number"]},
                "conversion_complete_tutorial":{"type":["null","number"]},
                "conversion_purchases_value":{"type":["null","number"]},
                "conversion_level_completes":{"type":["null","number"]},
                "conversion_add_to_wishlist":{"type":["null","number"]},
                "conversion_visit":{"type":["null","number"]},
                "conversion_start_checkout":{"type":["null","number"]},
                "conversion_spend_credits":{"type":["null","number"]},
                "conversion_view_content":{"type":["null","number"]},
                "conversion_start_trial":{"type":["null","number"]},
                "conversion_add_billing":{"type":["null","number"]},
                "conversion_page_views":{"type":["null","number"]},
                "conversion_subscribe":{"type":["null","number"]},
                "conversion_purchases":{"type":["null","number"]},
                "conversion_sign_ups":{"type":["null","number"]},
                "conversion_achievement_unlocked":{"type":["null","number"]},
            }
        }
        return full_schema
    
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
                if record:
                    record.update({"ad_name": stream_slice["ad_name"]})
                    record.update({"ad_id": stream_slice["ad_id"]})
                    record.update({"ad_squad_id": stream_slice["ad_squad_id"]})
                    record.update({"ad_squad_name": stream_slice["ad_squad_name"]})
                    record.update({"campaign_id": stream_slice["campaign_id"]})
                    record.update({"campaign_name": stream_slice["campaign_name"]})
                    record.update({"ad_account_id": stream_slice["ad_account_id"]})
                    yield record
        else:
            yield from records
    

# Source
class SourceSnapchatAdsCustom(AbstractSource):
    entity_list = ("ad_account","campaign","ad_squad","ad")
    entity_info = []

    def _get_ads_object(self, config) -> List[dict]:
        auth = SnapchatAdvertiseAuthenticator(config=config)

        # get account and campaign info
        campaign_info = []
        for id in config["ad_account_id"]:
                    check_connection_steam = SnapchatEntityUnderOneAdAccount(
                        authenticator = auth, 
                        config=config, 
                        entity_name_in_path= self.entity_list[0],
                        entity_id_in_path = id,
                        parent_entity_name= self.entity_list[0],
                        child_entity = self.entity_list[1]
                    ) 
                    check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
                    for record in check_connection_records:
                        campaign_info.append(record)
        
        # get campaign and adsquad info
        adsquad_info = []
        for id in config["ad_account_id"]:
                    check_connection_steam = SnapchatEntityUnderOneAdAccount(
                        authenticator = auth, 
                        config=config, 
                        entity_name_in_path= self.entity_list[0],
                        entity_id_in_path = id,
                        parent_entity_name= self.entity_list[1],
                        child_entity = self.entity_list[2]
                    ) 
                    check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
                    for record in check_connection_records:
                        adsquad_info.append(record)
        
        # get adsquad and ad info
        ad_info = []
        for id in config["ad_account_id"]:
                    check_connection_steam = SnapchatEntityUnderOneAdAccount(
                        authenticator = auth, 
                        config=config, 
                        entity_name_in_path= self.entity_list[0],
                        entity_id_in_path = id,
                        parent_entity_name= self.entity_list[2],
                        child_entity = self.entity_list[3]
                    ) 
                    check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
                    for record in check_connection_records:
                        ad_info.append(record)
        
        # list comprehension for update each ad_id with it"s campaign, adsquad and account infomation in below format
        # [{"ad_id": "xxx", "ad_name": "xxx", "ad_squad_id": "xxx", "ad_squad_name": "xxx", "campaign_id": "xxx", "campaign_name": "xxx", "ad_account_id": "xxx"}]

        [i.update(k) for i in adsquad_info for k in campaign_info if i["campaign_id"] == k["campaign_id"]]
        [i.update(k) for i in ad_info for k in adsquad_info if i["ad_squad_id"] == k["ad_squad_id"]]

        return ad_info


    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = SnapchatAdvertiseAuthenticator(config=config)
            logger.info(f"load auth {auth}")
            if len(config["ad_account_id"]) == 0:
                raise Exception("Please fill List Account ID in your config")
            else: 
                for id in config["ad_account_id"]:
                    check_connection_steam = SnapchatAdsCheckConnection(
                        authenticator = auth, 
                        config=config, 
                        entity_name_in_path= self.entity_list[0],
                        entity_id_in_path= id,
                    ) 
                    logger.info(f"Successfully build {check_connection_steam}")
                    check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
                    logger.info(f"Successfully read records {check_connection_records}")
                    record = next(check_connection_records)
                    logger.info(f" Check permission for Account ID {id} successfully")
                # print(self._get_ads_object(config))
                # print(len(self._get_ads_object(config)))
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = SnapchatAdvertiseAuthenticator(config=config)
        if not self.entity_info:
            self.entity_info = self._get_ads_object(config)

        streams = [SnapchatAdsAdStatsReport(
            authenticator = auth, 
            config=config, 
            entity_info = self.entity_info,
            entity_name_in_path= self.entity_list[3],
            entity_id_in_path= None,
        )]
        return streams
