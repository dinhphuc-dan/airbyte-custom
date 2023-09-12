from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.models import SyncMode
import pendulum
from datetime import date, datetime
from source_appsflyer_custom.base_and_check_connection_stream import AppsflyerCustomStream
from io import StringIO
import pandas as pd
import numpy as np

class AppsFlyerIAPRawNonOrganic(AppsflyerCustomStream, IncrementalMixin):
    primary_key = None
    _cursor_value: date = None

    def __init__(self, app_id, app_name, **kwargs):
        super().__init__(**kwargs)
        self.app_id = app_id
        self.app_name = app_name
        self.number_days_backward = self.config.get("number_days_backward", 2)
        self.chunk_date_range = self.config.get("chunk_date_range", 10)
        self.timezone  = self.config.get("timezone", "UTC")

    @property
    def name(self) -> str:
        """Override method to get stream name according to each app name with suffix IAP Raw Non Organic """
        suffix = "_IAPRawNonOrganic"
        stream_name = self.app_id + suffix
        return stream_name
    
    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "event_time"
    
    @property
    def state(self) -> Mapping[str, Any]:
        '''airbyte always starts syncing by checking stream availability, then sets cursor value as your logic at read_records() fucntion''' 
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value: date = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        # self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"raw-data/export/app/{self.app_id}/in_app_events_report/v5?"
    
    def request_headers(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """
        Require by AppsFlyer
        """
        header = {"accept":"text/csv"}
        return header
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice: list = []
        
        if stream_state:
            ''' this code for incremental run, the stream will start with the last date of record minus number_days_backward'''
            start_date: date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")
            start_date_as_str: str = start_date.to_date_string()
            end_date_as_str: str = pendulum.today(tz=self.timezone).subtract(days=2).date().to_date_string()
            slice.append({
                "from": start_date_as_str,
                "to": end_date_as_str,
                "maximum_rows": 1000000,
                }
            )

        elif self._cursor_value: 
            '''' this code for the first time run or full refresh run, the stream will start with the start date in config'''
            start_date: date = pendulum.parse(self.config["start_date"]).date()
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

            while start_date <= pendulum.today().subtract(days=2).date():
                start_date_as_str: str = start_date.to_date_string()
                if (pendulum.today().subtract(days=2).date() - start_date).days >= self.chunk_date_range:
                    end_date: date = start_date.add(days=self.chunk_date_range)
                    end_date_as_str: str = end_date.to_date_string()
                    slice.append({
                        "from": start_date_as_str,
                        "to": end_date_as_str,
                        "maximum_rows": 1000000,
                        }
                    )
                else:
                    end_date: date = pendulum.today().subtract(days=2).date()
                    end_date_as_str: str = end_date.to_date_string()
                    slice.append({
                        "from": start_date_as_str,
                        "to": end_date_as_str,
                        "maximum_rows": 1000000,
                        }
                    )
                start_date: date = end_date.add(days=1)

        else:
            ''' this code for airbyte to checking stream availability. It will be run first then starting sync. In order to make this procees shorter, start date and end date is yesterday, and maximum_rows is 100'''  
            start_date: date = pendulum.today().subtract(days=2).date()
            # self.logger.info(f"stream slice start date in ELSE {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")
            start_date_as_str: str = start_date.to_date_string()
            slice.append({
                "from": start_date_as_str,
                "to": start_date_as_str,
                "maximum_rows": 100,
                }
            )

        return slice or [None]

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "additional_fields": "store_reinstall,impressions,contributor3_match_type,custom_dimension,conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,monetization_network,segment,gp_referrer,blocked_reason_value,device_category,app_type,rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att",
            "event_name": "af_purchase,af_purchase_canceled,af_purchase_pending,af_purchase_refund,af_ars_trial_started,af_ars_trial_canceled,af_ars_trial_churned,af_ars_trial_converted,af_ars_subscription_started,af_ars_subscription_canceled,af_ars_subscription_paused,af_ars_subscription_resumed,af_ars_subscription_churned,af_ars_subscription_refunded,af_ars_subscription_billing_grace,af_ars_subscription_renewed,af_ars_subscription_xgraded,af_ars_existing_subscriber",
        }
        self.logger.info(f"Slice in params {self.name} {stream_slice}")
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
            record_cursor_value: date = pendulum.parse(record[self.cursor_field]).date()
            self._cursor_value: date = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value 
            yield record
        
        # if there is no record backs, the cursor value will be None, so we update it as the start date in config
        if self._cursor_value == None:
                self._cursor_value: date = pendulum.parse((self.config["start_date"])).date()
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_as_string = response.content.decode('utf-8-sig')
        '''
        First, we convert response text to string by decoding utf-8-sig
        Then, we covert response string to a file object using StringIO
        After that, we load file object to pandas frame and rename column
        Finally, we yield records by using to_dict() of pandas data frame
        '''
        # load response to pandas data frame
        df = pd.read_csv(StringIO(response_as_string))
        # rename column
        df.rename(columns=lambda x: x.replace(' ','_').lower(), inplace=True)
        # replace Nan value as None
        df.replace(np.nan, None, inplace=True)
        for record in df.to_dict(orient='records'):
            yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "app_id": {"type": ["null", "string"]},
                "app_name": {"type": ["null", "string"]},
                "bundle_id": {"type": ["null", "string"]},
                "platform": {"type": ["null", "string"]},
                "attributed_touch_type": {"type": ["null", "string"]},
                "attributed_touch_time": {"type": ["null", "string"]},
                "install_time": {"type": ["null", "string"]},
                "event_time": {"type": ["null", "string"]},
                "event_name": {"type": ["null", "string"]},
                "event_value": {"type": ["null", "string"]},
                "event_source": {"type": ["null", "string"]},
                "conversion_type": {"type": ["null", "string"]},
                "event_revenue": {"type": ["null", "number"]},
                "event_revenue_currency": {"type": ["null", "string"]},
                "event_revenue_usd": {"type": ["null", "number"]},
                "impressions": {"type": ["null", "number"]},
                "mediation_network": {"type": ["null", "string"]},
                "monetization_network": {"type": ["null", "string"]},
                "ad_unit": {"type": ["null", "string"]},
                "placement": {"type": ["null", "string"]},
                "segment": {"type": ["null", "string"]},
                "network_account_id": {"type": ["null", "string"]},
                "cost_model": {"type": ["null", "string"]},
                "cost_value": {"type": ["null", "number"]},
                "cost_currency": {"type": ["null", "string"]},
                "partner": {"type": ["null", "string"]},
                "media_source": {"type": ["null", "string"]},
                "channel": {"type": ["null", "string"]},
                "keywords": {"type": ["null", "string"]},
                "campaign": {"type": ["null", "string"]},
                "campaign_type": {"type": ["null", "string"]},
                "campaign_id": {"type": ["null", "string"]},
                "adset": {"type": ["null", "string"]},
                "adset_id": {"type": ["null", "string"]},
                "ad": {"type": ["null", "string"]},
                "ad_id": {"type": ["null", "string"]},
                "ad_type": {"type": ["null", "string"]},
                "keyword_match_type": {"type": ["null", "string"]},
                "keyword_id": {"type": ["null", "string"]},
                "site_id": {"type": ["null", "string"]},
                "sub_site_id": {"type": ["null", "string"]},
                "sub_param_1": {"type": ["null", "string"]},
                "sub_param_2": {"type": ["null", "string"]},
                "sub_param_3": {"type": ["null", "string"]},
                "sub_param_4": {"type": ["null", "string"]},
                "sub_param_5": {"type": ["null", "string"]},
                "match_type": {"type": ["null", "string"]},
                "contributor_1_partner": {"type": ["null", "string"]},
                "contributor_1_media_source": {"type": ["null", "string"]},
                "contributor_1_campaign": {"type": ["null", "string"]},
                "contributor_1_touch_type": {"type": ["null", "string"]},
                "contributor_1_touch_time": {"type": ["null", "string"]},
                "contributor_1_match_type": {"type": ["null", "string"]},
                "contributor_2_partner": {"type": ["null", "string"]},
                "contributor_2_media_source": {"type": ["null", "string"]},
                "contributor_2_campaign": {"type": ["null", "string"]},
                "contributor_2_touch_type": {"type": ["null", "string"]},
                "contributor_2_touch_time": {"type": ["null", "string"]},
                "contributor_2_match_type": {"type": ["null", "string"]},
                "contributor_3_partner": {"type": ["null", "string"]},
                "contributor_3_media_source": {"type": ["null", "string"]},
                "contributor_3_campaign": {"type": ["null", "string"]},
                "contributor_3_touch_type": {"type": ["null", "string"]},
                "contributor_3_touch_time": {"type": ["null", "string"]},
                "contributor_3_match_type": {"type": ["null", "string"]},
                "attribution_lookback": {"type": ["null", "string"]},
                "is_primary_attribution": {"type": ["null", "string"]},
                "original_url": {"type": ["null", "string"]},
                "http_referrer": {"type": ["null", "string"]},
                "region": {"type": ["null", "string"]},
                "country_code": {"type": ["null", "string"]},
                "state": {"type": ["null", "string"]},
                "city": {"type": ["null", "string"]},
                "postal_code": {"type": ["null", "string"]},
                "dma": {"type": ["null", "string"]},
                "ip": {"type": ["null", "string"]},
                "wifi": {"type": ["null", "string"]},
                "operator": {"type": ["null", "string"]},
                "carrier": {"type": ["null", "string"]},
                "language": {"type": ["null", "string"]},
                "appsflyer_id": {"type": ["null", "string"]},
                "advertising_id": {"type": ["null", "string"]},
                "idfa": {"type": ["null", "string"]},
                "android_id": {"type": ["null", "string"]},
                "customer_user_id": {"type": ["null", "string"]},
                "imei": {"type": ["null", "string"]},
                "idfv": {"type": ["null", "string"]},
                "oaid": {"type": ["null", "string"]},
                "amazon_fire_id": {"type": ["null", "string"]},
                "device_category": {"type": ["null", "string"]},
                "device_model": {"type": ["null", "string"]},
                "device_type": {"type": ["null", "string"]},
                "user_agent": {"type": ["null", "string"]},
                "os_version": {"type": ["null", "string"]},
                "app_version": {"type": ["null", "string"]},
                "sdk_version": {"type": ["null", "string"]},
                "is_receipt_validated": {"type": ["null", "string"]},
                "att": {"type": ["null", "string"]},               
                "is_retargeting": {"type": ["null", "string"]},
                "retargeting_conversion_type": {"type": ["null", "string"]},
                "reengagement_window": {"type": ["null", "string"]},
                "install_app_store": {"type": ["null", "string"]},
                "store_reinstall": {"type": ["null", "string"]},
                "google_play_click_time": {"type": ["null", "string"]},
                "google_play_broadcast_referrer": {"type": ["null", "string"]},
                "google_play_install_begin_time": {"type": ["null", "string"]},
                "google_play_referrer": {"type": ["null", "string"]},
                "device_download_time": {"type": ["null", "string"]},
                "deeplink_url": {"type": ["null", "string"]},
                "blocked_reason": {"type": ["null", "string"]},
                "blocked_sub_reason": {"type": ["null", "string"]},
                "blocked_reason_value": {"type": ["null", "string"]},
                "rejected_reason": {"type": ["null", "string"]},
                "rejected_reason_value": {"type": ["null", "string"]},
                "custom_data": {"type": ["null", "string"]},  
                "custom_dimension": {"type": ["null", "string"]},  
                "app_type": {"type": ["null", "string"]},
            }
        }
        return full_schema