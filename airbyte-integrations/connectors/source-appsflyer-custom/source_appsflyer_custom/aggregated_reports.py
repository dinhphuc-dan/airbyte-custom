from abc import ABC, abstractmethod
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


class AppsFlyerAggregatedReportsBase(AppsflyerCustomStream, IncrementalMixin, ABC):
    _cursor_value: date = None
    _custom_back_off = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.number_days_backward = self.config.get("number_days_backward", 2)
        self.chunk_date_range = self.config.get("chunk_date_range", 10)
        self.timezone = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
        # instead of return each app as a stream like raw reports, we will return all apps as one stream for aggreageted reports
        self.list_apps = self.config.get("list_apps")

    @property
    def name(self) -> str:
        stream_name = self.suffix
        return stream_name

    @property
    def availability_strategy(self):
        return None

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

    @property
    def state(self) -> Mapping[str, Any]:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value: date = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        # self.logger.info(f"Cursor Setter {self._cursor_value}")

    @abstractmethod
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """return the path for the stream"""

    def request_headers(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """
        Require by AppsFlyer
        """
        header = {"accept": "text/csv"}
        return header

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice: list = []

        # data_available_date is the date that the newest data can be accessed
        data_avaliable_date: date = pendulum.today(self.timezone).date()

        if self.get_last_X_days:
            """' this code for all kind of run, such as: the first time run or full refresh or incremental run, the stream will start with today date minus number_days_backward"""
            start_date: datetime.date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        elif stream_state:
            """this code for incremental run and get_last_X_days is false, the stream will start with the last date of stream state minus number_days_backward"""
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        else:
            """' this code for the first time run or full refresh run, the stream will start with the start date in config"""
            start_date: datetime.date = pendulum.parse(self.config["start_date"]).date()
            # self.logger.info(f"stream slice start date in ELSE {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        while start_date <= data_avaliable_date:
            start_date_as_str: str = start_date.to_date_string()
            if (data_avaliable_date - start_date).days >= self.chunk_date_range:
                end_date: date = start_date.add(days=self.chunk_date_range)
                end_date_as_str: str = end_date.to_date_string()
                for app in self.list_apps:
                    slice.append({"from": start_date_as_str, "to": end_date_as_str, "app_id": app})
            else:
                end_date: date = data_avaliable_date
                end_date_as_str: str = end_date.to_date_string()
                for app in self.list_apps:
                    slice.append({"from": start_date_as_str, "to": end_date_as_str, "app_id": app})
            start_date: date = end_date.add(days=1)

        return slice or [None]

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        self.logger.info(f"Slice in params {self.name} {stream_slice}")
        params = {
            "from": stream_slice["from"],
            "to": stream_slice["to"],
        }
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

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_as_string = response.content.decode("utf-8-sig")
        """
        First, we convert response text to string by decoding utf-8-sig
        Then, we covert response string to a file object using StringIO
        After that, we load file object to pandas frame and rename column
        Finally, we yield records by using to_dict() of pandas data frame
        """
        # load response to pandas data frame
        df = pd.read_csv(StringIO(response_as_string))
        # rename column
        df.rename(columns=lambda x: x.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", ""), inplace=True)
        # replace Nan value as None
        df.replace(np.nan, None, inplace=True)
        for record in df.to_dict(orient="records"):
            yield record

    @abstractmethod
    def get_json_schema(self) -> Mapping[str, Any]:
        """return the schema for stream"""

    def should_retry(self, response: requests.Response) -> bool:
        """
        this api endpoint is limited per 1-minute
        read more at https://support.appsflyer.com/hc/en-us/articles/207034366-Report-generation-quotas-rate-limitations
        """

        if response.status_code == 403:
            self._custom_back_off = True
            self._back_off_time = 60
            self._error_message = f"API reached limit. Pause for {self._back_off_time} second"
            return True
        else:
            return super().should_retry(response=response)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if self._custom_back_off:
            return self._back_off_time
        return super().backoff_time(response=response)

    def error_message(self, response: requests.Response) -> str:
        if self._custom_back_off:
            return self._error_message
        return super().error_message(response=response)


class AppsFlyerAggregatedDailyReport(AppsFlyerAggregatedReportsBase):
    suffix = "daily_report"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        app_id = stream_slice["app_id"]
        return f"agg-data/export/app/{app_id}/daily_report/v5"

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "agency_pmd_af_prt": {"type": ["null", "string"]},
                "media_source_pid": {"type": ["null", "string"]},
                "campaign_c": {"type": ["null", "string"]},
                "impressions": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]},
                "ctr": {"type": ["null", "number"]},
                "installs": {"type": ["null", "number"]},
                "conversion_rate": {"type": ["null", "number"]},
                "sessions": {"type": ["null", "number"]},
                "loyal_users": {"type": ["null", "number"]},
                "loyal_users_installs": {"type": ["null", "number"]},
                "total_cost": {"type": ["null", "number"]},
                "average_ecpi": {"type": ["null", "number"]},
            },
        }
        return full_schema


class AppsFlyerAggregatedGeoDailyReport(AppsFlyerAggregatedReportsBase):
    suffix = "geo_daily_report"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        app_id = stream_slice["app_id"]
        return f"agg-data/export/app/{app_id}/geo_by_date_report/v5"

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "agency_pmd_af_prt": {"type": ["null", "string"]},
                "media_source_pid": {"type": ["null", "string"]},
                "campaign_c": {"type": ["null", "string"]},
                "impressions": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]},
                "ctr": {"type": ["null", "number"]},
                "installs": {"type": ["null", "number"]},
                "conversion_rate": {"type": ["null", "number"]},
                "sessions": {"type": ["null", "number"]},
                "loyal_users": {"type": ["null", "number"]},
                "loyal_users_installs": {"type": ["null", "number"]},
                "total_cost": {"type": ["null", "number"]},
                "average_ecpi": {"type": ["null", "number"]},
                "total_revenue": {"type": ["null", "number"]},
                "roi": {"type": ["null", "number"]},
                "arpu": {"type": ["null", "number"]},
                "af_ad_revenue_unique_users": {"type": ["null", "number"]},
                "af_ad_revenue_event_counter": {"type": ["null", "number"]},
                "af_ad_revenue_sales_in_usd": {"type": ["null", "number"]},
            },
        }
        return full_schema


class AppsFlyerAggregatedPartnersDailyReport(AppsFlyerAggregatedReportsBase):
    suffix = "partners_daily_report"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        app_id = stream_slice["app_id"]
        return f"agg-data/export/app/{app_id}/partners_by_date_report/v5"

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "agency_pmd_af_prt": {"type": ["null", "string"]},
                "media_source_pid": {"type": ["null", "string"]},
                "campaign_c": {"type": ["null", "string"]},
                "impressions": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]},
                "ctr": {"type": ["null", "number"]},
                "installs": {"type": ["null", "number"]},
                "conversion_rate": {"type": ["null", "number"]},
                "sessions": {"type": ["null", "number"]},
                "loyal_users": {"type": ["null", "number"]},
                "loyal_users_installs": {"type": ["null", "number"]},
                "total_cost": {"type": ["null", "number"]},
                "average_ecpi": {"type": ["null", "number"]},
                "total_revenue": {"type": ["null", "number"]},
                "roi": {"type": ["null", "number"]},
                "arpu": {"type": ["null", "number"]},
                "af_ad_revenue_unique_users": {"type": ["null", "number"]},
                "af_ad_revenue_event_counter": {"type": ["null", "number"]},
                "af_ad_revenue_sales_in_usd": {"type": ["null", "number"]},
            },
        }
        return full_schema
