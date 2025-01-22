#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
import pendulum
import datetime
from source_mintegral_ads.authenticator import MintegralTokenAuthenticator
import time
import pandas as pd
import numpy as np
from io import StringIO


# Basic full refresh stream
class MintegralAdsStream(HttpStream, IncrementalMixin, ABC):
    primary_key = None
    _cursor_value = None
    url_base = "https://ss-api.mintegral.com/api/"
    chunk_date_range = 6

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.number_days_backward: int = self.config.get("number_days_backward", 7)
        self.timezone: str = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
        self.utc_offset: str = str(self.config.get("utc_offset", 0))

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_json = response.json()
        if response_json["code"] == 10000:
            raise requests.HTTPError(response_json["data"])
        else:
            yield response_json

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

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []

        # data_available_date is the date that the newest data can be accessed
        data_avaliable_date: datetime.date = pendulum.today(self.timezone).date()

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
                end_date: datetime.date = start_date.add(days=self.chunk_date_range)
                end_date_as_str: str = end_date.to_date_string()
                slice.append(
                    {
                        "start_date": start_date_as_str,
                        "end_date": end_date_as_str,
                    }
                )
            else:
                end_date: datetime.date = data_avaliable_date
                end_date_as_str: str = end_date.to_date_string()
                slice.append(
                    {
                        "start_date": start_date_as_str,
                        "end_date": end_date_as_str,
                    }
                )
            start_date: datetime.date = end_date.add(days=1)

        # self.logger.info(f"slice {slice}")
        return slice or [None]


class MintegralCheckConnection(MintegralAdsStream):
    max_limit_per_page = 50

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "open/v1/campaign"

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name"""
        stream_name = "Campaign_Info"
        return stream_name

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return []

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        total = response_json["data"]["total"]
        limit = response_json["data"]["limit"]
        page = response_json["data"]["page"]
        if page * limit > total:
            return None
        else:
            return page

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {}
        params.update({"limit": self.max_limit_per_page, "page": 1})
        if next_page_token:
            next_page = next_page_token + 1
            params.update({"page": next_page})
        # self.logger.info(f"check campaign, params as  {params}")
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        for record in response_json["data"]["list"]:
            list_campaign_as_dict = {}
            list_campaign_as_dict.update({"campaign_id": record["campaign_id"], "campaign_name": record["campaign_name"]})
            yield list_campaign_as_dict

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "campaign_id": {"type": ["null", "number"]},
                "campaign_name": {"type": ["null", "string"]},
            },
        }
        return full_schema


class MintegralAdsReport(MintegralAdsStream):

    def __init__(self, list_campaign_as_dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._list_campaign_as_dict: list = list_campaign_as_dict

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v1/reports/data"

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name"""
        stream_name = "Campaign_Offer_Country"
        return stream_name

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        date_slices = super().stream_slices(stream_state=stream_state, **kwargs)
        for date_slice in date_slices:
            for campaign in self._list_campaign_as_dict:
                slice.append(
                    {
                        "start_date": date_slice["start_date"],
                        "end_date": date_slice["end_date"],
                        "campaign_id": str(campaign["campaign_id"]),
                        "campaign_name": str(campaign["campaign_name"]),
                    }
                )
        self.logger.info(f"number slice {len(slice)}")
        return slice

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        request_params.update(
            {
                "start_date": stream_slice["start_date"],
                "end_date": stream_slice["end_date"],
                "campaign_id": stream_slice["campaign_id"],
                "utc": self.utc_offset,
                "dimension": "location",
                "not_empty_field": "click,install,impression,spend",
            }
        )
        self.logger.info(f" Request Params {stream_slice['start_date'], stream_slice['end_date'], stream_slice['campaign_id']}")
        return request_params

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        for record in records:
            record_cursor_value = pendulum.parse(record[self.cursor_field]).date()
            self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record with ELSE, record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            if record is not None:
                record.update({"campaign_id": stream_slice["campaign_id"]})
                record.update({"campaign_name": stream_slice["campaign_name"]})
            yield record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        # yield response_json
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        if response_json["data"] is None:
            yield {}
        else:
            for item in response_json["data"]:
                record = {}
                record.update({"date": item["date"]})
                record.update({"package_name": item["package_name"]})
                record.update({"platform": item["platform"]})
                record.update({"location": item["location"]})
                record.update({"geo": str(item["geo"])})
                record.update({"currency": item["currency"]})
                record.update({"utc": item["utc"]})
                record.update({"offer_id": item["offer_id"]})
                record.update({"offer_name": item["offer_name"]})
                record.update({"impression": item["impression"]})
                record.update({"click": item["click"]})
                record.update({"install": item["install"]})
                record.update({"spend": item["spend"]})
                yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "package_name": {"type": ["null", "string"]},
                "platform": {"type": ["null", "string"]},
                "location": {"type": ["null", "string"]},
                "geo": {"type": ["null", "string"]},
                "currency": {"type": ["null", "string"]},
                "utc": {"type": ["null", "string"]},
                "campaign_id": {"type": ["null", "string"]},
                "campaign_name": {"type": ["null", "string"]},
                "offer_id": {"type": ["null", "string"]},
                "offer_name": {"type": ["null", "string"]},
                "impression": {"type": ["null", "number"]},
                "click": {"type": ["null", "number"]},
                "install": {"type": ["null", "number"]},
                "spend": {"type": ["null", "number"]},
            },
        }
        return full_schema


class MintegralCustomReportRegisterJob(MintegralAdsStream):

    def __init__(self, request_slice_date_range: dict = None, request_custom_params: dict = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_report_dimension: list = self.config.get("custom_report_dimensions")
        self.custom_report_time_granularity: list = self.config.get("custom_report_time_granularity", ["daily"])
        self._request_slice_date_range: dict = request_slice_date_range
        self._request_custom_params: dict = request_custom_params

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v2/reports/data"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        request_params = {}
        if self._request_slice_date_range:
            request_params.update(
                {
                    "start_time": self._request_slice_date_range["start_date"],
                    "end_time": self._request_slice_date_range["end_date"],
                    "utc": self.utc_offset,
                    "dimension_option": ",".join(self.custom_report_dimension),
                    "time_granularity": self.custom_report_time_granularity[0],
                    "type": 1,
                }
            )
        elif self._request_custom_params:
            request_params = self._request_custom_params
        self.logger.debug(request_params)
        return request_params


class MintegralCustomReportStream(MintegralAdsStream):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v2/reports/data"

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name"""
        stream_name = "Custom_Report"
        return stream_name

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any] | None]:
        """
        First, we register job to server
        Then, we check the status of job. If the response field code is 202, then we mark it as completed
        Return:
            - list of all completed jobs
        """
        try:
            list_registed_jobs: list[dict] = self._send_create_job_request(stream_state=stream_state, **kwargs)
            number_of_jobs: int = len(list_registed_jobs)
            self.logger.info(f"Successfully create {number_of_jobs} jobs")
            list_completed_jobs: list[dict] = self._handle_job_request_status(list_registed_jobs=list_registed_jobs)
            return list_completed_jobs
        except Exception as e:
            self.logger.info(f"Something went wrong {e}")
            return [None]

    def _send_create_job_request(self, stream_state: Mapping[str, Any] = None, **kwargs) -> list[dict]:
        """
        we produce slices base on stream condition such as stream_state, sync_mode
        then we pass those slices to MintegralCustomReportRegisterJob to create jobs
        a job contains job_staus and job_params, ex
        {
            "job_status": 200,
            "job_params": {'start_time': '2025-01-01', 'end_time': '2025-01-07', 'utc': '7', 'dimension': 'Creative,Package', 'time_granularity': 'daily', 'type': 1},
            "job_id": "2025-01-012025-01-077Creative,Packagedaily"
        }
        """
        list_registed_jobs: list[dict] = []
        date_range_slices: list[dict] = super().stream_slices(stream_state, **kwargs)
        for slice in date_range_slices:
            job_instance = MintegralCustomReportRegisterJob(
                request_slice_date_range=slice, config=self.config, authenticator=self._session.auth
            )
            # update job info to slice
            job_params = job_instance.request_params(stream_state=stream_state)
            job_status = next(job_instance.read_records(sync_mode="full_refresh"))
            list_registed_jobs.append(
                {
                    "job_status": job_status["code"],
                    "job_params": job_params,
                    "job_id": str(job_params["start_time"])
                    + str(job_params["end_time"])
                    + str(job_params["dimension_option"])
                    + str(job_params["time_granularity"]),
                }
            )
        return list_registed_jobs

    def _handle_job_request_status(self, list_registed_jobs: list[dict]) -> list[dict]:
        """
        This function check the status of job ID. Job will be check status until it's status reaches as 'Job Completed', then we pass those completed job to the MintegralCustomReportStream stream_slices to pull data
        Job status can be one of 200, 201, 202
            201: Job are prepared for generating
            202: Job are generating
            200: Job Completed
        More details can be found: https://adv-new.mintegral.com/doc/en/guide/report/advancedPerformanceReport.html
        Return:
            - list of completed jobs
            Ex: [
                {
                    "job_status": 200,
                    "job_params": {'start_time': '2025-01-01', 'end_time': '2025-01-07', 'utc': '7', 'dimension': 'Creative,Package', 'time_granularity': 'daily', 'type': 1},
                    "job_id": "2025-01-012025-01-077Creative,Packagedaily"
                },
                {
                    "job_status": 200,
                    "job_params": {'start_time': '2025-01-08', 'end_time': '2025-01-014', 'utc': '7', 'dimension': 'Creative,Package', 'time_granularity': 'daily', 'type': 1},
                    "job_id": "2025-01-082025-01-147Creative,Packagedaily"
                }
            ]
        """
        list_completed_jobs_as_set: set = set()
        all_job_completed = False

        while not all_job_completed:
            for job in list_registed_jobs:
                self.logger.debug(job)
                if job["job_status"] == 200:
                    list_completed_jobs_as_set.add(job["job_id"])
                else:
                    new_job_status = next(
                        MintegralCustomReportRegisterJob(
                            request_custom_params=job["job_params"], config=self.config, authenticator=self._session.auth
                        ).read_records(sync_mode="full_refresh")
                    )
                    self.logger.debug(new_job_status)
                    job["job_status"] = new_job_status["code"]
                    # we wait 5 seconds then check again
                    time.sleep(3)
            self.logger.info(f"Total jobs: {len(list_registed_jobs)}, Completed jobs {len(list_completed_jobs_as_set)}")
            if len(list_completed_jobs_as_set) == len(list_registed_jobs):
                all_job_completed = True
        return list_registed_jobs

    def request_params(self, stream_state, stream_slice=None, next_page_token=None):
        param = stream_slice["job_params"]
        # update to type 2 to pull record
        param["type"] = 2
        self.logger.info(f"Request Params {param}")
        return param

    def parse_response(self, response, **kwargs):
        """
        First, we create a string to a file object using StringIO
        After that, we load file object to pandas frame and rename column
        Finally, we yield records by using to_dict() of pandas data frame
        """
        df = pd.read_csv(StringIO(response.text), sep="\t")
        df.rename(columns=lambda x: x.replace(" ", "_").lower(), inplace=True)
        df.replace([np.nan, np.inf, -np.inf], None, inplace=True)
        for record in df.to_dict(orient="records"):
            yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        dynamic_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": {
                "date": {"type": ["null", "number"]},
                "timestamp": {"type": ["null", "number"]},
                "currency": {"type": ["null", "string"]},
                "impression": {"type": ["null", "number"]},
                "click": {"type": ["null", "number"]},
                "conversion": {"type": ["null", "number"]},
                "spend": {"type": ["null", "number"]},
                "ecpm": {"type": ["null", "number"]},
                "cpc": {"type": ["null", "number"]},
                "ctr": {"type": ["null", "number"]},
                "cvr": {"type": ["null", "number"]},
                "ivr": {"type": ["null", "number"]},
            },
        }

        dynamic_dimension = [
            {
                "Offer": {
                    "offer_id": {"type": ["null", "number"]},
                    "offer_uuid": {"type": ["null", "string"]},
                    "offer_name": {"type": ["null", "string"]},
                }
            },
            {
                "Campaign": {
                    "campaign_id": {"type": ["null", "number"]},
                }
            },
            {
                "CampaignPackage": {
                    "campaign_package": {"type": ["null", "string"]},
                }
            },
            {
                "Creative": {
                    "creative_id": {"type": ["null", "number"]},
                    "creative_name": {"type": ["null", "string"]},
                }
            },
            {
                "AdType": {
                    "ad_type": {"type": ["null", "string"]},
                }
            },
            {
                "Sub": {
                    "sub_id": {"type": ["null", "string"]},
                }
            },
            {
                "Package": {
                    "package_name": {"type": ["null", "string"]},
                }
            },
            {
                "Location": {
                    "location": {"type": ["null", "string"]},
                }
            },
            {
                "Endcard": {
                    "endcard_id": {"type": ["null", "number"]},
                    "endcard_name": {"type": ["null", "string"]},
                }
            },
            {
                "AdOutputType": {
                    "ad_output_type": {"type": ["null", "string"]},
                }
            },
        ]
        for each_dimension in dynamic_dimension:
            if list(each_dimension.keys())[0] in self.config.get("custom_report_dimensions"):
                dynamic_schema["properties"].update(list(each_dimension.values())[0])
        return dynamic_schema


# Source
class SourceMintegralAds(AbstractSource):
    def _get_campaign_list(self, config) -> list[dict]:
        auth = MintegralTokenAuthenticator(config=config)
        campaign_list_generator = MintegralCheckConnection(authenticator=auth, config=config)
        read_records_generator = campaign_list_generator.read_records(sync_mode="full_refresh")
        list_campaign_as_dict = []
        list_campaign_as_dict.extend(read_records_generator)
        return list_campaign_as_dict

    def _check_custom_report_dimensions(self, logger, config) -> tuple[bool, str | None]:
        """
        Return
            True, None or Raise Error
        """
        try:
            auth = MintegralTokenAuthenticator(config=config)
            date_range: dict = {
                "start_date": pendulum.today().subtract(days=1).date().to_date_string(),
                "end_date": pendulum.today().subtract(days=1).date().to_date_string(),
            }
            register_status = next(
                MintegralCustomReportRegisterJob(authenticator=auth, config=config, request_slice_date_range=date_range).read_records(
                    sync_mode="full_refresh"
                )
            )
            logger.info(register_status)
            return True, None
        except Exception as e:
            return False, e

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            check_campaign_list = self._get_campaign_list(config=config)
            if not check_campaign_list:
                raise Exception("Campaign list is empty")

            if config.get("custom_report_dimensions"):
                check_custom_report, error = self._check_custom_report_dimensions(logger=logger, config=config)
                if not check_custom_report:
                    raise Exception(error)

            return True, None
        except Exception as e:
            return (False, e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = MintegralTokenAuthenticator(config=config)
        campaign_list = self._get_campaign_list(config=config)
        streams = [MintegralAdsReport(authenticator=auth, config=config, list_campaign_as_dict=campaign_list)]
        if config.get("custom_report_dimensions"):
            streams.extend(
                [
                    MintegralCustomReportStream(authenticator=auth, config=config),
                    MintegralCheckConnection(authenticator=auth, config=config),
                ]
            )
        return streams
