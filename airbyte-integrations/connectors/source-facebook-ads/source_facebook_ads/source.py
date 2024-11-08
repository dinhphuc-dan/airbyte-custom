#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC, ABCMeta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.models import SyncMode
import pendulum
import datetime
import json
import time


# Basic full refresh stream
class FacebookAdsStream(HttpStream, IncrementalMixin, ABC):
    url_base = "https://graph.facebook.com/"
    _cursor_value = None
    primary_key = None
    custom_backoff = False

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config: dict = config
        self.number_days_backward: int = self.config.get("number_days_backward", 7)
        self.timezone: str = self.config.get("timezone", "UTC")
        self.get_last_X_days: int = self.config.get("get_last_X_days", False)
        self.api_version = self.config.get("api_version")

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date_start"

    @property
    def state(self) -> Mapping[str, Any]:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response = response.json()
        if (
            "paging" in response
            and "cursors" in response["paging"]
            and "after" in response["paging"]["cursors"]
            and
            # 'after' will always exist even if no more pages are available
            "next" in response["paging"]
        ):
            cursor_param_value = response["paging"]["cursors"]["after"]
            return cursor_param_value
        else:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {"access_token": self.config["access_token"]}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        response_json = response.json()
        yield response_json

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice: list = []

        # data_available_date is the date that the newest data can be accessed
        data_avaliable_date: datetime.date = pendulum.today(self.timezone).date()

        if self.get_last_X_days:
            """ " this code for all kind of run, such as: the first time run or full refresh or incremental run, the stream will start with today date minus number_days_backward"""
            start_date: datetime.date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        elif stream_state:
            """this code for incremental run and get_last_X_days is false, the stream will start with the last date of stream state minus number_days_backward"""
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        else:
            """ " this code for the first time run or full refresh run, the stream will start with the start date in config"""
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
                for id in self.config["ad_account_id"]:
                    slice.append({"time_range[since]": start_date_as_str, "time_range[until]": end_date_as_str, "ad_account_id": id})
            else:
                end_date_as_str: str = start_date.end_of("month").to_date_string()
                for id in self.config["ad_account_id"]:
                    slice.append({"time_range[since]": start_date_as_str, "time_range[until]": end_date_as_str, "ad_account_id": id})
            start_date: datetime.date = start_date.add(months=1).start_of("month")

        # self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def should_retry(self, response: requests.Response) -> bool:
        """
        when they see too many requests, this api can
        - get 400 error with sub code as 100
        - get 500 error with sub code as 2
        - get 400 error with sub code as 80000, 80003, 80004, 80014
        """
        if (
            response.status_code == 400
            and "error" in response.json()
            and response.json()["error"]["code"] == 100
            and "error_subcode" not in response.json()["error"]
        ):
            self._back_off_time = 10
            self._error_message = f"API reached limit with error 400, sub code 100. Pause for {self._back_off_time} second"
            self.custom_backoff = True
            return True
        elif response.status_code == 500 and "error" in response.json() and response.json()["error"]["code"] == 2:
            self._back_off_time = 60
            self._error_message = f"API reached limit with error 500, sub code 2. Pause for {self._back_off_time} second"
            self.custom_backoff = True
            return True
        elif (
            response.status_code == 400 and "error" in response.json() and response.json()["error"]["code"] in (80000, 80003, 80004, 80014)
        ):
            self._back_off_time = 10
            self._error_message = (
                f"API reached limit with error 400, sub code {response.json()['error']['code']}. Pause for {self._back_off_time} second"
            )
            self.custom_backoff = True
            return True
        else:
            return super().should_retry(response=response)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if self.custom_backoff:
            return self._back_off_time
        else:
            return super().backoff_time(response=response)

    def error_message(self, response: requests.Response) -> str:
        if self.custom_backoff:
            return self._error_message
        else:
            return super().error_message(response=response)

    @property
    def max_retries(self) -> Union[int, None]:
        """
        Override if needed. Specifies maximum amount of retries for backoff policy. Return None for no limit.
        """
        return None

    @property
    def max_time(self) -> Union[int, None]:
        """
        Override if needed. Specifies maximum total waiting time (in seconds) for backoff policy. Return None for no limit.
        """
        return None


class FacebookAdsCheckConnection(FacebookAdsStream):
    def __init__(self, account_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.account_id = account_id

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.api_version}/act_{self.account_id}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()


# Synchronous Stream
class FacebookAdsSynchronousStream(FacebookAdsStream):
    fields = [
        "ad_id",
        "ad_name",
        "adset_id",
        "adset_name",
        "campaign_id",
        "campaign_name",
        "account_name",
        "account_id",
        "impressions",
        "spend",
        "clicks",
        "reach",
        "actions",
        "objective",
        "buying_type",
        "created_time",
        "updated_time",
        "unique_clicks",
        "unique_actions",
        "outbound_clicks",
        "account_currency",
        "optimization_goal",
        "inline_link_clicks",
        "inline_post_engagement",
        "unique_outbound_clicks",
        "unique_inline_link_clicks",
    ]
    # time_increment = 1 means each record will be 1 day
    time_increment = 1
    level = "ad"
    breakdowns = "country"

    @property
    def name(self) -> str:
        """Override method to get stream name according to each package name"""
        stream_name = "ads_insights_country"
        return stream_name

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        account_id = stream_slice["ad_account_id"]
        return f"{self.api_version}/act_{account_id}/insights"

    def request_params(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params: dict = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update(
            {
                "level": self.level,
                "fields": (",").join(self.fields),
                "time_increment": self.time_increment,
                "sort": "date_start_ascending",
                "breakdowns": self.breakdowns,
            }
        )
        if stream_slice:
            params.update(stream_slice)
            self.logger.info(f"Job info: {stream_slice}")
        if next_page_token:
            params.update({"after": next_page_token})

        return params

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        for record in records:
            record_cursor_value: datetime.date = pendulum.parse(record[self.cursor_field]).date()
            self._cursor_value: datetime.date = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "ad_id": {"type": ["null", "string"]},
                "reach": {"type": ["null", "integer"]},
                "spend": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "integer"]},
                "actions": {"type": ["null", "array"], "items": {"type": "object"}},
                "ad_name": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "adset_id": {"type": ["null", "string"]},
                "date_stop": {"type": ["null", "string"], "format": "date"},
                "objective": {"type": ["null", "string"]},
                "account_id": {"type": ["null", "string"]},
                "adset_name": {"type": ["null", "string"]},
                "date_start": {"type": ["null", "string"], "format": "date"},
                "buying_type": {"type": ["null", "string"]},
                "campaign_id": {"type": ["null", "string"]},
                "impressions": {"type": ["null", "integer"]},
                "account_name": {"type": ["null", "string"]},
                "created_time": {"type": ["null", "string"], "format": "date"},
                "updated_time": {"type": ["null", "string"], "format": "date"},
                "campaign_name": {"type": ["null", "string"]},
                "unique_clicks": {"type": ["null", "integer"]},
                "unique_actions": {"type": ["null", "array"], "items": {"type": "object"}},
                "outbound_clicks": {"type": ["null", "array"], "items": {"type": "object"}},
                "account_currency": {"type": ["null", "string"]},
                "optimization_goal": {"type": ["null", "string"]},
                "inline_link_clicks": {"type": ["null", "integer"]},
                "inline_post_engagement": {"type": ["null", "integer"]},
                "unique_outbound_clicks": {"type": ["null", "array"], "items": {"type": "object"}},
                "unique_inline_link_clicks": {"type": ["null", "integer"]},
            },
        }
        return full_schema

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        if "data" in response_json:
            for record in response_json["data"]:
                yield record


class FacebookAdsAsynchronousCreateJob(FacebookAdsStream):
    """
    This class register job to Facebook Ads API, then get the job ID
    """

    def __init__(self, request_slice: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._request_slice = request_slice

    @property
    def http_method(self) -> str:
        return "POST"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        account_id = self._request_slice["ad_account_id"]
        return f"{self.api_version}/act_{account_id}/insights"

    def request_params(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {}
        params.update(self._request_slice)
        # remove account id from params since we don't need it
        params.pop("ad_account_id")
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # we add a uuid to each jobs so that we can retry failed jobs
        record = {
            "job_id": response.json().get("report_run_id"),
            "job_account_id_and_time_range": (
                str(self._request_slice["ad_account_id"])
                + ", "
                + str(self._request_slice["time_range[since]"])
                + ", "
                + str(self._request_slice["time_range[until]"])
            ),
            "x_fb_ads_insights_throttle": json.loads(response.headers.get("x-fb-ads-insights-throttle")),
            "x_business_use_case_usage": json.loads(response.headers.get("x-business-use-case-usage")),
        }
        yield record


class FacebookAdsAsynchronousCheckJobStatus(FacebookAdsAsynchronousCreateJob):
    """
    This class check job status based on job ID
    """

    @property
    def http_method(self) -> str:
        return "GET"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        job_id = self._request_slice["job_info"]["job_id"]
        return f"{self.api_version}/{job_id}"

    def request_params(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = FacebookAdsStream.request_params(self, stream_state, stream_slice, next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        record = {"x_business_use_case_usage": json.loads(response.headers.get("x-business-use-case-usage")), **response.json()}
        yield record


class FacebookAdsAsynchronousStream(FacebookAdsSynchronousStream):
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        job_id = stream_slice
        return f"{self.api_version}/{job_id}/insights"

    def request_params(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = FacebookAdsStream.request_params(self, stream_state, stream_slice, next_page_token)
        if next_page_token:
            params.update({"after": next_page_token})
        self.logger.debug(f"Get data from job {stream_slice}, params: {params.get('after')}")
        return params

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any] | None]:
        try:
            list_jobs_plus_info = self._send_create_job_request(stream_state=stream_state, **kwargs)
            number_of_jobs = len(list_jobs_plus_info)
            self.logger.info(f"Successfully create {number_of_jobs} jobs")
            list_completed_jobs = self._handle_job_request_status(list_jobs=list_jobs_plus_info, **kwargs)
            return list_completed_jobs
        except Exception as e:
            self.logger.info(f"Something went wrong {e}")
            return [None]

    def _send_create_job_request(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any] | None]:
        """
        we produce slices base on stream condition such as stream_state, sync_mode
        then we pass those slices to FacebookAdsAsynchronousCreateJob to create jobs
        """
        slices = super().stream_slices(stream_state, **kwargs)
        list_jobs_plus_info = []
        for slice in slices:
            # update slice with request params then pass to FacebookAdsAsynchronousCreateJob
            slice.update(super().request_params())
            # create job
            job_id_plus_info = next(
                FacebookAdsAsynchronousCreateJob(request_slice=slice, config=self.config).read_records(sync_mode="full_refresh")
            )
            self.logger.info(f"Successfully create job {job_id_plus_info}")
            slice.update({"job_info": job_id_plus_info})
            list_jobs_plus_info.append(slice)
        return list_jobs_plus_info

    def _handle_job_request_status(self, list_jobs, **kwargs) -> Iterable[Mapping[str, Any] | None]:
        """
        This function check the status of job ID.
        Job status can be one of 'Job Not Started', 'Job Started', 'Job Running', 'Job Completed', 'Job Failed', 'Job Skipped'
        More details can be found: https://developers.facebook.com/docs/marketing-api/insights/best-practices
        In case the status is 'Job Failed' or 'Job Skipped', we will try to re-summit the query and get new job ID.
        Otherwise, the job ID will be check status until it's status reaches as 'Job Completed', then we pass those completed job to the FacebookAdsAsynchronousStream stream_slices to pull data
        """
        list_completed_jobs_as_set = set()
        list_retry_jobs_id_as_set = set()
        all_job_completed = False

        while not all_job_completed:
            for job in list_jobs:
                if job["job_info"]["job_id"] not in list_completed_jobs_as_set:
                    job_status = next(
                        FacebookAdsAsynchronousCheckJobStatus(request_slice=job, config=self.config).read_records(sync_mode="full_refresh")
                    )
                    self.logger.debug(
                        f"Job id {job_status['id']}, pecent: {job_status['async_percent_completion']}%, staus: {job_status['async_status']}, limit: {job_status['x_business_use_case_usage']} "
                    )
                    if job_status["async_status"] in ("Job Failed", "Job Skipped"):
                        list_retry_jobs_id_as_set.add(job_status["id"])
                    elif job_status["async_status"] == "Job Completed":
                        list_completed_jobs_as_set.add(job_status["id"])
                    else:
                        # we wait 5 seconds then check again
                        time.sleep(5)
            self.logger.info(
                f"Total jobs: {len(list_jobs)}, Failed jobs {len(list_retry_jobs_id_as_set)}, Completed jobs {len(list_completed_jobs_as_set)}"
            )
            if len(list_retry_jobs_id_as_set) > 0:
                self._resumit_failed_jobs(list_retry_jobs_id_as_set=list_retry_jobs_id_as_set, list_jobs=list_jobs)
            elif len(list_completed_jobs_as_set) == len(list_jobs):
                all_job_completed = True

        return list(list_completed_jobs_as_set)

    def _resumit_failed_jobs(self, list_retry_jobs_id_as_set, list_jobs) -> Iterable[Mapping[str, Any] | None]:
        """
        Based on list_retry_jobs_id_as_set, we get their request params and create new job id
        Then we replace the old job info with the new job info
        """
        for job in list_jobs:
            for id in list_retry_jobs_id_as_set:
                if job["job_info"]["job_id"] == id:
                    # remove old job info
                    job.pop("job_info")
                    new_job_id_plus_info = next(
                        FacebookAdsAsynchronousCreateJob(request_slice=job, config=self.config).read_records(sync_mode="full_refresh")
                    )
                    # update new job info
                    job.update({"job_info": new_job_id_plus_info})
        # reset list_retry_jobs_id_as_set which means we resumitted all failed jobs
        list_retry_jobs_id_as_set.clear()
        return list_retry_jobs_id_as_set


# Source
class SourceFacebookAds(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            if len(config["ad_account_id"]) == 0:
                raise Exception("Please fill List Account ID in your config")
            else:
                for id in config["ad_account_id"]:
                    check_connection_steam = FacebookAdsCheckConnection(config=config, account_id=id)
                    # logger.info(f"Successfully build {check_connection_steam}")
                    check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
                    # logger.info(f"Successfully read records {check_connection_records}")
                    record = next(check_connection_records)
                    logger.info(f" Check permission for Account ID {id} successfully")
            return True, None
        except Exception as e:
            logger.info(f"Something went wrong with Account ID {id}")
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        if config.get("sync_type") == "synchronous requests as GET":
            streams = [FacebookAdsSynchronousStream(config=config)]
        elif config.get("sync_type") == "asynchronous requests as POST":
            streams = [FacebookAdsAsynchronousStream(config=config)]
        return streams
