#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pendulum
import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from pendulum import DateTime


class ExchangeRates(HttpStream):

    date_field_name = "date"

    # HttpStream related fields
    url_base = "https://api.apilayer.com/exchangerates_data/"
    cursor_field = date_field_name
    primary_key = None

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base = config.get("base", "USD")
        self._start_date = config["start_date"]
        self.access_key = config["access_key"]
        self.number_days_backward = config.get("number_days_backward", 2)
        self.timezone  = config.get("timezone", "UTC")
        self.get_last_X_days = config.get("get_last_X_days", False)
    
    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        self.logger.info(f"Request Path {stream_slice[self.date_field_name]}")
        return stream_slice[self.date_field_name]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(self, **kwargs) -> MutableMapping[str, Any]:
        params = {}

        if self._base is not None:
            params["base"] = self._base

        return params

    def request_headers(self, **kwargs) -> MutableMapping[str, Any]:
        headers = {"apikey": self.access_key}

        return headers

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield response_json

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_state = stream_state or {}
        # self.logger.info(f"Stream Slice Stream State {stream_state }")
        if self.get_last_X_days is True:
            start_date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
        else: 
            start_date = pendulum.parse(stream_state.get(self.date_field_name, self._start_date)).subtract(days=1).date()
        # self.logger.info(f"Stream Slice {start_date}")
        return self.chunk_date_range(start_date)

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]):
        current_stream_state = current_stream_state or {}
        # self.logger.info(f"Cursor Stream State before if {current_stream_state}")
        if latest_record[self.date_field_name] == current_stream_state.get(self.date_field_name, self._start_date):
            current_stream_state[self.date_field_name] = (pendulum.parse(latest_record[self.date_field_name]).add(days=1)).to_date_string()
            # self.logger.info(f"Cursor Stream State with if {current_stream_state}")
        else:
            current_stream_state[self.date_field_name] = max(
                latest_record[self.date_field_name], current_stream_state.get(self.date_field_name, self._start_date)
            )
            # self.logger.info(f"Cursor Stream State with else {current_stream_state}")
        return current_stream_state

    def chunk_date_range(self, start_date: DateTime, **kwargs) -> Iterable[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now. Ignore weekends since exchanges don't run on weekends.
        The return value is a list of dicts {'date': date_string}.
        """
        days = []
        now = pendulum.now().date()
        while start_date <= now:
            days.append({"date": start_date.to_date_string()})
            start_date = start_date.add(days=1)
        # self.logger.info(f"chunk date range value {days}")
        return days


class SourceExchangeRatesLocal(AbstractSource):
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            headers = {"apikey": config["access_key"]}
            params = {}
            base = config.get("base")
            if base is not None:
                params["base"] = base

            resp = requests.get(f"{ExchangeRates.url_base}{config['start_date']}", params=params, headers=headers)
            status = resp.status_code
            logger.info(f"Ping response code: {status}")
            if status == 200:
                return True, None
            # When API requests is sent but the requested data is not available or the API call fails
            # for some reason, a JSON error is returned.
            # https://exchangeratesapi.io/documentation/#errors
            error = resp.json().get("error", resp.json())
            code = error.get("code")
            message = error.get("message") or error.get("info")
            # If code is base_currency_access_restricted, error is caused by switching base currency while using free
            # plan
            if code == "base_currency_access_restricted":
                message = f"{message} (this plan doesn't support selecting the base currency)"
            return False, message
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [ExchangeRates(config = config)]

