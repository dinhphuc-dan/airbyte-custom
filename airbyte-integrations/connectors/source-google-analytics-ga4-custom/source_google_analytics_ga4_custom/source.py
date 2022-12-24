#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union, Dict

import json
import datetime
import pendulum
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, auth
from airbyte_cdk.models import SyncMode
from source_google_analytics_ga4_custom.authenticator import GoogleServiceKeyAuthenticator


# Base Abstract Class
class GoogleAnalyticsGa4CustomAbstractStream(HttpStream, ABC):

    url_base = "https://analyticsdata.googleapis.com/v1beta/"

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.custom_reports = json.loads(config["custom_reports"])


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}

class GoogleAnalyticsGa4CustomStream(GoogleAnalyticsGa4CustomAbstractStream):
    primary_key = None

    def __init__(self, *args, **kwargs):
        """Due to multiple inheritance, so need MRO"""
        super(GoogleAnalyticsGa4CustomStream, self).__init__(*args, **kwargs)
        self._cursor_value = None

    @property
    def http_method(self) -> str:
       """ Override because using POST. Default by airbyte is GET """
       return "POST"

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "DATE"

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            self.logger.info(f"Cursor Getter with IF {self._cursor_value}")
            return {self.cursor_field: self._cursor_value}
        else:
            self.logger.info(f"Cursor Getter with ELSE {self._cursor_value}")
            return {self.cursor_field: pendulum.parse(self.config["start_date"], exact=True)}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.parse(value[self.cursor_field], exact=True).add(days=1)
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"properties/{self.config['property_id']}:runReport"
    
    def get_dimensions(self) ->list:
            return self.custom_reports['dimensions']

    def get_metrics(self) ->list:
            return self.custom_reports['metrics']

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Override get_json_schema CDK method to get dynamic custom report
        """
        schema: dict[str, Any] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": ["null", "object"],
            "additionalProperties": True,
            "properties": {
                "uuid": {"type": ["string"], "description": "Custom unique identifier for each record, to support primary key"},
            },
        }

        schema["properties"].update({d: {"type": ["null", "string"]} for d in self.get_dimensions()})
        schema["properties"].update({m: {"type": ["null", "number"]} for m in self.get_metrics()})
       
        return schema

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        today: datetime.date = datetime.date.today()
        number_days_backward: int = int(self.config['number_days_backward'])
        start_date: datetime.date = self.state[self.cursor_field] -  datetime.timedelta(days=number_days_backward)

        while start_date < today:
            end_date: datetime.date = start_date 
            slice.append(
                {
                    'startDate': start_date.strftime("%Y-%m-%d"),
                    'endDate': end_date.strftime("%Y-%m-%d"),
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
            date_range = [stream_slice]
            dimensions = [{"name": d} for d in self.get_dimensions()]
            metrics = [{"name": m} for m in self.get_metrics()]
            body_json = {
                'dateRanges': date_range,
                'dimensions': dimensions,
                'metrics': metrics
            }
            return body_json

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """ The items API returns records inside a list dict with the first item is header, the next to before last item is row, and the last item is footer """
        response_json = response.json()

        """ use to check the orgirinal form of reponse from API when use check fuction """
        yield response_json 
        """ use to check the number of record when use read fuction """
        # return response_json 

        """ use to check return row item only, exclude header and footer item """
        # for reponse_item in response_json:
        #     if 'row' in reponse_item:
        #         yield reponse_item

        """ turn a row item to a dict without keyword row, then add uuid and also transform the format of API response"""
        # row = (response_item.get('row') for response_item in response_json if 'row' in response_item)
        # for a_dict in row:
        #     result = {"uuid": str(uuid.uuid4())}
        #     if 'dimensionValues' in a_dict:
        #         for key, value in a_dict.get('dimensionValues').items():
        #             if 'value' and 'displayLabel' in value.keys():
        #                 result.update({key: value.get('value')})
        #                 result.update({key + '_NAME': value.get('displayLabel')})
        #             else:
        #                 result.update({key: value.get('value','null')})
        #     if 'metricValues' in a_dict:
        #         for key, value in a_dict.get('metricValues').items():
        #             """  
        #             filter(None, it) removes all Falsy values such as [], {}, 0, False, set(), '', None, etc.
        #             filter() return a generator so need use next() to get item
        #             """
        #             result.update({key: next(filter(None, [value.get('microsValue'), value.get('integerValue'),value.get('doubleValue')]),0)})
            # yield result

    # def read_records(
    #     self,
    #     sync_mode: SyncMode,
    #     cursor_field: List[str] = None,
    #     stream_slice: Mapping[str, Any] = None,
    #     stream_state: Mapping[str, Any] = None,
    # ) -> Iterable[Mapping[str, Any]]:
    #     if not stream_slice:
    #         return []
    #     # records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
    #     # for record in records:
    #     #     next_cursor_value = pendulum.parse(record[self.cursor_field], exact=True)
    #     #     self._cursor_value = max(self._cursor_value, next_cursor_value) if self._cursor_value else next_cursor_value
    #     #     # self.logger.info(f"next_cursor_value: {next_cursor_value} and self._cursor_value: {self._cursor_value} ")
    #     yield records

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        r = response.json()

        if all(key in r for key in ["limit", "offset", "rowCount"]):
            limit, offset, total_rows = r["limit"], r["offset"], r["rowCount"]

            if total_rows <= offset:
                return None

            return {"limit": limit, "offset": offset + limit}


# Source
class SourceGoogleAnalyticsGa4Custom(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = GoogleServiceKeyAuthenticator(credentials= json.loads(config["credentials"]["credentials_json"]))
        return [GoogleAnalyticsGa4CustomStream(authenticator=auth,config=config)]
