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
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from source_google_analytics_ga4_custom.authenticator import GoogleServiceKeyAuthenticator


# Base Abstract Class
class GoogleAnalyticsGa4CustomAbstractStream(HttpStream, ABC):

    url_base = "https://analyticsdata.googleapis.com/v1beta/"

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
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

class GoogleAnalyticsGa4CustomStreamTestConnectionStream(GoogleAnalyticsGa4CustomAbstractStream):
    primary_key = None

    def __init__(self,property_id, *args, **kwargs):
        """Due to multiple inheritance, so need MRO"""
        super(GoogleAnalyticsGa4CustomStreamTestConnectionStream, self).__init__(*args, **kwargs)
        self.property_id = property_id

    @property
    def http_method(self) -> str:
       """ Override because using POST. Default by airbyte is GET """
       return "GET"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"properties/{self.property_id}/metadata"

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        yield response.json()


class GoogleAnalyticsGa4CustomStream(GoogleAnalyticsGa4CustomAbstractStream,IncrementalMixin):
    primary_key = None
    number_days_backward_default = 7

    def __init__(self, property_name, property_id, *args, **kwargs):
        """Due to multiple inheritance, so need MRO"""
        super(GoogleAnalyticsGa4CustomStream, self).__init__(*args, **kwargs)
        self._cursor_value = None
        self.propery_name = property_name
        self.propery_id = property_id
        self.custom_reports = json.loads(self.config["custom_reports"])

    @property
    def http_method(self) -> str:
       """ Override because using POST. Default by airbyte is GET """
       return "POST"
    
    @property
    def name(self) -> str:
        """Override method to get stream name according to custom report name """
        # stream_name = self.custom_reports['name']
        stream_name = self.propery_name
        return stream_name

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

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
        # return f"properties/{self.config['property_id']}:runReport"
        return f"properties/{self.propery_id}:runReport"
    
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
            },
        }

        schema["properties"].update({d: {"type": ["null", "string"]} for d in self.get_dimensions()})
        schema["properties"].update({m: {"type": ["null", "number"]} for m in self.get_metrics()})
       
        return schema

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        today: datetime.date = datetime.date.today()
        number_days_backward: int = int(next(filter(None,[self.config.get('number_days_backward')]),self.number_days_backward_default))
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
            https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport
            """
            date_range = [stream_slice]
            # date_range = [{"startDate": "2022-12-20", "endDate": "2022-12-20"}]
            dimensions = [{"name": d} for d in self.get_dimensions()]
            metrics = [{"name": m} for m in self.get_metrics()]
            body_json = {
                'dateRanges': date_range,
                'dimensions': dimensions,
                'metrics': metrics
            }
            return body_json

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        r = response.json()

        if all(key in r for key in ["limit", "offset", "rowCount"]):
            limit, offset, total_rows = r["limit"], r["offset"], r["rowCount"]

            if total_rows <= offset:
                return None

            return {"limit": limit, "offset": offset + limit}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()

        """ use to check the orgirinal form of reponse from API """
        # yield response_json 

        """transform the format of API response"""
        dimensions: list = [header['name'] for header in response_json['dimensionHeaders']]
        metrics: list = [header['name'] for header in response_json['metricHeaders']]

        result = {}

        for record in response_json.get('rows',[]):
            dimensions_values: list = []
            for value_dict in record['dimensionValues']:
                dimensions_values.append(value_dict['value'])
            result.update(dict(zip(dimensions, dimensions_values)))

            metrics_values: list = []
            for value_dict in record['metricValues']:
                metrics_values.append(value_dict['value'])
            result.update(dict(zip(metrics, metrics_values)))
            yield result

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
            next_cursor_value = pendulum.parse(record[self.cursor_field], exact=True)
            self._cursor_value = max(self._cursor_value, next_cursor_value) if self._cursor_value else next_cursor_value
            # self.logger.info(f"next_cursor_value: {next_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record


# Source
class SourceGoogleAnalyticsGa4Custom(AbstractSource):

    @staticmethod
    def _get_one_property_id(config) -> str:
        property_list = json.loads(config["property_list_as_dict"])
        property_id_list = []
        for property_name,property_id in property_list.items():
            property_id_list.append(property_id)
        return property_id_list[0]

    @staticmethod
    def _generate_streams(config) -> list[Any]:
        auth = GoogleServiceKeyAuthenticator(credentials= json.loads(config["credentials"]["credentials_json"]))
        property_list = json.loads(config["property_list_as_dict"])
        for property_name,property_id  in property_list.items():
            yield GoogleAnalyticsGa4CustomStream(
                authenticator=auth,
                config=config,
                property_name=property_name,
                property_id=property_id
            )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try: 
            auth = GoogleServiceKeyAuthenticator(credentials= json.loads(config["credentials"]["credentials_json"]))
            property_id = self._get_one_property_id(config=config)
            stream = GoogleAnalyticsGa4CustomStreamTestConnectionStream(authenticator=auth,config=config, property_id=property_id)
            stream_records = stream.read_records(sync_mode="full_refresh")
            record = next(stream_records)
            logger.info(f"There is one of records: {record}")
            return True, None
        except Exception as e:
            return False, e 

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = GoogleServiceKeyAuthenticator(credentials= json.loads(config["credentials"]["credentials_json"]))
        streams = []
        streams.extend(self._generate_streams(config=config))
        # return [GoogleAnalyticsGa4CustomStream(authenticator=auth,config=config)]
        return streams
