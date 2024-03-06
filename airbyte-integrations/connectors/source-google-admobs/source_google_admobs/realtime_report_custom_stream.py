#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import datetime
import pendulum
import uuid
import re
from abc import ABC
import string
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from source_google_admobs import utils
from source_google_admobs import schemas
from source_google_admobs.mediation_report_base_stream  import MediationReportBase
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import IncrementalMixin

class RealtimeCustomReport(MediationReportBase,IncrementalMixin ):
    """
    Subclass of Incremental Mediation Report
    Adjust only some part so that users can pick custom items
    """
    primary_key = "uuid"

    def __init__(self, *args, **kwargs):
        """Due to multiple inheritance, so need MRO"""
        super(RealtimeCustomReport, self).__init__(*args, **kwargs)
        self._cursor_value = None
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone  = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
  
    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "DATE"

    """ Override parent name method to add new prefix Custom_MP"""
    @property
    def name(self) -> str:
        prefix = "CustomMP"
        stream_name = prefix + self.app_id
        return stream_name
    
    @property
    def state(self) -> Mapping[str, Any]:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def get_dimensions(self) ->list:
        required_dimensions = ['DATE','APP']
        if self.config.get("custom_report_dimensions"):
            return required_dimensions + self.config.get("custom_report_dimensions")
        else:
            return required_dimensions

    def get_metrics(self) ->list:
        if self.config.get("custom_report_metrics"):
            return self.config.get("custom_report_metrics")

    """Override parent request+body_json"""
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
        date_range = stream_slice

        app_id = self.app_id

        dimensions = self.get_dimensions()

        metrics = self.get_metrics()

        sort_conditions = [{'dimension': 'DATE', 'order': 'ASCENDING'}]

        dimension_filters = {'dimension': 'APP','matches_any': {'values': app_id}}

        report_spec = {
            'dateRange': date_range,
            'dimensions': dimensions,
            'metrics': metrics,
            'sortConditions': sort_conditions,
            'dimensionFilters': dimension_filters
        }

        body_json = {"reportSpec": report_spec}
        self.logger.info(f"stream slice date {stream_slice['startDate']} - {stream_slice['endDate']}")
        return body_json
    
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
                "APP_NAME":{"type": ["null", "string"]},
            },
        }

        schema["properties"].update({d: {"type": ["null", "string"]} for d in self.get_dimensions()})
        if "AD_UNIT" in self.get_dimensions():
            schema["properties"].update({"AD_UNIT_NAME": {"type": ["null", "string"]}})
        if "AD_SOURCE" in self.get_dimensions():
            schema["properties"].update({"AD_SOURCE_NAME":{"type": ["null", "string"]}})
        if "MEDIATION_GROUP" in self.get_dimensions():
            schema["properties"].update({"MEDIATION_GROUP_NAME": {"type": ["null", "string"]}})
        schema["properties"].update({m: {"type": ["null", "number"]} for m in self.get_metrics()})
       
        return schema

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        data_avaliable_date : datetime.date = pendulum.today(self.timezone).date()

        if stream_state:
            ''' this code for incremental run, the stream will start with the last date of record minus number_days_backward'''
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        elif self.get_last_X_days:
            '''' this code for (the first time run or full refresh run) and get_last_X_days is true, the stream will start with today date minus number_days_backward'''
            start_date: datetime.date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")
        
        else: 
            '''' this code for the first time run or full refresh run, the stream will start with the start date in config'''
            start_date: datetime.date = pendulum.parse(self.config["start_date"]).date()
            # self.logger.info(f"stream slice start date in ELSE {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        slice.append({
            'startDate': utils.turn_date_to_dict(start_date),
            'endDate': utils.turn_date_to_dict(data_avaliable_date),
            }
        )

        return slice or [None]

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
            record_cursor_value = pendulum.parse(record[self.cursor_field]).date()
            self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record with ELSE, record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record
    