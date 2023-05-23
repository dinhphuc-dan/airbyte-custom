#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import datetime
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
    _record_date_format = "%Y%m%d"

    def __init__(self, *args, **kwargs):
        """Due to multiple inheritance, so need MRO"""
        super(RealtimeCustomReport, self).__init__(*args, **kwargs)
        self._cursor_value = None
  
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
        if self._cursor_value:
            self.logger.info(f"Cursor Getter with IF {self._cursor_value}")
            return {self.cursor_field: self._cursor_value}
        else:
            self.logger.info(f"Cursor Getter with ELSE {self._cursor_value} and start date is { self.config['start_date'] }")
            return {self.cursor_field: utils.string_to_date(self.config["start_date"])}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = utils.string_to_date(value[self.cursor_field]) + datetime.timedelta(days=1)
        self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def get_dimensions(self) ->list:
        required_dimensions = ['DATE','APP','AD_SOURCE']
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

        body_json = {
                "reportSpec": report_spec
        }
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
                "AD_SOURCE_NAME":{"type": ["null", "string"]},
            },
        }

        schema["properties"].update({d: {"type": ["null", "string"]} for d in self.get_dimensions()})
        if "AD_UNIT" in self.get_dimensions():
            schema["properties"].update({"AD_UNIT_NAME": {"type": ["null", "string"]}})
        if "MEDIATION_GROUP" in self.get_dimensions():
            schema["properties"].update({"MEDIATION_GROUP_NAME": {"type": ["null", "string"]}})
        schema["properties"].update({m: {"type": ["null", "number"]} for m in self.get_metrics()})
       
        return schema

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        today: datetime.date = datetime.date.today()
        start_date: datetime.date = self.state[self.cursor_field]

        slice.append({
            'startDate': utils.turn_date_to_dict(start_date),
            'endDate': utils.turn_date_to_dict(today),
            }
        )
        self.logger.info(f"stream slice {slice}")
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
            next_cursor_value = utils.string_to_date(record[self.cursor_field], self._record_date_format)
            self._cursor_value = max(self._cursor_value, next_cursor_value) if self._cursor_value else next_cursor_value
            # self.logger.info(f"Record date is {record['DATE']} and self._cursor_value {self._cursor_value} ")
            yield record
    