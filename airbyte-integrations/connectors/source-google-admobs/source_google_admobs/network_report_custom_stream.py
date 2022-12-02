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
from source_google_admobs.network_report_base_stream  import NetworkReport

class CustomNetworkReport(NetworkReport):
    """
    Subclass of Incremental Network Report
    Adjust only some part so that users can pick custom items
    """
    primary_key = "uuid"

    """ Override parent name method to add new prefix Custom_NP"""
    @property
    def name(self) -> str:
        prefix = "CustomNP"
        stream_name = prefix + self.app_id
        return stream_name
    
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

        sort_conditions = [{'dimension': 'DATE', 'order': 'DESCENDING'}]

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
            },
        }

        schema["properties"].update({d: {"type": ["null", "string"]} for d in self.get_dimensions()})
        if "AD_UNIT" in self.get_dimensions():
            schema["properties"].update({"AD_UNIT_NAME": {"type": ["null", "string"]}})
        schema["properties"].update({m: {"type": ["null", "number"]} for m in self.get_metrics()})
       

        return schema

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        today: datetime.date = datetime.date.today()
        start_date: datetime.date = self.state[self.cursor_field]

        while start_date <= today:

            end_date: datetime.date = start_date
            slice.append(
                {
                    'startDate': utils.turn_date_to_dict(start_date),
                    'endDate': utils.turn_date_to_dict(end_date),
                }
            )
            start_date: datetime.date = end_date + datetime.timedelta(days=1)
        self.logger.info(f"stream slice {slice}")
        return slice or [None]
    


  