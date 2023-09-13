#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from source_iron_source_mediation.authenticator import IronSourceMediationAuthenticator
import pendulum
import datetime

# Base stream
class IronSourceMediationStream(HttpStream, ABC):
    url_base = "https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats?"

    def __init__(self, config: Mapping[str, Any], *args ,**kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
    
    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}

class IronSourceMediationCheckConnnectionStream(IronSourceMediationStream):
    primary_key = None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        start_date :str = pendulum.today().subtract(days=1).to_date_string()
        end_date :str = pendulum.today().to_date_string()
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "metrics": 'revenue'
        }
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield response_json
    
class IronSourceMediationAdSourceReport(IronSourceMediationStream, IncrementalMixin):
    primary_key = None
    number_days_backward_default = 7
    _record_date_format = "YYYY-MM-DD"
    _cursor_value: datetime = None

    @property
    def name(self) -> str:
        """Override method to get stream name """
        stream_name = 'Ad_Source_Report'
        return stream_name

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"
    
    @property
    def state(self) -> Mapping[str, Any]:
        '''airbyte always starts syncing by checking stream availability, then sets cursor value as your logic at read_records() fucntion''' 
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        # self.logger.info(f"Cursor Setter Value {value}")
        self._cursor_value: datetime.date = pendulum.from_format(value[self.cursor_field], self._record_date_format).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    def get_json_schema(self) -> Mapping[str, Any]:
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                "date": {"type": ["null", "string"]},
                "appKey": {"type": ["null", "string"]},
                "platform": {"type": ["null", "string"]},
                "adUnits": {"type": ["null", "string"]},
                "instanceName": {"type": ["null", "string"]},
                "instanceId": {"type": ["null", "string"]},
                "bundleId": {"type": ["null", "string"]},
                "appName": {"type": ["null", "string"]},
                "providerName": {"type": ["null", "string"]},
                "countryCode": {"type": ["null", "string"]},
                "revenue": {"type": ["null", "number"]},
                "adSourceChecks": {"type": ["null", "number"]},
                "adSourceResponses": {"type": ["null", "number"]},
                "impressions": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]},
                "videoCompletions": {"type": ["null", "number"]},
                "activeUsers": {"type": ["null", "number"]},
                "engagedUsers": {"type": ["null", "number"]},
                "engagedSessions": {"type": ["null", "number"]},
            }
        }
        return full_schema

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice: list = []
        
        number_days_backward: int = int(next(filter(None,[self.config.get('number_days_backward')]),self.number_days_backward_default))
        if stream_state:
            ''' this code for incremental run, the stream will start with the last date of record minus number_days_backward'''
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=number_days_backward)
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")
            start_date_as_str: str = start_date.to_date_string()
            today_as_string: str = pendulum.today().to_date_string()
            slice.append({
                "startDate": start_date_as_str,
                "endDate": today_as_string
                }
            )

        elif self._cursor_value: 
            '''' this code for the first time run or full refresh run, the stream will start with the start date in config'''
            start_date: datetime.date = pendulum.from_format(self.config["start_date"], self._record_date_format).date()
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

            while start_date <= pendulum.today().date():
                start_date_as_str: str = start_date.to_date_string()
                if start_date.month == pendulum.today().month:
                    today_as_string: str = pendulum.today().to_date_string()
                    slice.append({
                        "startDate": start_date_as_str,
                        "endDate": today_as_string
                        }
                    )
                else:
                    end_date_as_str: str = start_date.end_of('month').to_date_string()
                    slice.append({
                        "startDate": start_date_as_str,
                        "endDate": end_date_as_str
                        }
                    )
                start_date: datetime.date = start_date.add(months=1).start_of('month')

        else:
            ''' this code for airbyte to checking stream availability. It will be run first then starting sync. In order to make this procees shorter, start date is yesteray and end date is today'''  
            start_date:datetime.date = pendulum.today().subtract(days=1).date()
            # self.logger.info(f"stream slice start date in ELSE {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")
            start_date_as_str: str = start_date.to_date_string()
            today_as_string: str = pendulum.today().to_date_string()
            slice.append({
                "startDate": start_date_as_str,
                "endDate": today_as_string
                }
            )

        # self.logger.info(f"stream slice {slice}")
        return slice or [None]

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        param = {
            "breakdowns": 'date,app,platform,adSource,adUnits,instance,country',
            "metrics": 'revenue,adSourceChecks,adSourceResponses,impressions,clicks,completions,activeUsers,engagedUsers,engagedSessions',
        }
        self.logger.info(f"Slice in params {stream_slice}")
        param.update(stream_slice)
        return param

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
            record_cursor_value: datetime.date = pendulum.from_format(record[self.cursor_field], self._record_date_format).date()
            self._cursor_value: datetime.date = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record; record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record
        
        # if there is no record backs, the cursor value will be None, so we update it as the start date in config
        if self._cursor_value == None:
                self._cursor_value: datetime.date = pendulum.parse((self.config["start_date"])).date()

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        record = {}
        for row in response_json:
            for k,v in row.items():
                if k != 'data': 
                    record.update({k: v})
            for data in row['data']:
                for key, value in data.items():
                    record.update({key: value})
                yield record
# Source
class SourceIronSourceMediation(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = IronSourceMediationAuthenticator(config=config)
            logger.info(f"load auth {auth}")
            check_connection_steam = IronSourceMediationCheckConnnectionStream(authenticator = auth, config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            record = next(check_connection_records)
            logger.info(f"There is one of records: {record}")
            return True, None
        except Exception as e:
            return False

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = IronSourceMediationAuthenticator(config=config)
        stream = IronSourceMediationAdSourceReport(authenticator = auth, config=config)
        return [stream]
