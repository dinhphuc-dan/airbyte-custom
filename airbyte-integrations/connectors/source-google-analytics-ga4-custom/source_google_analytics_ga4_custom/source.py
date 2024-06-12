#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union, Dict, Generator

import json
import datetime
import pendulum
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
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
    
    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None
    
    @property
    def name(self) -> str:
        """
        :return: Stream name. By default this is the implementing class name, but it can be overridden as needed.
        because this is a @property decorator so we have to use super().name instead of super().name()
        """
        return super().name
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return None

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
        yield response.status_code                           


class GoogleAnalyticsGa4CustomStream(GoogleAnalyticsGa4CustomAbstractStream,IncrementalMixin):
    primary_key = None
    limit = 250000
    offset = 0
    _property_id_is_changed = False
    _property_id_in_request = 0

    def __init__(self, 
                    report_name, 
                    report_dimensions, 
                    report_metrics, 
                    report_start_date, 
                    list_properties_name_and_id_as_dict,
                    is_daily_report, 
                    cursor_field,
                    cohort_range = None,
                    *args, **kwargs
    ):
        """Due to multiple inheritance, so need MRO"""
        super(GoogleAnalyticsGa4CustomStream, self).__init__(*args, **kwargs)
        self._cursor_value = None
        self.cs_field = cursor_field
        self.is_daily_report = is_daily_report
        self.stream_name = report_name

        if is_daily_report:
            # adding date as default dimension for daily report
            report_dimensions.append("date")
        else:
            # adding cohort and cohortNthDay as default dimension for daily cohort report
            report_dimensions.extend(["cohort", "cohortNthDay"])
            
        self.dimensions = report_dimensions
        self.metrics = report_metrics
        self.start_date = report_start_date
        self.list_properties_name_and_id_as_dict = list_properties_name_and_id_as_dict
        self.cohort_range = cohort_range
        self.number_days_backward: int = self.config.get("number_days_backward", 7)
        self.timezone: str  = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)

    @property
    def http_method(self) -> str:
       """ Override because using POST. Default by airbyte is GET """
       return "POST"
    
    @property
    def name(self) -> str:
        """Override method to get stream name according to custom report name """
        return self.stream_name.lower().replace(" ", "_")

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return self.cs_field

    @property
    def state(self) -> Mapping[str, Any]:
        # self.logger.info(f"Cursor Getter {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        # self.logger.info(f"Cursor Setter BEFORE {value}")
        ''' 
        in case the first run does not return any record, the second incremental run will get value of cursor_field of stream state as null
        which in turn will cause error. Thus, we add if value of cursor_field is None then getting the config start_date instead
        '''
        if value[self.cursor_field] is None:
            self._cursor_value = pendulum.parse(self.start_date).date()
        else:
            self._cursor_value = pendulum.parse(value[self.cursor_field]).add(days=1).date()
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"properties/{stream_slice['property_id']}:runReport"

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Override get_json_schema CDK method to get dynamic custom report
        """
        schema: dict[str, Any] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": ["null", "object"],
            "additionalProperties": True,
            "properties": {
                "property_name": {"type": ["null", "string"]},
                "property_id": {"type": ["null", "string"]},
                "dataLossFromOtherRow": {"type": ["null", "boolean"]},
            },
        }

        schema["properties"].update({d: {"type": ["null", "string"]} for d in self.dimensions})
        schema["properties"].update({m: {"type": ["null", "number"]} for m in self.metrics})
       
        return schema

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        data_avaliable_date : datetime.date = pendulum.today(self.timezone).date()
        
        if self.get_last_X_days:
            '''' this code for all kind of run, such as: the first time run or full refresh or incremental run, the stream will start with today date minus number_days_backward'''
            start_date: datetime.date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
            # self.logger.info(f"stream slice start date in IF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        elif stream_state:
            ''' this code for incremental run and get_last_X_days is false, the stream will start with the last date of stream state minus number_days_backward'''
            start_date: datetime.date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
            # self.logger.info(f"stream slice start date in ELIF {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        else: 
            '''' this code for the first time run or full refresh run, the stream will start with the start date in config'''
            start_date: datetime.date = pendulum.parse(self.start_date).date()
            # self.logger.info(f"stream slice start date in ELSE {start_date}, cusor value {self._cursor_value}, stream state {stream_state}")

        '''
        for daily report, slice will be start_date and end_date (end_date = data_avaliable_date)
        for daily cohort report, start_date = end_date, and we loop this until reach data_avaliable_date
        '''
        if self.is_daily_report:
            end_date: datetime.date = data_avaliable_date
            for i in self.list_properties_name_and_id_as_dict:
                property_name, property_id = i['property_name'], i['property_id']
                slice.append(
                    {
                        'startDate': start_date.to_date_string(),
                        'endDate': end_date.to_date_string(),
                        'property_id': property_id,
                        'property_name': property_name,
                        
                    }
                )
        else:
            list_cohort_date_range = []
            while start_date <= data_avaliable_date:
                end_date: datetime.date = start_date
                list_cohort_date_range.append(
                    {
                        'startDate': start_date.to_date_string(),
                        'endDate': end_date.to_date_string()
                    }
                )
                start_date: datetime.date = start_date.add(days=1)
            
            for i in self.list_properties_name_and_id_as_dict:
                property_name, property_id = i['property_name'], i['property_id']
                slice.append(
                    {
                        'list_date_range': list_cohort_date_range,
                        'property_id': property_id,
                        'property_name': property_name
                    }
                )


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
            
            dimensions = [{"name": d} for d in self.dimensions]
            metrics = [{"name": m} for m in self.metrics]

            # date_info_in_request is for printing out the date in request only
            date_info_in_request = []

            '''
            daily report vs cohort report has different body json, so we tried each case differently
            '''
            if self.is_daily_report:
                date_range =  [{
                    'startDate': stream_slice['startDate'],
                    'endDate': stream_slice['endDate']
                }]

                date_info_in_request.append(stream_slice['startDate'])
                date_info_in_request.append(stream_slice['endDate']) 

                body_json = {
                    'dimensions': dimensions,
                    'metrics': metrics,
                    'returnPropertyQuota': True,
                    'limit': self.limit,

                    'orderBys': [{'dimension':{'dimensionName': self.cursor_field}}],

                    'dateRanges': date_range,
                }
            else:
                cohorts = []
                for date_range in stream_slice['list_date_range']:
                    cohorts.append(
                        {
                            'dateRange': date_range,
                            'dimension': 'firstSessionDate',
                            'name': date_range['startDate']
                        }
                    )
                    # date_info_in_request is for printing date in request only
                    date_info_in_request.append(date_range['startDate'])

                
                body_json = {
                    'dimensions': dimensions,
                    'metrics': metrics,
                    'returnPropertyQuota': True,
                    'limit': self.limit,

                    'orderBys': [{'dimension':{'dimensionName': 'cohort'}}, {'dimension':{'dimensionName': 'cohortNthDay'}}],

                    'cohortSpec':{
                        'cohortsRange':{
                            'endOffset': self.cohort_range,
                            'granularity': 'DAILY'
                        },

                        'cohorts': cohorts
                    },
                }

            if next_page_token:
                body_json.update(next_page_token)

            '''
            We update property_id each time we call request_body_json, so we can detect whenever calling new property, we reset offset param to 0
            '''
            self._update_property_id_in_request = stream_slice['property_id']

            date_info_in_request = {
                'startDate': min(date_info_in_request),
                'endDate': max(date_info_in_request)
            }

            # self.logger.info(f'BODDDYYYYY {body_json}')
            self.logger.info(f"Request Body {date_info_in_request}, proterty id {self._update_property_id_in_request}")
            return body_json
    
    @property
    def _update_property_id_in_request(self) -> str:
        return self._property_id_in_request

    @_update_property_id_in_request.setter
    def _update_property_id_in_request(self, value):
        '''
        We try to set the _property_id_in_request whenever we call request_body_json,
        if the new value is the same as the old value, we don't update the _property_id_in_request and set _property_id_is_changed to False
        else we update the _property_id_in_request with the new value, and also update variable _property_id_is_changed to True
        '''
        if self._property_id_in_request == value:
            # self.logger.info('Happen in SETTER IF')
            self._property_id_is_changed = False
        else:
            # self.logger.info('Happen in SETTER ELSE')
            self._property_id_is_changed = True
            self._property_id_in_request = value


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        '''
        The next page token fucntion is called after read_record is done
        '''
        r = response.json()

        # get total_row from each request, then we calculate the next offset based on the constant limit
        total_row = r.get('rowCount', 0)

        '''Whenever _property_id_is_changed is True and the request has more than one page (meaning we call new property), we reset the offset to 0'''

        if self._property_id_is_changed is True and total_row > self.limit:
            # self.logger.info(f'RESET OFFSET: offset before reset {self.offset}')
            self.offset = 0

        '''
        total_row = 0 means, the response is empty
        if total_row < limit, means, all records is in one page
        if total_row > limit, means, the response has more than one page. Thus, we make a stop when 
        number of time call (offset / limit) is equal to quotitient of (total_row // limit) 
        '''
        
            
        if total_row == 0 or total_row < self.limit or (self.offset / self.limit) == (total_row // self.limit):
            # self.logger.info(f'Next page token in IF, OFFSET: {self.offset}')
            return None
        else:
            self.offset = self.limit + self.offset
            # self.logger.info(f'Next page token in Else, OFFSET: {self.offset}')
            return {"limit": self.limit, "offset": self.offset}

    
    def should_retry(self, response: requests.Response) -> bool:
        """
        By default, aribyte uses the following HTTP status codes for retries:
         - 429 (Too Many Requests) indicating rate limiting
         - 500s to handle transient server errors
        For case of GA4, we are trying to pause the connection before it hits quota limit
        """

        '''
        response quota is in format
          "propertyQuota": {
            "tokensPerDay": {"consumed": 1, "remaining": 199848},
            "tokensPerHour": {"consumed": 1, "remaining": 39984},
            "concurrentRequests": {"consumed": 0, "remaining": 10},
            "serverErrorsPerProjectPerHour": {"consumed": 0, "remaining": 10},
            "potentiallyThresholdedRequestsPerHour": {"consumed": 0, "remaining": 120},
            "tokensPerProjectPerHour": {"consumed": 1, "remaining": 13997}
            }
        When the quota remaining is <= 0, we pause the connection for an back_off time accordingly
        We only pause for 
        + tokensPerProjectPerHour
        + potentiallyThresholdedRequestsPerHour
        + tokensPerHour
        + serverErrorsPerProjectPerHour
        '''
        
        response_json = response.json()
        quota_list = ['tokensPerProjectPerHour', 
                      'potentiallyThresholdedRequestsPerHour', 
                      'tokensPerHour', 
                      'serverErrorsPerProjectPerHour'
        ]
        for quota in quota_list:
            if response_json['propertyQuota'][quota]['remaining'] <= 0:
                self._back_off_time = 3600
                self._error_message = f"Quota for {quota} is about to exhausted. Pause for {self._back_off_time} second"
                return True
        else: 
            return super().should_retry(response=response)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if self._back_off_time:
            return self._back_off_time
        return super().backoff_time(response=response)
    
    def error_message(self, response: requests.Response) -> str:
        if self._error_message:
            return self._error_message
        return super().error_message(response=response)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()

        """ use to check the orgirinal form of reponse from API """
        # yield response_json 

        '''
        response_json in this format
        { 
            "dimensionHeaders": [
                {"name": "cohort"},
                {"name": "cohortNthDay"}
            ],
            "metricHeaders": [
                {"name": "cohortActiveUsers","type": "TYPE_INTEGER"}
            ],
            "rows": [{
                "dimensionValues": [
                    {"value": "cohort_0"},
                    {"value": "0000"}
                ],
                "metricValues": [
                    {"value": "3549"}
                ]},
            ]
        }
        '''

        """transform the format of API response"""
        dimensions: list = [header['name'] for header in response_json['dimensionHeaders']]
        metrics: list = [header['name'] for header in response_json['metricHeaders']]

        data_loss = response_json['metadata'].get('dataLossFromOtherRow', False)
        if data_loss:
            self.logger.warning("Warning: dataLossFromOtherRow detected, becarefully for using recorded data downstream. To reduce effect of dataLossFromOtherRow, please select less dimensions in report config. For more information: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/ResponseMetaData")

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

            result.update({'dataLossFromOtherRow': data_loss})   
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
            if record is not None:
                record.update({'property_name': stream_slice['property_name']})
                record.update({'property_id': stream_slice['property_id']})
            yield record


# Source
class SourceGoogleAnalyticsGa4Custom(AbstractSource):

    def _generate_daily_streams(self, config) -> List[Any]:
        auth = GoogleServiceKeyAuthenticator(credentials= json.loads(config["credentials"]["credentials_json"]))
        list_properties_name_and_id_as_dict = config["list_properties_name_and_id_as_dict"]
        
        if config.get('daily_reports', None):
            for report in config['daily_reports']:
                yield GoogleAnalyticsGa4CustomStream(
                    authenticator=auth,
                    config = config,
                    list_properties_name_and_id_as_dict=list_properties_name_and_id_as_dict,
                    report_name = report['report_name'], 
                    report_dimensions = report.get('dimensions', []), 
                    report_metrics = report.get('metrics', []), 
                    report_start_date = report['start_date'],
                    is_daily_report = True,
                    cursor_field = 'date',
                )
        
        if config.get('daily_cohort_reports', None):
            for report in config['daily_cohort_reports']:
                yield GoogleAnalyticsGa4CustomStream(
                    authenticator=auth,
                    config = config,
                    list_properties_name_and_id_as_dict=list_properties_name_and_id_as_dict,
                    report_name = report['report_name'], 
                    report_dimensions = report.get('dimensions', []), 
                    report_metrics = report.get('metrics', []), 
                    report_start_date = report['start_date'],
                    is_daily_report = False,
                    cursor_field = 'cohort',
                    cohort_range= report['cohort_range'],
                )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try: 
            auth = GoogleServiceKeyAuthenticator(credentials= json.loads(config["credentials"]["credentials_json"]))
            if len(config["list_properties_name_and_id_as_dict"]) == 0:
                raise Exception("Please fill List Properties Name and ID in your config")
            for i in config["list_properties_name_and_id_as_dict"]:
                property_name, property_id = i['property_name'], i['property_id']
                stream = GoogleAnalyticsGa4CustomStreamTestConnectionStream(authenticator=auth,config=config, property_id=property_id)
                stream_records = stream.read_records(sync_mode="full_refresh")
                record = next(stream_records)
                if record == 200:
                    logger.info(f" Check permission for {property_id} as {property_name} successfully")
            # check if user fill date dimension in daily_reports, if so, raise exception because we will add date dimension by default
            if config.get('daily_reports', None):
                for report in config['daily_reports']:
                    dimensions: list = [x.lower() for x in report.get('dimensions', [])]
                    if 'date' in dimensions:
                        raise Exception("Please remove date Dimension from Daily Reports config") 
            # check if user fill cohort or cohortNthDay dimension in daily_cohort_reports, if so, raise exception because we will add those dimensions by default
            if config.get('daily_cohort_reports', None):
                for report in config['daily_cohort_reports']:
                    dimensions: list = [x.lower() for x in report.get('dimensions', [])]
                    if 'cohort' in dimensions or 'cohortnthday' in dimensions:
                        raise Exception("Please remove cohort or cohortNthDay Dimension from Daily Cohort Reports config") 
            return True, None
        except Exception as e:
            return False, e 

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = []
        streams.extend(self._generate_daily_streams(config=config))
        return streams
