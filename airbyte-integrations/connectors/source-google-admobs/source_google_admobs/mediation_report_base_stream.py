#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import uuid
import datetime
import re
from abc import ABC
import string
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator
from airbyte_cdk.models import SyncMode
from source_google_admobs import utils
from source_google_admobs import schemas
from source_google_admobs.network_report_base_stream  import NetworkReportBase

class MediationReportBase(NetworkReportBase):
    """
    Base class for incremental stream and custom mediation report stream
    Due to Admobs API limit 100k row reponse per request, and it also does not support pagination, so I create dynamic stream for each app
    First, I call list app API to get app_name and app_id. Then convert to name of each stream
    Uuid is added so that later on I can deduplicate records in data warehouse
    """
    primary_key = "uuid"

    def __init__(self, app_name: str = None, app_id: str = None, **kwargs):
        """override __init__ to add app_name and app_id"""
        super().__init__(**kwargs)
        self.app_name = app_name
        self.app_id = app_id

    @property
    def name(self) -> str:
        """Override method to get stream name according to each app name with prefix MP (mediation report) """
        prefix = "MP"
        stream_name = prefix + self.app_id
        return stream_name

    @property
    def http_method(self) -> str:
       """ Override because using POST. Default by airbyte is GET """
       return "POST"

    def path(
        self ,stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """Here is how I get the publisher id from user's config and add it to the path to call API"""
        publisher_id = self.config["publisher_id"]
        path = f"{publisher_id}/mediationReport:generate"
        return path
    
    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Override get_json_schema CDK method to retrieve the schema of mediation report
        """
        schema: dict[str, Any] = schemas.mediation_report_schema()

        return schema
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """ The items API returns records inside a list dict with the first item is header, the next to before last item is row, and the last item is footer """
        response_json = response.json()

        """ use to check the orgirinal form of reponse from API when use check fuction """
        # yield response_json 
        """ use to check the number of record when use read fuction """
        # return response_json 

        """ use to check return row item only, exclude header and footer item """
        # for reponse_item in response_json:
        #     if 'row' in reponse_item:
        #         yield reponse_item

        """ turn a row item to a dict without keyword row, then add uuid and also transform the format of API response"""
        row = (response_item.get('row') for response_item in response_json if 'row' in response_item)
        for a_dict in row:
            result = {"uuid": str(uuid.uuid4())}
            if 'dimensionValues' in a_dict:
                for key, value in a_dict.get('dimensionValues').items():
                    if 'value' and 'displayLabel' in value.keys():
                        result.update({key: value.get('value')})
                        result.update({key + '_NAME': value.get('displayLabel')})
                    else:
                        result.update({key: value.get('value','null')})
            if 'metricValues' in a_dict:
                for key, value in a_dict.get('metricValues').items():
                    """  
                    filter(None, it) removes all Falsy values such as [], {}, 0, False, set(), '', None, etc.
                    filter() return a generator so need use next() to get item
                    """
                    result.update({key: next(filter(None, [value.get('microsValue'), value.get('integerValue'),value.get('doubleValue')]),0)})
            yield result

    """ use to check the orgirinal form of reponse from API when use check fuction """
    # def request_body_json(
    #     self,
    #     stream_state: Mapping[str, Any],
    #     stream_slice: Mapping[str, Any] = None,
    #     next_page_token: Mapping[str, Any] = None,
    # ) -> Optional[Mapping]:
    #     """ 
    #     POST method needs a body json. 
    #     Json body according to Google Admobs API: https://developers.google.com/admob/api/v1/reference/rest/v1/accounts.mediationReport/generate 
    #     """
    #     date_range = {
    #         "startDate": {"year": 2022, "month": 12, "day": 18},
    #         "endDate": {"year": 2022, "month": 12, "day": 18}
    #         }

    #     app_id = self.app_id

    #     ad_source_id = ['5240798063227064260','1477265452970951479','15586990674969969776','4600416542059544716','6895345910719072481','3528208921554210682','10593873382626181482','17253994435944008978','1063618907739174004','1328079684332308356','2873236629771172317','6432849193975106527','9372067028804390441','18351550913290782395','4839637394546996422','8419777862490735710','3376427960656545613','5208827440166355534','159382223051638006','4100650709078789802','7681903010231960328','6325663098072678541','6925240245545091930','2899150749497968595','18298738678491729107','7505118203095108657','1343336733822567166','2127936450554446159','6060308706800320801','10568273599589928883','11198165126854996598','8079529624516381459','10872986198578383917','8450873672465271579','9383070032774777750','6101072188699264581','3224789793037044399','4918705482605678398','3525379893916449117','3841544486172445473','7068401028668408324','2831998725945605450','3993193775968767067','734341340207269415','3362360112145450544','7295217276740746030','4692500501762622178','7007906637038700218','8332676245392738510','4970775877303683148','7360851262951344112','1940957084538325905','1953547073528090325','4692500501762622185','5506531810221735863','5264320421916134407']

    #     dimensions = ['DATE','AD_SOURCE','AD_SOURCE_INSTANCE','AD_UNIT','APP','MEDIATION_GROUP','COUNTRY','FORMAT','PLATFORM']

    #     metrics = ['AD_REQUESTS','MATCHED_REQUESTS','CLICKS','ESTIMATED_EARNINGS','IMPRESSIONS']
        
    #     # dimensions = ['DATE','AD_UNIT','APP','COUNTRY','FORMAT','PLATFORM','MOBILE_OS_VERSION','APP_VERSION_NAME','SERVING_RESTRICTION','GMA_SDK_VERSION']

    #     # metrics = ['AD_REQUESTS','MATCHED_REQUESTS','SHOW_RATE','MATCH_RATE','CLICKS','ESTIMATED_EARNINGS','IMPRESSIONS','IMPRESSION_CTR','IMPRESSION_RPM']

    #     sort_conditions = [{'dimension': 'DATE', 'order': 'DESCENDING'}]

    #     dimension_filters = [{'dimension': 'AD_SOURCE','matches_any': {'values': ad_source_id} }]

    #     report_spec = {
    #         'dateRange': date_range,
    #         'dimensions': dimensions,
    #         'metrics': metrics,
    #         'sortConditions': sort_conditions,
    #         'dimensionFilters': dimension_filters
    #     }

    #     body_json = {"reportSpec": report_spec}

    #     return body_json

class MediationReport(MediationReportBase,IncrementalMixin):
    """Incremental stream for mediation report api. Just adding some new function to get incremental"""
    _record_date_format = "%Y%m%d"

    def __init__(self, *args, **kwargs):
        """Due to multiple inheritance, so need MRO"""
        super(MediationReport, self).__init__(*args, **kwargs)
        self._cursor_value = None
  
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
            return {self.cursor_field: utils.string_to_date(self.config["start_date"])}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = utils.string_to_date(value[self.cursor_field]) + datetime.timedelta(days=1)
        self.logger.info(f"Cursor Setter {self._cursor_value}")
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        slice = []
        today: datetime.date = datetime.date.today()
        yesterday: datetime.date = datetime.date.today() - datetime.timedelta(days=1)
        number_days_backward: int = int(next(filter(None,[self.config.get('number_days_backward')]),self.number_days_backward_default))
        start_date: datetime.date = self.state[self.cursor_field] - datetime.timedelta(days=number_days_backward)

        while start_date < today:
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

        """ add list ad_source_id does not include Admobs Network"""
        ad_source_id = ['5240798063227064260','1477265452970951479','15586990674969969776','4600416542059544716','6895345910719072481','3528208921554210682','10593873382626181482','17253994435944008978','1063618907739174004','1328079684332308356','2873236629771172317','6432849193975106527','9372067028804390441','18351550913290782395','4839637394546996422','8419777862490735710','3376427960656545613','5208827440166355534','159382223051638006','4100650709078789802','7681903010231960328','6325663098072678541','6925240245545091930','2899150749497968595','18298738678491729107','7505118203095108657','1343336733822567166','2127936450554446159','6060308706800320801','10568273599589928883','11198165126854996598','8079529624516381459','10872986198578383917','8450873672465271579','9383070032774777750','6101072188699264581','3224789793037044399','4918705482605678398','3525379893916449117','3841544486172445473','7068401028668408324','2831998725945605450','3993193775968767067','734341340207269415','3362360112145450544','7295217276740746030','4692500501762622178','7007906637038700218','8332676245392738510','4970775877303683148','7360851262951344112','1940957084538325905','1953547073528090325','4692500501762622185','5506531810221735863','5264320421916134407']

        # dimensions = ['DATE','AD_SOURCE','APP']
        dimensions = ['DATE','AD_SOURCE','AD_UNIT','APP','MEDIATION_GROUP','COUNTRY','FORMAT','PLATFORM']

        metrics = ['AD_REQUESTS','MATCHED_REQUESTS','CLICKS','ESTIMATED_EARNINGS','IMPRESSIONS']

        sort_conditions = [{'dimension': 'DATE', 'order': 'DESCENDING'}]

        dimension_filters = [{'dimension': 'APP','matches_any': {'values': app_id}},{'dimension': 'AD_SOURCE','matches_any': {'values': ad_source_id} }]

        report_spec = {
            'dateRange': date_range,
            'dimensions': dimensions,
            'metrics': metrics,
            'sortConditions': sort_conditions,
            'dimensionFilters': dimension_filters
        }

        body_json = {"reportSpec": report_spec}

        return body_json
    
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
            record_cursor_value = utils.string_to_date(record[self.cursor_field], self._record_date_format)
            self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            # self.logger.info(f"read record with ELSE, record_cursor_value: {record_cursor_value} and self._cursor_value: {self._cursor_value} ")
            yield record

