#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.models import SyncMode, AirbyteMessage, AirbyteStream, ConfiguredAirbyteStream
import pendulum
import datetime
import time
from io import StringIO

import re
from enum import Enum
from functools import lru_cache

from google.api_core.grpc_helpers import _StreamingResponseIterator
from google.ads.googleads.client import GoogleAdsClient

from google.ads.googleads.v20.services.services.google_ads_service import GoogleAdsServiceClient
from google.ads.googleads.v20.services.types.google_ads_service import (
    GoogleAdsRow, 
    SearchGoogleAdsStreamRequest, 
    SearchGoogleAdsStreamResponse,
    SearchGoogleAdsRequest,
    SearchGoogleAdsResponse
)


from google.ads.googleads.v20.services.services.google_ads_field_service import GoogleAdsFieldServiceClient
from google.ads.googleads.v20.services.types.google_ads_field_service import (
    SearchGoogleAdsFieldsRequest, 
    SearchGoogleAdsFieldsResponse
)


StreamData = Union[Mapping[str, Any], AirbyteMessage]

class GoogleAdsCustomBaseStream(Stream, ABC):
    primary_key = None
    _cursor_value = None

    # google data type: https://developers.google.com/google-ads/api/reference/rpc/v20/GoogleAdsFieldDataTypeEnum.GoogleAdsFieldDataType
    google_datatype_mapping = {
        "DATE": {"type": ["null", "string"], "format": "date"},
        "RESOURCE_NAME": {"type": ["null", "string"]},
        "STRING": {"type": ["null", "string"]},
        "MESSAGE": {"type": ["null", "string"]},
        "ENUM": {"type": ["null", "string"]},
        "BOOLEAN": {"type": ["null", "boolean"]},
        "INT64": {"type": ["null", "integer"]},
        "INT32": {"type": ["null", "integer"]},
        "UINT64": {"type": ["null", "integer"]},
        "FLOAT": {"type": ["null", "integer"]},
        "DOUBLE": {"type": ["null", "number"]}
    }

    def __init__(self, 
                 config: Mapping[str, Any],
                 table_name: str = None,
                 validated_custom_query : str = None,
                 query_metadata : dict[str] = None,
                 *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        config['credentials']['use_proto_plus'] = True
        self.config = config
        self.table_name = table_name
        self.validated_custom_query = validated_custom_query
        self.query_metadata = query_metadata
        self.credentials = config['credentials']
        self.google_ads_client = self._get_google_ads_client()
        self.google_ads_service_report = self._create_google_ads_service_report()
        self.google_ads_service_field = self._create_google_ads_service_field()
    
    @property
    def name(self) -> str:
        return self.table_name
    
    def _get_google_ads_client(self) -> GoogleAdsClient:
        """ create google ads client """
        try: 
            return GoogleAdsClient.load_from_dict(self.credentials)
        except Exception as e:
            raise e
    
    def _create_google_ads_service_report(self, service_name='GoogleAdsService') -> GoogleAdsServiceClient:
        """ create google ads service for pulling report data """
        return self.google_ads_client.get_service(name=service_name)
    
    def _create_google_ads_search_report_object(self, customer_id: str, query: str) -> SearchGoogleAdsStreamRequest:
        """ create a search object in order to use with google_ads_service_report.search_stream() """
        search_report_object: SearchGoogleAdsStreamRequest = self.google_ads_client.get_type(name='SearchGoogleAdsStreamRequest')
        search_report_object.customer_id = customer_id
        search_report_object.query = query
        return search_report_object
    
    def _create_google_ads_service_field(self, service_name='GoogleAdsFieldService') -> GoogleAdsFieldServiceClient:
        """ create google ads service for getting report data """
        return self.google_ads_client.get_service(name=service_name)
    
    def _create_google_ads_search_field_object(self, query: str, page_size: int = None) -> SearchGoogleAdsFieldsRequest:
        """ create a search object """
        search_fields_object: SearchGoogleAdsFieldsRequest = self.google_ads_client.get_type(name='SearchGoogleAdsFieldsRequest')
        search_fields_object.query = query
        search_fields_object.page_size = page_size
        return search_fields_object
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """ each slice is a customer ID, which then will be passed to read_records individually """
        slice = self.config['customer_id'].split(',')
        return slice or [None]

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
        custom_query: Optional[str] = None,
    ) -> Iterable[StreamData]:
        """
        Read records from Google Ads
        Args:
            sync_mode: SyncMode
            custom_query: optional to pass a custom query, otherwise use self.validated_custom_query
        """
        # each slice is a customer ID
        customer_id = stream_slice
        query = custom_query if custom_query else self.validated_custom_query
        self.logger.info(f"Query: {query}")
        search_report_object = self._create_google_ads_search_report_object(customer_id=customer_id, query=query)
        response = self.google_ads_service_report.search_stream(request=search_report_object)
        yield from self.parse_response(response=response, stream_slice=stream_slice)
    
    def parse_response(self, response: _StreamingResponseIterator[SearchGoogleAdsStreamResponse], **kwargs) -> Iterable[Mapping]:
        """ each value in response is an SearchGoogleAdsStreamResponse """
        for batch in response:
            for row in batch.results:
                record = self._convert_googleads_row_to_dict(googleads_row=row)
                yield record
    
    def _convert_googleads_row_to_dict(self, googleads_row: GoogleAdsRow) -> dict[str, Any]:
        """
        Due to Google Ads Row is a class, we need to mapping its attribution to each column
        Format need to be smt like this: 
            record = {
                'segments_date': googleads_row.segments.date,
                'customer_id': googleads_row.customer.id,
                'customer_currency_code': googleads_row.customer.currency_code,
                'user_location_view_country_criterion_id': googleads_row.user_location_view.country_criterion_id,
                'campaign_status': googleads_row.campaign.status,
                'campaign_app_campaign_setting_app_id': googleads_row.campaign.app_campaign_setting.app_id,
                'campaign_app_campaign_setting_app_store': googleads_row.campaign.app_campaign_setting.app_store,
                'metrics_clicks': googleads_row.metrics.clicks,
                'metrics_impressions': googleads_row.metrics.impressions,
                'metrics_conversions': googleads_row.metrics.conversions,
                'metrics_cost_micros': googleads_row.metrics.cost_micros
        }
        """
        record = {}
        for field in self.query_metadata['select_clause']:
            field_value = googleads_row
            for field_name in field.split('.'):
                try:
                    # for each nested field, ex segments.date, we get the attribute of the first class
                    # then we get the attribution of the second class by re-assigning first class to a same varible
                    # ex field_value = getattr(field_value, 'segments')
                    # then field_value = getattr(field_value, 'date')
                    field_value = getattr(field_value, field_name)
                except AttributeError as e:
                    raise e
            
            if isinstance(field_value, Enum):
                field_value = field_value.name

            self.logger.debug(f'{field}: {field_value}, {type(field_value)}')

            record.update({field.strip().replace('.', '_'): field_value})
        return record

    # caching for better performance 
    @lru_cache()
    def get_json_schema(self) -> Mapping[str, Any]:
        """ get field metadata from Google Ads Field Service, then covert to json schema """

        # need to add schema into properties
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {
                # "campaign_id": {"type": ["null", "number"]},
                # "campaign_name": {"type": ["null", "string"]},
            },
        }

        # get field metadata and covert to json
        fields = (',').join( f"'{field}'" for field in self.query_metadata['select_clause'])
        page_size = len(self.query_metadata['select_clause'])
        query = f""" 
            SELECT
                name,
                data_type,
                is_repeated  
            WHERE name in ({fields}) 
        """
        search_fields_object = self._create_google_ads_search_field_object(query=query, page_size=page_size) 
        for field in self.google_ads_service_field.search_google_ads_fields(request=search_fields_object):
            if not field.is_repeated:
                full_schema['properties'].update({
                     field.name.replace('.', '_'): self.google_datatype_mapping.get(field.data_type.name)
                })
            else:
                full_schema['properties'].update({
                    field.name.replace('.', '_'): {"type": ["null", "array"], "items": self.google_datatype_mapping.get(field.data_type.name)}
                })

        return full_schema

    def check_query_connection(self):
        """ 
        Send query for each customer ID to Google Ads for checking connection.
        The query contains Select and From clause from users. 
        The Where clause will be set to segments.date DURING TODAY and the Limit clause will be set to 1 to reduce calling time.
        """
        select_clause =  (',').join(self.query_metadata['select_clause'])
        from_clause = self.query_metadata['from_clause']

        check_connection_query = f""" 
            SELECT {select_clause} 
            FROM {from_clause} 
            WHERE segments.date DURING TODAY 
            LIMIT 1 
        """

        for customer_id in self.stream_slices():
            return next(self.read_records(sync_mode=SyncMode.full_refresh, stream_slice=customer_id, custom_query=check_connection_query))


# Source
class SourceGoogleAdsCustom(AbstractSource):
    query_regex = re.compile(
        r"""
            \s*
            SELECT
                \s+(?P<FieldNames>\S.*)\s+
            FROM
                \s+(?P<ResourceNames>[a-z][a-zA-Z_]*)
            \s*
            (\s+WHERE\s+(?P<WhereClause>\S.*?))?
            (\s+ORDER\s+BY\s+(?P<OrderByClause>\S.*?))?
            (\s+LIMIT\s+(?P<LimitClause>[0-9]*?))?
            (\s+PARAMETERS\s+(?P<ParametersClause>\S.*))?
            \s*
            $
        """,
        flags=re.I | re.DOTALL | re.VERBOSE
    )

    def _validate_and_set_custom_query_metadata(self, custom_query) -> dict[str, Any]:
        query_regex = self.query_regex.search(custom_query)
        if not query_regex:
            raise Exception(f"Invalid query: {custom_query}")
        query_metadata = {
            'select_clause' : [f.strip() for f in query_regex.group("FieldNames").split(",")],
            'from_clause': query_regex.group("ResourceNames").strip(),
            'where_clause': query_regex.group("WhereClause").strip(),
        }
        return query_metadata
            
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            for query in config['custom_gaql_query']:
                query_metadata = self._validate_and_set_custom_query_metadata(query['custom_query'])
                connection = GoogleAdsCustomBaseStream(
                    config=config,
                    table_name=query['table_name'],
                    validated_custom_query=query['custom_query'],
                    query_metadata=query_metadata
                )
                connection.check_query_connection()
                logger.info(f"Check connection success")
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = []
        for query in config['custom_gaql_query']:
            query_metadata = self._validate_and_set_custom_query_metadata(query['custom_query'])
            streams.append(
                GoogleAdsCustomBaseStream(
                    config=config,
                    table_name=query['table_name'],
                    validated_custom_query=query['custom_query'],
                    query_metadata=query_metadata
                )
            )
        return streams