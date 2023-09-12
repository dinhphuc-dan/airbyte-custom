#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from source_appsflyer_custom.base_and_check_connection_stream import AppsFlyerCheckConnectionStream
from source_appsflyer_custom.ad_revenue_non_organic_stream import AppsFlyerAdRevenueRawNonOrganic
from source_appsflyer_custom.ad_revenue_organic_stream import AppsFlyerAdRevenueRawOrganic
from source_appsflyer_custom.iap_organic_stream import AppsFlyerIAPRawOrganic
from source_appsflyer_custom.iap_non_organic_stream import AppsFlyerIAPRawNonOrganic


# Source
class SourceAppsflyerCustom(AbstractSource):
    def get_authenticator(self,config) -> TokenAuthenticator:
        return TokenAuthenticator(token=config["api_key"])
    
    def _get_app_info(self, config) -> Tuple[bool, any]:
        auth = self.get_authenticator(config = config)
        list_app_info: list = next(AppsFlyerCheckConnectionStream(authenticator = auth, config=config).read_records(sync_mode="full_refresh"))
        return list_app_info

    def _generate_ad_revenue_raw_non_organic(self, config) -> List[Mapping]:
        auth = self.get_authenticator(config = config)
        list_app_info = self._get_app_info(config=config)
        for item in list_app_info:
            yield AppsFlyerAdRevenueRawNonOrganic(
                authenticator = auth,
                config = config,
                app_id = item["id"],
                app_name=item["name"]
            )
    
    def _generate_ad_revenue_raw_organic(self, config) -> List[Mapping]:
        auth = self.get_authenticator(config = config)
        list_app_info = self._get_app_info(config=config)
        for item in list_app_info:
            yield AppsFlyerAdRevenueRawOrganic(
                authenticator = auth,
                config = config,
                app_id = item["id"],
                app_name=item["name"]
            )

    def _generate_iap_raw_non_organic(self, config) -> List[Mapping]:
        auth = self.get_authenticator(config = config)
        list_app_info = self._get_app_info(config=config)
        for item in list_app_info:
            yield AppsFlyerIAPRawNonOrganic(
                authenticator = auth,
                config = config,
                app_id = item["id"],
                app_name=item["name"]
            )

    def _generate_iap_raw_organic(self, config) -> List[Mapping]:
        auth = self.get_authenticator(config = config)
        list_app_info = self._get_app_info(config=config)
        for item in list_app_info:
            yield AppsFlyerIAPRawOrganic(
                authenticator = auth,
                config = config,
                app_id = item["id"],
                app_name=item["name"]
            )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = self.get_authenticator(config = config)
            logger.info(f"load auth {auth}")
            check_connection_steam = AppsFlyerCheckConnectionStream(authenticator = auth, config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            record = next(check_connection_records)
            logger.info(f"There is one of records: {record}")
            return True, None
        except Exception as e:
            return False

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = []

        ad_revenue_raw_non_organic_streams = self._generate_ad_revenue_raw_non_organic(config=config)
        streams.extend(ad_revenue_raw_non_organic_streams)

        ad_revenue_raw_organic_streams = self._generate_ad_revenue_raw_organic(config=config)
        streams.extend(ad_revenue_raw_organic_streams)

        iap_raw_non_organic_streams = self._generate_iap_raw_non_organic(config=config)
        streams.extend(iap_raw_non_organic_streams)

        iap_raw_organic_streams = self._generate_iap_raw_organic(config=config)
        streams.extend(iap_raw_organic_streams)

        return streams
