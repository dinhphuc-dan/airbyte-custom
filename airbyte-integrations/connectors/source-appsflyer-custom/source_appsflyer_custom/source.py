#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from source_appsflyer_custom.base_and_check_connection_stream import AppsFlyerCheckConnectionStream
from source_appsflyer_custom.raw_reports import (
    AppsFlyerAdRevenueRawNonOrganic,
    AppsFlyerAdRevenueRawOrganic,
    AppsFlyerIAPRawOrganic,
    AppsFlyerIAPRawNonOrganic,
)
from source_appsflyer_custom.aggregated_reports import (
    AppsFlyerAggregatedDailyReport,
    AppsFlyerAggregatedGeoDailyReport,
    AppsFlyerAggregatedPartnersDailyReport,
)


# Source
class SourceAppsflyerCustom(AbstractSource):
    def get_authenticator(self, config) -> TokenAuthenticator:
        return TokenAuthenticator(token=config["api_key"])

    def _generate_raw_reports(self, config):
        auth = self.get_authenticator(config=config)
        list_apps = config["list_apps"]
        for app in list_apps:
            yield AppsFlyerAdRevenueRawNonOrganic(authenticator=auth, config=config, app_id=app)
            yield AppsFlyerAdRevenueRawOrganic(authenticator=auth, config=config, app_id=app)
            yield AppsFlyerIAPRawNonOrganic(authenticator=auth, config=config, app_id=app)
            yield AppsFlyerIAPRawOrganic(authenticator=auth, config=config, app_id=app)

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = self.get_authenticator(config=config)
            logger.info(f"load auth {auth}")
            check_connection_steam = AppsFlyerCheckConnectionStream(authenticator=auth, config=config)
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            list_apps = list(set(i for i in check_connection_records))
            for app in config["list_apps"]:
                if app.lower() not in list_apps:
                    raise Exception(f"{app} not found in AppsFlyer")
                else:
                    logger.info(f"Check permission {app} successfully")
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = []
        streams.extend(self._generate_raw_reports(config=config))
        streams.extend(
            [
                AppsFlyerAggregatedDailyReport(authenticator=self.get_authenticator(config=config), config=config),
                AppsFlyerAggregatedGeoDailyReport(authenticator=self.get_authenticator(config=config), config=config),
                AppsFlyerAggregatedPartnersDailyReport(authenticator=self.get_authenticator(config=config), config=config),
            ]
        )
        return streams
