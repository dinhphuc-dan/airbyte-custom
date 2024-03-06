#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union


from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
from source_apple_store_custom.authenticator import AppleStoreConnectAPIAuthenticator
from source_apple_store_custom.reports import AppleStoreCheckConnectionStream, AppleStoreReport

# Source
class SourceAppleStoreCustom(AbstractSource):

    def _get_list_sale_report_stream(self, config) -> list[Stream]:
        auth = AppleStoreConnectAPIAuthenticator(config=config)
        dict_sale_reports = {
            "SummarySales" : {"reportType":"SALES", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_1", "cursor_field": "Begin_Date", "cursor_format":"MM/DD/YYYY"},
            "SubscriptionEvent": {"reportType":"SUBSCRIPTION_EVENT", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_3","cursor_field": "Event_Date","cursor_format":"YYYY-MM-DD"},  
            # "Subscriber":  {"reportType":"SUBSCRIBER", "reportSubType":"DETAILED", "frequency":"DAILY", "version":"1_3"},
            # "Subscription": {"reportType":"SUBSCRIPTION", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_3"}, 
            # "SubscriptionOfferCodeRedemption": {"reportType":"SUBSCRIPTION_OFFER_CODE_REDEMPTION", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_0"},
            # "PreOrder": {"reportType":"PRE_ORDER", "reportSubType":"SUMMARY", "frequency":"DAILY", "version":"1_0"}
        }

        for key,value in dict_sale_reports.items():
            yield AppleStoreReport(
                report_name=key,
                report_type=value["reportType"],
                report_frequency=value["frequency"], 
                report_sub_type=value["reportSubType"],
                report_version=value["version"],
                cursor_field=value["cursor_field"],
                cursor_format=value["cursor_format"],
                authenticator=auth, 
                config=config
            )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = AppleStoreConnectAPIAuthenticator(config=config)
            logger.info(f"load auth {auth}")
            check_connection_steam = AppleStoreCheckConnectionStream(authenticator=auth, config=config) 
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
        streams.extend(self._get_list_sale_report_stream(config=config))
        return streams
