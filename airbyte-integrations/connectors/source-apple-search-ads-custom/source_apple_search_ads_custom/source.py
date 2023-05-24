#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from source_apple_search_ads_custom.authenticator import AppleSearchAdsAPIAuthenticator
from source_apple_search_ads_custom.campaign_report import AppleSearchAdsCheckConnectionStream, AppleSearchAdsCampaignStream
from source_apple_search_ads_custom.ad_group_report import AppleSearchAdsAdGroupStream

# Source
class SourceAppleSearchAdsCustom(AbstractSource):

    def _get_campaign_id(self, config, authenticator):
        check_connection_steam = AppleSearchAdsCheckConnectionStream(authenticator=authenticator, config=config) 
        check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
        record = next(check_connection_records)
        return record

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = AppleSearchAdsAPIAuthenticator(config=config)
            logger.info(f"load auth {auth}")
            check_connection_steam = AppleSearchAdsCheckConnectionStream(authenticator=auth, config=config) 
            logger.info(f"Successfully build {check_connection_steam}")
            check_connection_records = check_connection_steam.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {check_connection_records}")
            record = next(check_connection_records)
            logger.info(f"There is one of records: {record}")

            return True, None
        except Exception as e:
            return False

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = AppleSearchAdsAPIAuthenticator(config=config)
        list_campaign_id: dict = self._get_campaign_id(config=config, authenticator=auth)
        streams = []
        streams.append(AppleSearchAdsCampaignStream(authenticator=auth, config=config))
        streams.append(AppleSearchAdsAdGroupStream(authenticator=auth, config=config, list_campaign=list_campaign_id ))
        return streams
