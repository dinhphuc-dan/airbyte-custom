#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator
from source_google_admobs.network_report_base_stream import AListApps, NetworkReport
from source_google_admobs.network_report_custom_stream import CustomNetworkReport

token_refresh_endpoint = "https://oauth2.googleapis.com/token"

""" Source """
class SourceGoogleAdmobs(AbstractSource):

    @staticmethod
    def get_authenticator(config):
        """
        Return authentication based on user config
        """
        client_id = config.get("client_id")
        client_secret = config.get("client_secret")
        refresh_token = config.get("refresh_token")
        auth = Oauth2Authenticator(token_refresh_endpoint=token_refresh_endpoint,client_id=client_id,client_secret=client_secret,refresh_token=refresh_token)
        return auth

    @staticmethod
    def _get_app_name_to_id_dict(self, config: Mapping[str, Any]) -> Mapping[str, str]:
        """
        This function get ListApps response and then return a dictionary containing {App_name: App_id} entries.
        """
        list_app_name_to_id_dict = {}
        auth = self.get_authenticator(config)

        list_app_stream = AListApps(authenticator=auth, config=config)
        list_app_records = list_app_stream.read_records(sync_mode="full_refresh")

        for record in list_app_records:
            list_app_name_to_id_dict.update({record.get("app_name"): record.get("app_id")})
        return list_app_name_to_id_dict


    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            """ Check that authenticator can be retrieved """
            auth = self.get_authenticator(config)
            logger.info(f"Successfully build authenticator {auth}")
            network_report_stream = AListApps(authenticator=auth, config=config)
            logger.info(f"Successfully build report {network_report_stream}")
            network_report_records = network_report_stream.read_records(sync_mode="full_refresh")
            logger.info(f"Successfully read records {network_report_records}")
            record = next(network_report_records)
            logger.info(f"There is one of records: {record}")
            record_2 = next(network_report_records,0)
            logger.info(f"There is second records: {record_2}")
            record_3 = next(network_report_records,0)
            logger.info(f"There is third records: {record_3}")
            record_4 = next(network_report_records,0)
            logger.info(f"There is 4th records: {record_4}")
            return True, None
        except Exception as e:
            return False, e
    
    def _generate_network_report_streams(self, authenticator ,config: Mapping[str, Any])-> List[Stream]:
        """Generates a list of stream by app names."""

        auth = self.get_authenticator(config)
        list_app_name_to_id_dict = self._get_app_name_to_id_dict(self, config)

        for app_name, app_id in list_app_name_to_id_dict.items():
            yield NetworkReport(
                authenticator=auth,
                config=config,
                app_id=app_id,
                app_name=app_name,
            )
    
    def _generate_custom_streams(self, authenticator ,config: Mapping[str, Any])-> List[Stream]:
        """Generates a list of stream by app names."""

        auth = self.get_authenticator(config)
        list_app_name_to_id_dict = self._get_app_name_to_id_dict(self, config)

        for app_name, app_id in list_app_name_to_id_dict.items():
            yield CustomNetworkReport(
                authenticator=auth,
                config=config,
                app_id=app_id,
                app_name=app_name
            )

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = self.get_authenticator(config)
        streams = [AListApps(authenticator=auth, config=config)]
        """ add networks one by one in the streams list"""
        network_report_streams = self._generate_network_report_streams(authenticator=auth, config=config)
        streams.extend(network_report_streams)

        if  config.get("custom_report_metrics"):
            custom_streams = self._generate_custom_streams(authenticator=auth, config=config)
            streams.extend(custom_streams)

        return streams
        # return [ListApps(authenticator=auth, config=config),NetworkReport(authenticator=auth, config=config)]

