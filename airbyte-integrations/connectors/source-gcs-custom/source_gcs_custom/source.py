#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Dict, Generator, List, Optional, Iterable
import logging
from io import IOBase, StringIO, BytesIO
from datetime import timedelta, date
import pendulum
import json
import zipfile

from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.file_based_source import FileBasedSource
from airbyte_cdk.sources.file_based.stream.cursor import DefaultFileBasedCursor
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.exceptions import ErrorListingFiles, FileBasedSourceError

from source_gcs_custom.spec import SourceGCSCustomSpec
from source_gcs_custom.utilities import GCSRemoteFile, ZipHelper

from google.cloud import storage
from google.oauth2 import service_account

import smart_open
import requests

class SourceGCSCustomStreamReader(AbstractFileBasedStreamReader):
    START_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ss"
    """
    Stream reader for Google Cloud Storage Custom.
    """

    def __init__(self):
        super().__init__()
        self._gcs_client = None
        self._config = None

    @property
    def config(self) -> SourceGCSCustomSpec:
        return self._config

    @config.setter
    def config(self, value: SourceGCSCustomSpec):
        self._config = value        
    
    def _create_gcs_client(self) -> storage.Client:
        if not self._gcs_client:
            self._gcs_client = storage.Client(
                credentials=service_account.Credentials.from_service_account_info(
                    info=json.loads(self.config.service_account)
                )
            )
        return self._gcs_client
    
    @property
    def gcs_client(self) -> storage.Client:
        return self._create_gcs_client()

    def get_matching_files(
        self,
        globs: List[str],
        prefix: Optional[str],
        logger: logging.Logger,
    ) -> Iterable[RemoteFile]:
        """
        Return all files that match any of the globs.
        """
        
        try:
            user_defined_globs = [glob for glob in globs]
            final_globs = []
            if self.config and self.config.search_date_in_file_name.get_last_x_days_files_based_on_date_in_file_name:
                start_date: date = pendulum \
                    .today(self.config.search_date_in_file_name.timezone) \
                    .subtract(days=self.config.search_date_in_file_name.number_days_backward)
                for i in range(0, self.config.search_date_in_file_name.number_days_backward + 1):
                    for glob in user_defined_globs:
                        final_glob = glob + (f'{start_date.add(days=i).format(fmt=self.config.search_date_in_file_name.date_in_file_name_format)}**')
                        final_globs.append(final_glob)
                
            else: 
                final_globs = user_defined_globs
                start_date: date = (
                    pendulum.from_format(self.config.start_date, self.START_DATE_FORMAT) if self.config and self.config.start_date else None
                )

            logger.info(f'GLOBS: {final_globs}')
            for glob in final_globs:
                bucket: storage.Bucket = self.gcs_client.get_bucket(self.config.gcs_bucket)
                blobs: storage.Blob = bucket.list_blobs(match_glob=glob)
                for blob in blobs:
                    last_modified = pendulum.instance(dt=blob.updated).in_tz(tz=self.config.search_date_in_file_name.timezone)
                    file_compression = blob.name.split(".")[-1]

                    logger.info(f' Check file: {blob.name}, type: {blob.content_type}, encoding: {blob.content_encoding}, compression: {file_compression}, last_modified: {last_modified}')
                    
                    if not start_date or last_modified >= start_date:
                        if file_compression == 'zip':
                            yield from ZipHelper(blob=blob).create_gcs_remote_instance()
                        else:
                            uri = blob.generate_signed_url(expiration=timedelta(hours=1), version="v4")
                            yield GCSRemoteFile(
                                uri=uri, 
                                last_modified=last_modified, 
                                file_name=blob.name,
                                file_type=blob.content_type,
                                file_endcoding=blob.content_encoding,
                                file_compression=file_compression
                            )
        except Exception as exc:
            self._handle_file_listing_error(exc, prefix, logger)

        
    def _handle_file_listing_error(self, exc: Exception, prefix: str, logger: logging.Logger):
        logger.error(f"Error while listing files: {str(exc)}")
        raise ErrorListingFiles(
            FileBasedSourceError.ERROR_LISTING_FILES,
            source="gcs",
            bucket=self.config.gcs_bucket,
            prefix=prefix,
        ) from exc

    def open_file(self, file: GCSRemoteFile, mode: FileReadMode, encoding: Optional[str], logger: logging.Logger) -> IOBase:
        """
        Open and yield a remote file from GCS for reading.
        """
        logger.debug(f' OPEN FILE {file}')
        if 'gz' in file.file_compression:
            compression = ".gz"
        else:
            compression = "disable"
        try:
            if file.file_endcoding == 'gzip':
                result = StringIO(requests.get(file.uri, headers={'Accept-Encoding': 'gzip'}).text)
            else:
                result = smart_open.open(
                    uri=file.uri, 
                    mode=mode.value, 
                    encoding=encoding, 
                    compression=compression
                )

        except Exception as e:
            raise e
        return result



class SourceGcsCustomCursor(DefaultFileBasedCursor):
    pass


class SourceGcsCustom(FileBasedSource):
    pass