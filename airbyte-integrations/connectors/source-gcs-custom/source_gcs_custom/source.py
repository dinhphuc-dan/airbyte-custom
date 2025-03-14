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
from datetime import datetime

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
                start_date: date = pendulum.now(self.config.search_date_in_file_name.timezone)

                # support call backward day, hour and month
                if self.config.search_date_in_file_name.backward_type == 'day':
                    start_date = start_date.start_of('day').subtract(days=self.config.search_date_in_file_name.number_days_backward)
                elif self.config.search_date_in_file_name.backward_type == 'hour':
                    start_date = start_date.start_of('hour').subtract(hours=self.config.search_date_in_file_name.number_days_backward)
                elif self.config.search_date_in_file_name.backward_type == 'month':
                    start_date = start_date.start_of('month').subtract(months=self.config.search_date_in_file_name.number_days_backward)
                else:
                    raise ValueError("Backward Type not one of day, hour or month")

                for i in range(0, self.config.search_date_in_file_name.number_days_backward + 1):
                    for glob in user_defined_globs:
                        if self.config.search_date_in_file_name.backward_type == 'day':
                            final_glob = glob + (f'{start_date.add(days=i).format(fmt=self.config.search_date_in_file_name.date_in_file_name_format)}**')
                        elif self.config.search_date_in_file_name.backward_type == 'hour':
                            final_glob = glob + (f'{start_date.add(hours=i).format(fmt=self.config.search_date_in_file_name.date_in_file_name_format)}**')
                        elif self.config.search_date_in_file_name.backward_type == 'month':
                            final_glob = glob + (f'{start_date.add(months=i).format(fmt=self.config.search_date_in_file_name.date_in_file_name_format)}**')

                        final_globs.append(final_glob)
                
            else: 
                final_globs = user_defined_globs
                start_date = pendulum.instance(datetime.strptime(self.config.start_date, self.DATE_TIME_FORMAT)) if self.config and self.config.start_date else None
                
            final_globs = sorted(list(set(final_globs)))
            logger.info(f'GLOBS: {final_globs}')
            for glob in final_globs:
                bucket: storage.Bucket = self.gcs_client.get_bucket(self.config.gcs_bucket)
                blobs: storage.Blob = bucket.list_blobs(match_glob=glob)
                for blob in blobs:
                    last_modified = pendulum.instance(dt=blob.updated).in_tz(tz=self.config.search_date_in_file_name.timezone)
                    file_compression = blob.name.split(".")[-1]

                    logger.debug(f' Check file: {blob.name}, type: {blob.content_type}, encoding: {blob.content_encoding}, compression: {file_compression}, last_modified: {last_modified}')
                    
                    if not start_date or last_modified >= start_date:
                        signed_uri = blob.generate_signed_url(expiration=timedelta(hours=1), version="v4")
                        if file_compression == 'zip':
                            yield from ZipHelper(blob=blob).create_gcs_remote_instance()
                        else:
                            yield GCSRemoteFile(
                                uri=signed_uri, 
                                last_modified=last_modified, 
                                file_name=blob.name,
                                file_type=blob.content_type,
                                file_gcs_encoding=blob.content_encoding,
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

        if "charset" in file.file_type:
            file_encoding = file.file_type.split(";")[1].split("=")[1]
        else: 
            file_encoding = encoding
        

        if (file.file_compression == 'gz' and file.file_gcs_encoding != 'gzip'):
            compression = ".gz"
        else:
            compression = "disable"
        try:            
            result = smart_open.open(
                uri=file.uri, 
                mode=mode.value, 
                encoding=file_encoding, 
                compression=compression
            )

        except Exception as e:
            raise e
        return result



class SourceGcsCustomCursor(DefaultFileBasedCursor):
    pass


class SourceGcsCustom(FileBasedSource):
    pass