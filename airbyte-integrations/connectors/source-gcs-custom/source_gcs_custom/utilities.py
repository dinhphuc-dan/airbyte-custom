from datetime import datetime
from typing import Optional, Iterable
from pydantic import BaseModel, Field
from google.cloud import storage
import tempfile
from io import BytesIO
import zipfile
import os
import pendulum

# additional class for spec to search date in file name instead of call all files in bucket
class SearchDateInFileName(BaseModel):
    get_last_x_days_files_based_on_date_in_file_name: bool = Field(
        description="Scan only file has date value in its name and that value in last X days (X defined below), to reduce ammount of time scan files and read files when bucket contains a myriad of files. Start Date and Days to Sync will be ignored when this option is enabled.",
        default=False,
        order=1
    )
    number_days_backward: int = Field(
        description="Number of days backward to get data",
        default=7,
        order=2
    )
    date_in_file_name_format: str = Field(
        description="Date in file's name format. Follow pendulum format at <a href='https://pendulum.eustace.io/docs/#tokens'>here</a>",
        default='YYYY-MM-DD',
        order=3
    )
    timezone: str = Field(
        description="Date in file's name timezone. Default is UTC, get timezone name in <a href='https://en.wikipedia.org/wiki/List_of_tz_database_time_zones'>here</a>",
        default='UTC',
        order=4
    )

# class re-define the RemoteFile in airbyte cdk 
class GCSRemoteFile(BaseModel):
    """
    A file in a file-based stream.
    """

    uri: str
    last_modified: datetime
    file_name : str
    file_type: Optional[str] = None
    file_endcoding: Optional[str] = None
    file_compression: Optional[str] = None


# class for unzip file zip to a temporay file and return a GCSRemoteFile object
class ZipHelper():
    def __init__(self, blob: storage.Blob, tmp_dir = tempfile.TemporaryDirectory()):
        self._blob = blob
        self._tmp_dir = tmp_dir
    
    def _download_and_extract_zipfile_from_blob(self) -> None:
        file_as_bytes = self._blob.download_as_bytes()
        with BytesIO(file_as_bytes) as fb:
            with zipfile.ZipFile(fb, mode='r') as zf:
                zf.extractall(path=self._tmp_dir.name)
    
    def create_gcs_remote_instance(self) ->Iterable[GCSRemoteFile]:
        self._download_and_extract_zipfile_from_blob()

        for unzipped_file in os.listdir(self._tmp_dir.name):
            file_compression = unzipped_file.split(".")[-1]
            yield GCSRemoteFile(
                uri=os.path.join(self._tmp_dir.name, unzipped_file), 
                last_modified=pendulum.instance(dt=self._blob.updated), 
                file_name=unzipped_file,
                file_type=self._blob.content_type,
                file_endcoding=self._blob.content_encoding,
                file_compression=file_compression
            )
