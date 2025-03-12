from pydantic import AnyUrl, Field
from typing import List, Optional

from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from pydantic import BaseModel
from source_gcs_custom.utilities import SearchDateInFileName


class SourceGCSCustomSpec(AbstractFileBasedSpec):
    service_account: str = Field(
        title="Service Account Information.",
        airbyte_secret=True,
        description=(
            'Enter your Google Cloud <a href="https://cloud.google.com/iam/docs/'
            'creating-managing-service-account-keys#creating_service_account_keys">'
            "service account key</a> in JSON format"
        ),
        multiline=True,
        order=0,
    )

    gcs_bucket: str = Field(
        title="GCS bucket",
        description="GCS bucket name",
        order=1,
    )

    streams: List[FileBasedStreamConfig] = Field(
        title="The list of streams to sync",
        description='Each instance of this configuration defines a <a href="https://docs.airbyte.com/cloud/core-concepts#stream">stream</a>. Use this to define which files belong in the stream, their format, and how they should be parsed and validated. When sending data to warehouse destination such as Snowflake or BigQuery, each stream is a separate table.',
        order=2,
    )

    start_date: Optional[str] = Field(
        title="Start Date",
        description="UTC date and time in the format 2017-01-25T00:00:00. Any file modified before this date will not be replicated.",
        examples=["2025-01-01T00:00:00"],
        format="date-time",
        pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$",
        pattern_descriptor="YYYY-MM-DDTHH:mm:ss",
        order=3,
    )

    search_date_in_file_name: Optional[SearchDateInFileName] = Field(
        title="Scan only file has date in file name",
        description="An custom object for search date in file's name, to avoid scan all files in bucket",
        order=4,
    )

    @classmethod
    def documentation_url(cls) -> AnyUrl:
        """
        Returns the documentation URL.
        """
        return AnyUrl("https://docs.airbyte.com/integrations/sources/gcs", scheme="https")
    
    @staticmethod
    def replace_enum_allOf_and_anyOf(schema):
        """
        Field search_date_in_file_name is defined as allOf by pyndatics, which cannot show in airbyte UI
        Therefore, we have to remove allOf from pyndatics, and switch to schema that airbyte can understand
        """
        objects_to_check = schema["properties"]["search_date_in_file_name"]
        objects_old_title = objects_to_check["title"]
        property_object = objects_to_check['allOf'][0]
        objects_to_check.update(property_object)
        objects_to_check.pop("allOf")
        objects_to_check["title"]=objects_old_title

        return super(SourceGCSCustomSpec, SourceGCSCustomSpec).replace_enum_allOf_and_anyOf(schema)


    