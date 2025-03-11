#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch, AirbyteEntrypoint
from source_gcs_custom import SourceGcsCustom, SourceGCSCustomSpec, SourceGCSCustomStreamReader, SourceGcsCustomCursor

if __name__ == "__main__":
    _args = sys.argv[1:]

    catalog_path = AirbyteEntrypoint.extract_catalog(_args)
    config_path = AirbyteEntrypoint.extract_config(_args)
    state_path = AirbyteEntrypoint.extract_state(_args)
    
    source = SourceGcsCustom(
        stream_reader=SourceGCSCustomStreamReader(),
        spec_class=SourceGCSCustomSpec,
        catalog=SourceGcsCustom.read_catalog(catalog_path) if catalog_path else None,
        config=SourceGcsCustom.read_config(config_path) if config_path else None,
        state=SourceGcsCustom.read_state(state_path) if state_path else None,
        cursor_cls=SourceGcsCustomCursor,
    )

    launch(source, sys.argv[1:])
