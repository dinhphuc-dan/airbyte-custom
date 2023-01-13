#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_volio_store import SourceVolioStore

if __name__ == "__main__":
    source = SourceVolioStore()
    launch(source, sys.argv[1:])
