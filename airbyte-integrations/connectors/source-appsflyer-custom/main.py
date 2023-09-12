#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_appsflyer_custom import SourceAppsflyerCustom

if __name__ == "__main__":
    source = SourceAppsflyerCustom()
    launch(source, sys.argv[1:])
