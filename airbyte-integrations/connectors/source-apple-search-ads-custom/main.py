#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_apple_search_ads_custom import SourceAppleSearchAdsCustom

if __name__ == "__main__":
    source = SourceAppleSearchAdsCustom()
    launch(source, sys.argv[1:])
