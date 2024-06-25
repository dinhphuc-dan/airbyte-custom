#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_x_ads_twitter_custom import SourceXAdsTwitterCustom

if __name__ == "__main__":
    source = SourceXAdsTwitterCustom()
    launch(source, sys.argv[1:])
