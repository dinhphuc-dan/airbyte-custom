#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_tiktok_ads_custom import SourceTiktokAdsCustom

if __name__ == "__main__":
    source = SourceTiktokAdsCustom()
    launch(source, sys.argv[1:])
