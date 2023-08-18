#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_unity_ads_advertise import SourceUnityAdsAdvertise

if __name__ == "__main__":
    source = SourceUnityAdsAdvertise()
    launch(source, sys.argv[1:])
