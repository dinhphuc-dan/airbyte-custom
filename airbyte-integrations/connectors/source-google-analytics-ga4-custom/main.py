#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_google_analytics_ga4_custom import SourceGoogleAnalyticsGa4Custom

if __name__ == "__main__":
    source = SourceGoogleAnalyticsGa4Custom()
    launch(source, sys.argv[1:])
