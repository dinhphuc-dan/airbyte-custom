#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_google_play_console import SourceGooglePlayConsole

if __name__ == "__main__":
    source = SourceGooglePlayConsole()
    launch(source, sys.argv[1:])
