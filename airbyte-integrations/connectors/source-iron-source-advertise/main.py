#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_iron_source_advertise import SourceIronSourceAdvertise

if __name__ == "__main__":
    source = SourceIronSourceAdvertise()
    launch(source, sys.argv[1:])
