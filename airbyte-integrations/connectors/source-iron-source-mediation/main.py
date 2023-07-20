#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_iron_source_mediation import SourceIronSourceMediation

if __name__ == "__main__":
    source = SourceIronSourceMediation()
    launch(source, sys.argv[1:])
