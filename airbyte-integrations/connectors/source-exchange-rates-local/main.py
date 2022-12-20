#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_exchange_rates_local import SourceExchangeRatesLocal

if __name__ == "__main__":
    source = SourceExchangeRatesLocal()
    launch(source, sys.argv[1:])
