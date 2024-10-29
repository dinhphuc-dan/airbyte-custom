#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_content_server_by_trangntt import SourceContentServerByTrangntt

if __name__ == "__main__":
    source = SourceContentServerByTrangntt()
    launch(source, sys.argv[1:])
