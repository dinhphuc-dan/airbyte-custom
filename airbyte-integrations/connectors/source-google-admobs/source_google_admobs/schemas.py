#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

networkschema =  {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": [],
  "properties": {
    "uuid": {"type": ["string"], "description": "Custom unique identifier for each record, to support primary key"},
    "DATE": {"type": ["null", "string"]},
    "APP": {"type": ["null", "string"]},
    "APP_NAME":{"type": ["null", "string"]},
    "AD_UNIT": {"type": ["null", "string"]},
    "AD_UNIT_NAME": {"type": ["null", "string"]},
    "COUNTRY":{"type": ["null", "string"]},
    "FORMAT":{"type": ["null", "string"]},
    "PLATFORM":{"type": ["null", "string"]},
    "MOBILE_OS_VERSION":{"type": ["null", "string"]},
    "APP_VERSION_NAME":{"type": ["null", "string"]},
    "SERVING_RESTRICTION":{"type": ["null", "string"]},
    "GMA_SDK_VERSION":{"type": ["null", "string"]},
    "AD_REQUESTS":{"type": ["null", "number"]},
    "MATCHED_REQUESTS":{"type": ["null", "number"]},
    "CLICKS":{"type": ["null", "number"]},
    "ESTIMATED_EARNINGS":{"type": ["null", "number"]},
    "IMPRESSIONS":{"type": ["null", "number"]}
  }
}

listappsschema = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": [],
  "properties": {
    "app_name": {"type": ["null", "string"]},
    "app_id": {"type": ["null", "string"]},
    "app_id_full": {"type": ["null", "string"]}
  }
}


def network_report_schema() -> dict:
    return networkschema

def list_apps_schema() -> dict:
    return listappsschema

    # "SHOW_RATE":{"type": ["null", "number"]},
    # "MATCH_RATE":{"type": ["null", "number"]},
    # "IMPRESSION_CTR":{"type": ["null", "number"]},
    # "IMPRESSION_RPM":{"type": ["null", "number"]},

