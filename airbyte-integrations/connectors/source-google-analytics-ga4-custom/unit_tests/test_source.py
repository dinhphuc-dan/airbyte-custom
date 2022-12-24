#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from source_google_analytics_ga4_custom.source import SourceGoogleAnalyticsGa4Custom


def test_check_connection(mocker):
    source = SourceGoogleAnalyticsGa4Custom()
    logger_mock, config_mock = MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (True, None)


def test_streams(mocker):
    source = SourceGoogleAnalyticsGa4Custom()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    # TODO: replace this with your streams number
    expected_streams_number = 2
    assert len(streams) == expected_streams_number
