"""
Pytest configuration and shared fixtures for tests.

This module contains shared test fixtures and configuration
for the test suite.
"""

from unittest.mock import Mock, patch

import pytest


@pytest.fixture(autouse=True)
def _disable_request_throttle(monkeypatch):
    """Disable the real inter-request throttle (Fingrid: 1 req/2s) during tests.

    Each `FingridApiClient` owns its own `RateLimiter` instance now (rather
    than a module-level global), so tests that construct a client directly
    naturally get an isolated rate limiter. This fixture only needs to lower
    the *default* interval client.py falls back to, for tests exercising
    code paths (like `read_fingrid_data()`) that construct a client
    themselves without specifying one. Tests that specifically want to
    assert on `time.sleep` calls (e.g. retry-backoff behavior) patch
    `time.sleep` themselves, which still works fine with this fixture active.
    """
    monkeypatch.setattr("pyspark_fingrid.client.DEFAULT_MIN_REQUEST_INTERVAL_SECONDS", 0.0)


@pytest.fixture
def mock_spark_session():
    """Mock Spark session for testing."""
    with patch('pyspark.sql.SparkSession.getActiveSession') as mock:
        spark_session = Mock()
        mock.return_value = spark_session
        yield spark_session


@pytest.fixture
def sample_metadata():
    """Sample metadata response from Fingrid API."""
    return {
        'id': 192,
        'nameEn': 'Electricity production in Finland - real-time data',
        'unitEn': 'MW',
        'updateCadenceEn': '3 min',
        'descriptionEn': 'Real-time electricity production data',
    }


@pytest.fixture
def sample_production_data():
    """Sample production data response from Fingrid API."""
    return [
        {
            'datasetId': 192,
            'startTime': '2024-07-24T12:00:00.000Z',
            'endTime': '2024-07-24T12:03:00.000Z',
            'value': 6789.5,
        },
        {
            'datasetId': 192,
            'startTime': '2024-07-24T12:03:00.000Z',
            'endTime': '2024-07-24T12:06:00.000Z',
            'value': 6812.3,
        },
    ]


@pytest.fixture
def sample_shortage_data():
    """Sample shortage status data response from Fingrid API."""
    return [
        {'datasetId': 336, 'startTime': '2024-07-24T12:00:00.000Z', 'endTime': '2024-07-24T12:03:00.000Z', 'value': 0},
        {'datasetId': 336, 'startTime': '2024-07-24T12:03:00.000Z', 'endTime': '2024-07-24T12:06:00.000Z', 'value': 1},
    ]


@pytest.fixture
def sample_consumption_data():
    """Sample consumption data response with additionalJson."""
    return [
        {
            'datasetId': 363,
            'startTime': '2024-07-24T12:00:00.000Z',
            'endTime': '2024-07-24T13:00:00.000Z',
            'value': 3502751.77,
            'additionalJson': {
                'TimeSeriesType': 'CTT_SUM_CONS_ACP',
                'Res': 'PT1H',
                'Uom': 'KWH',
                'ReadTS': '2024-07-24T12:00:00Z',
                'Value': '3502751.77',
                'Count': '3600745',
            },
        }
    ]


@pytest.fixture
def mock_api_response():
    """Mock API response structure."""

    def _make_response(data, pagination=None):
        if pagination is None:
            pagination = {'total': len(data), 'currentPage': 1}
        return {'data': data, 'pagination': pagination}

    return _make_response


# Test configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
