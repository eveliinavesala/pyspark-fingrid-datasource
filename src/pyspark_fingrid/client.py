"""
HTTP client for the Fingrid open data API.

This is the single place that knows how to talk to Fingrid over HTTP:
request construction, pagination, rate limiting, and retry-with-backoff.
Neither `reader.py` nor `datasource.py` should build a `requests.get(...)`
call directly - both go through `FingridApiClient` so there is exactly one
implementation of "how we call the API" to test, log, and fix.

Fingrid's documented throttling policy, per API key:
    "you can make 10 000 requests in 24h period and one request every two
    seconds" - https://data.fingrid.fi/en/instructions

Status/progress is reported via the standard `logging` module rather than
`print()`, since this code runs on Spark executors as well as the driver;
printing directly interleaves badly with Spark's own console output across
processes, while a logger lets each environment configure verbosity and
routing as it sees fit.
"""

import contextlib
import logging
import time

import requests

from .exceptions import FingridApiError, FingridRateLimitError
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

BASE_URL = "https://data.fingrid.fi/api"

# Fingrid: max 1 request / 2 seconds per API key.
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 2.0
DEFAULT_MAX_RETRIES = 5
DEFAULT_INITIAL_BACKOFF_SECONDS = 5.0
DEFAULT_MAX_BACKOFF_SECONDS = 60.0
DEFAULT_PAGE_SIZE = 20000
DEFAULT_MAX_PAGES = 1000


class FingridApiClient:
    """Thin HTTP client for the Fingrid open data API.

    Owns request construction, throttling, and retry-with-backoff on 429
    responses. Deliberately knows nothing about PySpark, schemas, or
    DataFrames - see reader.py / datasource.py for that layer.
    """

    def __init__(
        self,
        api_key: str,
        min_request_interval_seconds: float | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        initial_backoff_seconds: float = DEFAULT_INITIAL_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
    ):
        self.api_key = api_key
        self.max_retries = max_retries
        self.initial_backoff_seconds = initial_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        # Resolved at call time (not as a default-argument value) so that
        # patching DEFAULT_MIN_REQUEST_INTERVAL_SECONDS (e.g. in tests) takes
        # effect for clients constructed after the patch, rather than being
        # frozen in at import/definition time.
        if min_request_interval_seconds is None:
            min_request_interval_seconds = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS
        self._rate_limiter = RateLimiter(min_request_interval_seconds)

    @property
    def _headers(self) -> dict[str, str]:
        return {"x-api-key": self.api_key}

    def _get_with_retry(self, url: str, params: dict) -> requests.Response:
        """GET a URL, retrying with exponential backoff on 429 responses.

        A single fixed-wait retry isn't reliable against Fingrid's rate
        limit (observed repeated 429s even on a second attempt), so this
        retries up to `max_retries` times, doubling the wait each time
        (capped at `max_backoff_seconds`), honoring a `Retry-After` header
        if the API sends one.
        """
        backoff = self.initial_backoff_seconds
        response = None

        for attempt in range(1, self.max_retries + 1):
            self._rate_limiter.wait()
            response = requests.get(url, headers=self._headers, params=params, timeout=60)

            if response.status_code != 429:
                return response

            retry_after = None
            with contextlib.suppress(AttributeError):
                retry_after = response.headers.get("Retry-After")
            try:
                wait_seconds = float(retry_after) if retry_after else backoff
            except (TypeError, ValueError):
                wait_seconds = backoff

            if attempt == self.max_retries:
                break

            logger.warning("Rate limited (attempt %d/%d). Waiting %.0fs...", attempt, self.max_retries, wait_seconds)
            time.sleep(wait_seconds)
            backoff = min(backoff * 2, self.max_backoff_seconds)

        return response

    def fetch_metadata(self, dataset_id: int) -> dict | None:
        """Fetch dataset metadata (name, unit, update cadence, ...).

        Metadata is a non-critical, best-effort call: a missing or failed
        metadata fetch degrades to `None` rather than raising, since
        reading actual data doesn't depend on it, but persistent rate
        limiting on this endpoint still raises `FingridRateLimitError`, so
        callers can tell "not available this time" apart from "we're being
        throttled."
        """
        url = f"{BASE_URL}/datasets/{dataset_id}"
        try:
            response = self._get_with_retry(url, params={})
        except requests.RequestException as e:
            raise FingridApiError(f"Error fetching metadata for dataset {dataset_id}: {e}") from e

        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:
            raise FingridRateLimitError(
                f"Rate limited fetching metadata for dataset {dataset_id} after {self.max_retries} attempts",
                status_code=429,
            )

        logger.warning("Could not fetch metadata for dataset %s (status: %s)", dataset_id, response.status_code)
        return None

    def fetch_data(
        self,
        dataset_id: int,
        start_time: str,
        end_time: str,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> list[dict]:
        """Fetch all pages of dataset data for the given time range.

        The Fingrid API paginates results (see `pagination.total` /
        `pagination.lastPage` in the response). This walks every page and
        concatenates the records, so callers always get the complete result
        set for the requested time range rather than just the first page.

        Returns an empty list when the API legitimately has no data for the
        range. Raises `FingridApiError` / `FingridRateLimitError` on
        failure, so callers can distinguish "no data" from "the request
        failed" instead of both collapsing to `None`.
        """
        url = f"{BASE_URL}/datasets/{dataset_id}/data"
        all_records: list[dict] = []
        page = 1
        total_records = None
        pagination: dict = {}

        while True:
            params = {
                "startTime": start_time,
                "endTime": end_time,
                "format": "json",
                "page": page,
                "pageSize": page_size,
            }

            logger.debug("Calling %s (page %d)", url, page)

            try:
                response = self._get_with_retry(url, params)
            except requests.RequestException as e:
                raise FingridApiError(f"Error fetching dataset {dataset_id} data (page {page}): {e}") from e

            if response.status_code == 429:
                raise FingridRateLimitError(
                    f"Rate limited fetching dataset {dataset_id} data (page {page}) after {self.max_retries} attempts",
                    status_code=429,
                )

            if response.status_code != 200:
                raise FingridApiError(
                    f"API error {response.status_code} fetching dataset {dataset_id} data "
                    f"(page {page}): {response.text}",
                    status_code=response.status_code,
                    response_text=response.text,
                )

            response_data = response.json()
            if "data" not in response_data:
                raise FingridApiError(
                    f"Unexpected response structure fetching dataset {dataset_id}: {list(response_data.keys())}"
                )

            page_records = response_data["data"]
            pagination = response_data.get("pagination", {}) or {}

            logger.debug("Retrieved %d records on page %d", len(page_records), page)

            if total_records is None and pagination.get("total") is not None:
                total_records = pagination["total"]

            all_records.extend(page_records)

            last_page = pagination.get("lastPage")
            if last_page is not None:
                has_more = page < last_page
            elif total_records is not None:
                has_more = len(all_records) < total_records
            else:
                # No pagination info available at all: infer from whether
                # this page came back full (likely another page follows).
                has_more = len(page_records) >= page_size

            if not page_records or not has_more:
                break

            page += 1
            if page > max_pages:
                logger.warning(
                    "Reached max_pages (%d) for dataset %s; stopping pagination early.", max_pages, dataset_id
                )
                break

        logger.debug("Retrieved %d total records across %d page(s)", len(all_records), page)
        return all_records
