"""
Exception hierarchy for pyspark_fingrid.

Both the legacy `read_fingrid_data()` entry point and the `format("fingrid")`
DataSource entry point raise from this module, so there is one place that
defines what can go wrong and how it's reported, rather than each entry
point inventing its own error strings.

`FingridConfigError` and `FingridSchemaError` also subclass `ValueError` for
backward compatibility with code (and tests) written against the pre-split
API, which raised plain `ValueError` for bad options and unsupported
datasets.
"""


class FingridError(Exception):
    """Base class for all errors raised by pyspark_fingrid."""


class FingridConfigError(FingridError, ValueError):
    """Invalid or missing configuration: a bad/missing option, an invalid
    time range, an unparsable dataset ID, and similar caller-fixable
    problems that don't require contacting the Fingrid API at all."""


class FingridSchemaError(FingridError, ValueError):
    """A dataset ID has no registered schema, or a raw API record could not
    be transformed into that schema's expected shape."""


class FingridApiError(FingridError):
    """The Fingrid API returned an error response, or the request itself
    failed (network error, unexpected response shape, etc.)."""

    def __init__(self, message: str, status_code: int | None = None, response_text: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class FingridRateLimitError(FingridApiError):
    """Raised when retries were exhausted while still being rate limited
    (HTTP 429) by the Fingrid API. See the client's rate limiting docs for
    Fingrid's documented policy: https://data.fingrid.fi/en/instructions."""
