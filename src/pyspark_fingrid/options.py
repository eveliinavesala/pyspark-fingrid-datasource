"""
Shared, validated options for a Fingrid read.

`read_fingrid_data()` and the `format("fingrid")` DataSource previously each
parsed and defaulted their own options (dataset ID, time range, page size,
...), which meant the same default (e.g. "30 minutes lookback") and the same
validation (e.g. "endTime must be after startTime") were implemented twice
and could drift apart. `FingridReadOptions` is the one place that happens
now; both entry points construct one and read from it.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .exceptions import FingridConfigError

DEFAULT_LOOKBACK_MINUTES = 30
DEFAULT_PARTITION_DAYS = 1
DEFAULT_PAGE_SIZE = 20000


def parse_option_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 'Z' timestamp into a timezone-aware datetime."""
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def format_timestamp(value: datetime) -> str:
    """Format a timezone-aware datetime back into the API's expected string form."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class FingridReadOptions:
    """Parsed, validated options for a single Fingrid read."""

    api_key: str
    dataset_id: int
    start_time: datetime
    end_time: datetime
    page_size: int = DEFAULT_PAGE_SIZE
    partition_days: int = DEFAULT_PARTITION_DAYS

    @property
    def start_time_str(self) -> str:
        return format_timestamp(self.start_time)

    @property
    def end_time_str(self) -> str:
        return format_timestamp(self.end_time)

    @classmethod
    def from_dict(cls, options: dict[str, str]) -> "FingridReadOptions":
        """Build from a Spark-style string-keyed/string-valued options dict
        (`spark.read.format("fingrid").option(...)`)."""
        api_key = options.get("apiKey")
        if not api_key:
            raise FingridConfigError("option 'apiKey' is required, e.g. .option('apiKey', 'your-key')")

        raw_dataset_id = options.get("datasetId")
        if raw_dataset_id is None:
            raise FingridConfigError("option 'datasetId' is required, e.g. .option('datasetId', 192)")
        try:
            dataset_id = int(raw_dataset_id)
        except (TypeError, ValueError) as e:
            raise FingridConfigError(f"option 'datasetId' must be an integer, got: {raw_dataset_id!r}") from e

        page_size = int(options.get("pageSize", DEFAULT_PAGE_SIZE))
        partition_days = int(options.get("partitionDays", DEFAULT_PARTITION_DAYS))
        if partition_days <= 0:
            raise FingridConfigError("option 'partitionDays' must be a positive integer")

        start_time, end_time = cls._resolve_time_range(options.get("startTime"), options.get("endTime"))

        return cls(
            api_key=api_key,
            dataset_id=dataset_id,
            start_time=start_time,
            end_time=end_time,
            page_size=page_size,
            partition_days=partition_days,
        )

    @classmethod
    def from_args(
        cls,
        api_key: str,
        dataset_id: int,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> "FingridReadOptions":
        """Build from `read_fingrid_data()`'s positional/typed arguments.

        Unlike `from_dict`, `dataset_id` must already be a real `int` here
        (Spark options are always strings, so `from_dict` casts; a direct
        Python call has no such excuse, so this keeps the stricter check).
        """
        if not api_key:
            raise FingridConfigError("api_key is required")
        if not isinstance(dataset_id, int):
            raise FingridConfigError("dataset_id must be an integer")

        resolved_start, resolved_end = cls._resolve_time_range(start_time, end_time)

        return cls(
            api_key=api_key,
            dataset_id=dataset_id,
            start_time=resolved_start,
            end_time=resolved_end,
        )

    @staticmethod
    def _resolve_time_range(start_time: str | None, end_time: str | None) -> tuple[datetime, datetime]:
        """Parse optional start/end strings, defaulting to the last
        DEFAULT_LOOKBACK_MINUTES minutes (in UTC) when omitted, and
        validate the resulting range."""
        now = datetime.now(timezone.utc)
        resolved_start = (
            parse_option_timestamp(start_time) if start_time else now - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
        )
        resolved_end = parse_option_timestamp(end_time) if end_time else now

        if resolved_end <= resolved_start:
            raise FingridConfigError("endTime must be after startTime")

        return resolved_start, resolved_end
