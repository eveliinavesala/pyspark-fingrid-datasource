# pyspark-fingrid-datasource

Access [Fingrid](https://data.fingrid.fi/en/) open data (Finnish electricity market and grid data) as PySpark DataFrames.

## Installation

```bash
poetry install
```

Requires PySpark >= 4.1.1 and an API key from https://data.fingrid.fi/en/.

## Usage

### Recommended: `spark.read.format("fingrid")`

Implemented using the [PySpark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html). Reads run per-partition (one partition per day of the requested range, by default), and each partition's pages are fetched from the API in full.

```python
from pyspark.sql import SparkSession
from pyspark_fingrid import register

spark = SparkSession.builder.appName("fingrid").getOrCreate()
register(spark)

df = (
    spark.read.format("fingrid")
    .option("apiKey", "your-api-key")
    .option("datasetId", 192)
    .option("startTime", "2024-07-24T00:00:00Z")  # optional, defaults to 30 min ago
    .option("endTime", "2024-07-25T00:00:00Z")  # optional, defaults to now
    .option("partitionDays", 1)  # optional, size of each partition's time slice
    .option("pageSize", 20000)  # optional, records requested per API page
    .load()
)

df.show()
```

### Legacy: `read_fingrid_data()`

A simpler, driver-side helper that builds the full result in memory before creating the DataFrame. Kept for backward compatibility; prefer the `format("fingrid")` API above for anything beyond small/ad hoc reads.

```python
from pyspark_fingrid import read_fingrid_data

df = read_fingrid_data("your-api-key", 192)  # dataset_id 192
df.show()
```

## Supported datasets

| Dataset ID | Description |
|---|---|
| 192 | Electricity production in Finland (real-time, MW) |
| 336 | Electricity shortage status |
| 363 | Electricity consumption in Finland |

```python
from pyspark_fingrid import list_available_datasets

list_available_datasets()
```

## Rate limiting

Fingrid throttles the API per key: **10,000 requests / 24h** and **one request every 2 seconds** ([official docs](https://data.fingrid.fi/en/instructions)). Exceeding this returns `HTTP 429`.

`FingridApiClient` (`client.py`) enforces the 2-second spacing itself before every request, via a `RateLimiter` instance, and retries `429`s with exponential backoff (honoring a `Retry-After` header if present) as a safety net. In practice this means:

- **`read_fingrid_data()`** (single process, sequential requests): fully rate-limit-safe by construction.
- **`spark.read.format("fingrid")`**: each Spark *partition* constructs its own `FingridApiClient` and enforces the 2-second spacing *within itself*, but a rate limiter can't coordinate *across* partitions running concurrently on different executor cores. If you see repeated `429`s with this API:
    - Increase `partitionDays` (fewer, larger partitions → fewer total requests and less concurrency), or
    - Limit Spark's parallelism while using this source, e.g. `SparkSession.builder.master("local[1]")` for local testing, or
    - Reduce `spark.executor.cores` / task slots in a cluster setting.

## Architecture

The package is split by concern rather than by entry point, so `read_fingrid_data()` and `format("fingrid")` share one implementation of each concern instead of each maintaining its own:

| Module | Responsibility |
|---|---|
| `client.py` | HTTP requests, pagination, rate limiting, retry/backoff (`FingridApiClient`) |
| `rate_limiter.py` | Minimum-interval throttling, one instance per client (`RateLimiter`) |
| `options.py` | Parsing, defaulting, and validating read options from either entry point (`FingridReadOptions`) |
| `transform.py` | Turning raw API records into schema `Row`s, skipping and logging failures (`transform_records`) |
| `exceptions.py` | Shared error types (`FingridConfigError`, `FingridApiError`, `FingridRateLimitError`, `FingridSchemaError`) |
| `schemas/` | Per-dataset schema definitions and the schema registry |
| `reader.py` | `read_fingrid_data()`: thin orchestration on top of the above, single-shot on the driver |
| `datasource.py` | `format("fingrid")`: thin orchestration on top of the above, partitioned across executors |

Errors are typed rather than generic. `FingridConfigError` and `FingridSchemaError` subclass `ValueError` for backward compatibility with code written against the pre-split API. Note one behavioral difference between the two entry points: `read_fingrid_data()` catches fetch failures and returns `None`, while a `format("fingrid")` partition lets a `FingridApiError` propagate and fail the Spark task, so a partition that couldn't be fetched surfaces as a job failure rather than silently producing an incomplete result.

## Development

```bash
poetry install --with dev
poetry run pytest tests/ -v
```