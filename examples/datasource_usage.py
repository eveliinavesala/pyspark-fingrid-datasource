"""
Usage examples for the "fingrid" PySpark DataSource (Python Data Source API).

This is the recommended way to read Fingrid data: it registers a proper
`DataSource`/`DataSourceReader` with Spark so reads go through
`spark.read.format("fingrid")...load()`, with the requested time range split
into per-day partitions (configurable via `partitionDays`) that are read
independently, rather than being built as a single Python list on the driver.

Compare with `basic_usage.py`, which uses the simpler legacy
`read_fingrid_data()` helper.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from pyspark_fingrid import register


def main():
    """Run DataSource API usage examples."""
    # NOTE: master("local[1]") is intentional here, not a default we recommend
    # blindly. Fingrid allows only 1 request/2s per API key (see README
    # "Rate limiting" section), and this library's throttle only coordinates
    # requests *within* a single process/partition - it can't stop multiple
    # Spark partitions running concurrently (e.g. under local[*]) from
    # colliding on that same 2-second window. Running this demo single
    # threaded keeps Example 2's 3 partitions sequential, avoiding that.
    # For your own workloads, prefer tuning `partitionDays` (fewer, larger
    # partitions) over cranking parallelism down permanently.
    spark = (
        SparkSession.builder.appName("Fingrid DataSource Usage")
        .master("local[1]")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    # Register the "fingrid" format with this SparkSession.
    register(spark)

    api_key = os.getenv("FINGRID_API_KEY")
    if not api_key:
        print("❌ Please set FINGRID_API_KEY environment variable")
        print("   Get your API key from: https://data.fingrid.fi/en/")
        spark.stop()
        return

    print("🚀 PySpark Fingrid DataSource - Usage Examples")
    print("=" * 60)

    # Example 1: Simple read using option defaults (last 30 minutes).
    print("\n" + "=" * 20 + " EXAMPLE 1 " + "=" * 20)
    print("📊 Reading electricity production data (last 30 minutes)")

    df_production = spark.read.format("fingrid").option("apiKey", api_key).option("datasetId", 192).load()
    df_production.printSchema()
    df_production.show(5, truncate=False)
    print(f"📈 Total records: {df_production.count()}")

    # Example 2: Explicit time range spanning multiple days, split into
    # one partition per day so each day's pages are fetched independently.
    # NOTE: with local[1] above, these 3 partitions run one at a time, so
    # expect this step to take a few seconds (3 partitions x ~2s spacing).
    print("\n" + "=" * 20 + " EXAMPLE 2 " + "=" * 20)
    print("📅 Reading 3 days of production data, partitioned by day")

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=3)

    df_multi_day = (
        spark.read.format("fingrid")
        .option("apiKey", api_key)
        .option("datasetId", 192)
        .option("startTime", start_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        .option("endTime", end_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        .option("partitionDays", 1)
        .load()
    )
    print(f"📊 Partitions used: {df_multi_day.rdd.getNumPartitions()}")
    print(f"📈 Total records: {df_multi_day.count()}")

    # Example 3: Reading the shortage-status dataset with SQL.
    print("\n" + "=" * 20 + " EXAMPLE 3 " + "=" * 20)
    print("⚠️  Reading electricity shortage status via SQL")

    df_shortage = spark.read.format("fingrid").option("apiKey", api_key).option("datasetId", 336).load()
    df_shortage.createOrReplaceTempView("shortage")
    spark.sql("""
              SELECT shortage_status, shortage_status_description, COUNT(*) AS count
              FROM shortage
              GROUP BY shortage_status, shortage_status_description
              ORDER BY shortage_status
              """).show(truncate=False)

    print("\n" + "=" * 60)
    print("✅ Examples completed!")

    spark.stop()
    print("\n🛑 Spark session stopped")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted - stopping...")
        try:
            from pyspark.sql import SparkSession as _SparkSession

            active = _SparkSession.getActiveSession()
            if active is not None:
                active.stop()
        except Exception:
            pass
        sys.exit(130)
