"""
Basic usage examples for PySpark Fingrid Data Source.

This script demonstrates how to use the library to read different
types of Fingrid datasets and perform basic operations.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark_fingrid import list_available_datasets, read_fingrid_data


def main():
    """Run basic usage examples."""
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Fingrid Basic Usage").config("spark.sql.adaptive.enabled", "true").getOrCreate()
    )

    # Get API key from environment variable
    api_key = os.getenv('FINGRID_API_KEY')
    if not api_key:
        print("❌ Please set FINGRID_API_KEY environment variable")
        print("   Get your API key from: https://data.fingrid.fi/en/")
        return

    print("🚀 PySpark Fingrid Data Source - Basic Usage Examples")
    print("=" * 60)

    # Show available datasets
    print("\n📋 Available Datasets:")
    list_available_datasets()

    # Example 1: Read electricity production data (default time range)
    print("\n" + "=" * 20 + " EXAMPLE 1 " + "=" * 20)
    print("📊 Reading electricity production data (last 30 minutes)")

    try:
        df_production = read_fingrid_data(api_key, 192)

        if df_production:
            print("\n✅ Success! Schema:")
            df_production.printSchema()

            print("\n📊 Sample data:")
            df_production.show(5, truncate=False)

            print(f"\n📈 Total records: {df_production.count()}")

            # Basic statistics
            print("\n📊 Production statistics:")
            df_production.select("production_mw").describe().show()

        else:
            print("❌ No data returned")

    except Exception as e:
        print(f"❌ Error reading production data: {e}")

    # Example 2: Read electricity shortage status
    print("\n" + "=" * 20 + " EXAMPLE 2 " + "=" * 20)
    print("⚠️  Reading electricity shortage status")

    try:
        df_shortage = read_fingrid_data(api_key, 336)

        if df_shortage:
            print("\n✅ Success! Schema:")
            df_shortage.printSchema()

            print("\n📊 Sample data:")
            df_shortage.show(5, truncate=False)

            # Show status distribution
            print("\n📊 Status distribution:")
            df_shortage.groupBy("shortage_status", "shortage_status_description").count().show(truncate=False)

        else:
            print("❌ No data returned")

    except Exception as e:
        print(f"❌ Error reading shortage data: {e}")

    # Example 3: Read data with custom time range
    print("\n" + "=" * 20 + " EXAMPLE 3 " + "=" * 20)
    print("⏰ Reading production data with custom time range (last 2 hours)")

    try:
        # Define time range
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=2)

        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f"📅 Time range: {start_time_str} to {end_time_str}")

        df_custom = read_fingrid_data(api_key, 192, start_time_str, end_time_str)

        if df_custom:
            print(f"\n📊 Records in 2-hour range: {df_custom.count()}")

            # Show time range of data
            time_stats = df_custom.agg(
                F.min("startTime").alias("min_start_time"),
                F.max("startTime").alias("max_start_time"),
                F.avg("production_mw").alias("avg_production_mw"),
            ).collect()[0]

            print(f"📊 Data from: {time_stats[0]} to {time_stats[1]}")
            print(f"📊 Average production: {time_stats[2]:.2f} MW")

        else:
            print("❌ No data returned")

    except Exception as e:
        print(f"❌ Error reading custom range data: {e}")

    # Example 4: Basic analysis combining datasets
    print("\n" + "=" * 20 + " EXAMPLE 4 " + "=" * 20)
    print("🔬 Basic analysis: Production trends")

    try:
        # Get recent data
        df_recent = read_fingrid_data(api_key, 192)

        if df_recent and df_recent.count() > 1:
            # Register as temporary view for SQL analysis
            df_recent.createOrReplaceTempView("production")

            # SQL analysis
            trend_analysis = spark.sql("""
                                       SELECT
                                           startTime,
                                           production_mw,
                                           production_mw - LAG(production_mw) OVER (ORDER BY startTime) as change_mw,
                                           CASE
                                               WHEN production_mw > LAG(production_mw) OVER (ORDER BY startTime) THEN 'Increasing'
                                               WHEN production_mw < LAG(production_mw) OVER (ORDER BY startTime) THEN 'Decreasing'
                                               ELSE 'Stable'
                                               END as trend
                                       FROM production
                                       ORDER BY startTime
                                       """)

            print("\n📈 Production trends:")
            trend_analysis.show(10, truncate=False)

            # Trend summary
            print("\n📊 Trend summary:")
            trend_summary = spark.sql("""
                                      SELECT
                                          trend,
                                          COUNT(*) as count,
                    ROUND(AVG(change_mw), 2) as avg_change_mw
                                      FROM (
                                          SELECT
                                          production_mw - LAG(production_mw) OVER (ORDER BY startTime) as change_mw,
                                          CASE
                                          WHEN production_mw > LAG(production_mw) OVER (ORDER BY startTime) THEN 'Increasing'
                                          WHEN production_mw < LAG(production_mw) OVER (ORDER BY startTime) THEN 'Decreasing'
                                          ELSE 'Stable'
                                          END as trend
                                          FROM production
                                          )
                                      WHERE trend IS NOT NULL
                                      GROUP BY trend
                                      ORDER BY count DESC
                                      """)
            trend_summary.show(truncate=False)

        else:
            print("❌ No data available for analysis")

    except Exception as e:
        print(f"❌ Error in trend analysis: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("✅ Examples completed!")
    print("\n💡 Next steps:")
    print("   - Explore more datasets using list_available_datasets()")
    print("   - Combine multiple datasets for advanced analysis")
    print("   - Use PySpark's DataFrame API for custom transformations")
    print("   - Check out energy_analysis.py for more advanced examples")

    # Stop Spark session
    spark.stop()
    print("\n🛑 Spark session stopped")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted - stopping...")
        try:
            active = SparkSession.getActiveSession()
            if active is not None:
                active.stop()
        except Exception:
            pass
        sys.exit(130)
