"""
Advanced energy analysis examples using PySpark Fingrid Data Source.

This script demonstrates more sophisticated analysis patterns including
multi-dataset joins, time series analysis, and energy market insights.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import round as spark_round
from pyspark.sql.window import Window

from pyspark_fingrid import list_available_datasets, read_fingrid_data


def analyze_production_vs_shortage_status(spark, api_key, hours=6):
    """
    Analyze relationship between electricity production and shortage status.

    Args:
        spark: Spark session
        api_key: Fingrid API key
        hours: Number of hours to analyze
    """
    print(f"\n🔬 Analysis: Production vs Shortage Status (last {hours} hours)")
    print("-" * 60)

    # Define time range
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        # Read production data
        print("📊 Reading production data...")
        df_production = read_fingrid_data(api_key, 192, start_str, end_str)

        # Read shortage status data
        print("⚠️  Reading shortage status data...")
        df_shortage = read_fingrid_data(api_key, 336, start_str, end_str)

        if not df_production or not df_shortage:
            print("❌ Could not load required datasets")
            return

        # Join datasets on time
        print("🔗 Joining datasets...")
        df_combined = (
            df_production.alias("prod")
            .join(df_shortage.alias("short"), col("prod.startTime") == col("short.startTime"), "inner")
            .select(
                col("prod.startTime").alias("timestamp"),
                col("prod.production_mw"),
                col("short.shortage_status"),
                col("short.shortage_status_description"),
            )
        )

        print(f"📊 Combined dataset: {df_combined.count()} records")

        # Analysis 1: Production statistics by shortage status
        print("\n📈 Production statistics by shortage status:")
        status_stats = (
            df_combined.groupBy("shortage_status", "shortage_status_description")
            .agg(
                F.count("*").alias("record_count"),
                spark_round(avg("production_mw"), 2).alias("avg_production_mw"),
                spark_round(spark_min("production_mw"), 2).alias("min_production_mw"),
                spark_round(spark_max("production_mw"), 2).alias("max_production_mw"),
            )
            .orderBy("shortage_status")
        )

        status_stats.show(truncate=False)

        # Analysis 2: Time series analysis
        print("\n⏰ Production trends over time:")

        # Add time-based features
        df_with_features = df_combined.withColumn("hour", F.hour("timestamp")).withColumn(
            "production_change", col("production_mw") - F.lag("production_mw").over(Window.orderBy("timestamp"))
        )

        # Hourly averages
        hourly_avg = (
            df_with_features.groupBy("hour")
            .agg(spark_round(avg("production_mw"), 2).alias("avg_production_mw"), F.count("*").alias("records"))
            .orderBy("hour")
        )

        hourly_avg.show()

        # Analysis 3: Critical insights
        print("\n🚨 Critical insights:")

        # Find lowest production periods
        low_production = (
            df_combined.filter(col("production_mw") < 6000)
            .select("timestamp", "production_mw", "shortage_status_description")
            .orderBy("production_mw")
        )

        if low_production.count() > 0:
            print("⚠️  Periods with production < 6000 MW:")
            low_production.show(5, truncate=False)
        else:
            print("✅ No critical low production periods found")

        # Shortage status distribution
        shortage_distribution = (
            df_combined.groupBy("shortage_status_description")
            .agg(F.count("*").alias("count"))
            .withColumn("percentage", spark_round(col("count") * 100.0 / df_combined.count(), 2))
        )

        print("\n📊 Shortage status distribution:")
        shortage_distribution.show(truncate=False)

    except Exception as e:
        print(f"❌ Analysis failed: {e}")


def analyze_consumption_patterns(spark, api_key, hours=24):
    """
    Analyze electricity consumption patterns from dataset 363.

    Args:
        spark: Spark session
        api_key: Fingrid API key
        hours: Number of hours to analyze
    """
    print(f"\n🏠 Analysis: Consumption Patterns (last {hours} hours)")
    print("-" * 60)

    # Define time range
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        print("🏠 Reading consumption data...")
        df_consumption = read_fingrid_data(api_key, 363, start_str, end_str)

        if not df_consumption:
            print("❌ Could not load consumption data")
            return

        print(f"📊 Consumption dataset: {df_consumption.count()} records")

        # Show sample with metadata
        print("\n📋 Sample consumption data with metadata:")
        df_consumption.select(
            "startTime", "consumption_kwh", "time_series_type", "resolution", "unit", "measurement_count"
        ).show(5, truncate=False)

        # Consumption statistics
        print("\n📊 Consumption statistics:")
        df_consumption.select("consumption_kwh").describe().show()

        # Convert KWH to MWH for easier comparison
        df_consumption_mwh = df_consumption.withColumn("consumption_mwh", spark_round(col("consumption_kwh") / 1000, 2))

        # Hourly analysis
        df_hourly = df_consumption_mwh.withColumn("hour", F.hour("startTime"))

        hourly_consumption = (
            df_hourly.groupBy("hour")
            .agg(spark_round(avg("consumption_mwh"), 2).alias("avg_consumption_mwh"), F.count("*").alias("records"))
            .orderBy("hour")
        )

        print("\n⏰ Hourly consumption patterns:")
        hourly_consumption.show()

        # Peak and off-peak analysis
        peak_hours = [7, 8, 9, 17, 18, 19, 20]  # Typical peak hours

        df_with_peak = df_hourly.withColumn(
            "period_type", when(col("hour").isin(peak_hours), "Peak").otherwise("Off-Peak")
        )

        peak_analysis = df_with_peak.groupBy("period_type").agg(
            spark_round(avg("consumption_mwh"), 2).alias("avg_consumption_mwh"),
            spark_round(spark_max("consumption_mwh"), 2).alias("max_consumption_mwh"),
            spark_round(spark_min("consumption_mwh"), 2).alias("min_consumption_mwh"),
        )

        print("\n⚡ Peak vs Off-Peak consumption:")
        peak_analysis.show(truncate=False)

    except Exception as e:
        print(f"❌ Consumption analysis failed: {e}")


def create_energy_dashboard_data(spark, api_key):
    """
    Create aggregated data suitable for an energy dashboard.

    Args:
        spark: Spark session
        api_key: Fingrid API key
    """
    print("\n📊 Creating Energy Dashboard Data")
    print("-" * 60)

    # Get last 4 hours of data
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=4)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        # Read multiple datasets
        print("📊 Reading production data...")
        df_production = read_fingrid_data(api_key, 192, start_str, end_str)

        print("⚠️  Reading shortage status...")
        df_shortage = read_fingrid_data(api_key, 336, start_str, end_str)

        if not df_production or not df_shortage:
            print("❌ Could not load required datasets")
            return

        # Create dashboard summary
        print("\n🎛️  Dashboard Summary:")

        # Current status (latest values)
        latest_production = df_production.orderBy(col("startTime").desc()).first()
        latest_shortage = df_shortage.orderBy(col("startTime").desc()).first()

        print(f"⚡ Current Production: {latest_production.production_mw:.1f} MW")
        print(f"⚠️  Current Status: {latest_shortage.shortage_status_description}")
        print(f"📅 Last Updated: {latest_production.startTime}")

        # Production statistics for the period
        production_stats = df_production.agg(
            spark_round(avg("production_mw"), 2).alias("avg_production"),
            spark_round(spark_max("production_mw"), 2).alias("max_production"),
            spark_round(spark_min("production_mw"), 2).alias("min_production"),
            F.count("*").alias("data_points"),
        ).collect()[0]

        print("\n📊 4-Hour Period Statistics:")
        print(f"   Average Production: {production_stats.avg_production} MW")
        print(f"   Peak Production: {production_stats.max_production} MW")
        print(f"   Minimum Production: {production_stats.min_production} MW")
        print(f"   Data Points: {production_stats.data_points}")

        # Create 15-minute aggregates for visualization
        print("\n📈 15-minute aggregated data for visualization:")

        # Round timestamps to 15-minute intervals
        df_aggregated = (
            df_production.withColumn(
                "interval_start",
                F.date_trunc("hour", "startTime") + F.expr("INTERVAL 15 MINUTES") * F.floor(F.minute("startTime") / 15),
            )
            .groupBy("interval_start")
            .agg(spark_round(avg("production_mw"), 2).alias("avg_production_mw"), F.count("*").alias("measurements"))
            .orderBy("interval_start")
        )

        df_aggregated.show(truncate=False)

        # Save for dashboard use (optional)
        print("\n💾 Data prepared for dashboard visualization")
        print("   Use df_aggregated for time series charts")
        print("   Use latest values for real-time indicators")

        return {
            'production_timeseries': df_aggregated,
            'current_production': latest_production.production_mw,
            'current_status': latest_shortage.shortage_status_description,
            'period_stats': production_stats,
        }

    except Exception as e:
        print(f"❌ Dashboard data creation failed: {e}")
        return None


def main():
    """Run advanced energy analysis examples."""
    # Initialize Spark session with optimizations
    spark = (
        SparkSession.builder.appName("Fingrid Energy Analysis")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    # Get API key
    api_key = os.getenv('FINGRID_API_KEY')
    if not api_key:
        print("❌ Please set FINGRID_API_KEY environment variable")
        print("   export FINGRID_API_KEY='your-api-key'")
        return

    print("🚀 PySpark Fingrid Data Source - Advanced Energy Analysis")
    print("=" * 70)

    # Show available datasets
    print("\n📋 Available Datasets:")
    list_available_datasets()

    # Run analyses
    analyze_production_vs_shortage_status(spark, api_key, hours=6)

    # Uncomment to run consumption analysis (requires dataset 363)
    # analyze_consumption_patterns(spark, api_key, hours=24)

    # Create dashboard data
    create_energy_dashboard_data(spark, api_key)

    print("\n✅ Advanced analysis completed!")
    print("\n💡 Next steps:")
    print("   - Save results to Delta Lake or Parquet for further analysis")
    print("   - Create visualizations using the aggregated data")
    print("   - Set up scheduled jobs for continuous monitoring")
    print("   - Add alerting for critical production levels")

    # Stop Spark session
    spark.stop()


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
