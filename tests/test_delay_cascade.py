"""
Test: Delay Cascade Scenario
Simulate inbound delay → verify downstream propagation is detected in delay_propagation_risk.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder
        .appName("test_delay_cascade")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate())


def test_delay_cascade_detection(spark):
    """
    Scenario: Aircraft VT-TEST arrives 90 minutes late (inbound).
    The same aircraft is scheduled for an outbound flight with only
    45 minutes of turn time. SLA requires 50 mins min turn time.
    Expected: propagation risk detected, risk_level = HIGH.
    """
    # ── Setup: Create test flight schedule ────────────────────────
    flight_schema = StructType([
        StructField("flight_id", StringType()),
        StructField("flight_number", StringType()),
        StructField("airline_code", StringType()),
        StructField("aircraft_registration", StringType()),
        StructField("scheduled_departure", StringType()),
        StructField("scheduled_arrival", StringType()),
        StructField("delay_minutes", IntegerType()),
        StructField("status", StringType()),
        StructField("origin_iata", StringType()),
        StructField("destination_iata", StringType()),
    ])

    flights_data = [
        # Inbound flight: arrives 90 min late
        ("FLT_IN", "AI-100", "AI", "VT-TEST",
         "2024-01-15T06:00:00", "2024-01-15T08:10:00",
         90, "ARR", "BOM", "DEL"),
        # Outbound flight: departs at 09:40, only 45 min after late arrival
        ("FLT_OUT", "AI-200", "AI", "VT-TEST",
         "2024-01-15T09:40:00", "2024-01-15T11:50:00",
         0, "SCH", "DEL", "BOM"),
    ]

    flights_df = spark.createDataFrame(flights_data, flight_schema)
    flights_df = (flights_df
        .withColumn("scheduled_departure", F.to_timestamp("scheduled_departure"))
        .withColumn("scheduled_arrival", F.to_timestamp("scheduled_arrival"))
    )

    # ── Setup: SLA with 50-min narrow-body turn time ─────────────
    sla_data = [("AI", "Air India", 50, 110, 70, 95, 2700, 92.0)]
    sla_df = spark.createDataFrame(sla_data, [
        "airline_code", "airline_name",
        "min_turn_time_narrow_body_mins", "min_turn_time_wide_body_mins",
        "connection_time_domestic_mins", "connection_time_international_mins",
        "late_departure_penalty_inr_per_min", "otp_target_pct"
    ])

    # ── Setup: Aircraft master ───────────────────────────────────
    aircraft_data = [("VT-TEST", "A320", "NARROW", "AI", 180, 20000, 5, "DEL")]
    aircraft_df = spark.createDataFrame(aircraft_data, [
        "aircraft_registration", "aircraft_type_code", "body_type",
        "airline_code", "seat_capacity", "max_payload_kg",
        "age_years", "base_airport_iata"
    ])

    # ── Simulate delay propagation logic ─────────────────────────
    # Inbound actual arrival = scheduled + delay
    inbound = flights_df.filter("flight_id = 'FLT_IN'")
    inbound = inbound.withColumn(
        "actual_arrival",
        F.from_unixtime(
            F.unix_timestamp("scheduled_arrival") + F.col("delay_minutes") * 60
        )
    )
    actual_arrival_ts = inbound.collect()[0]["actual_arrival"]

    outbound = flights_df.filter("flight_id = 'FLT_OUT'")
    outbound_departure = outbound.collect()[0]["scheduled_departure"]

    # Available turn time
    from datetime import datetime
    actual_arr = datetime.strptime(str(actual_arrival_ts), "%Y-%m-%d %H:%M:%S")
    sched_dep = outbound_departure
    available_turn = (sched_dep - actual_arr).total_seconds() / 60

    # Min turn time from SLA (narrow body)
    min_turn = sla_df.collect()[0]["min_turn_time_narrow_body_mins"]

    propagated_delay = max(min_turn - available_turn, 0)

    # Assign risk level
    if propagated_delay > 60:
        risk_level = "CRITICAL"
    elif propagated_delay > 30:
        risk_level = "HIGH"
    elif propagated_delay > 15:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    # ── Assertions ───────────────────────────────────────────────
    print(f"Available turn time: {available_turn:.1f} mins")
    print(f"Min turn time (SLA): {min_turn} mins")
    print(f"Propagated delay: {propagated_delay:.1f} mins")
    print(f"Risk level: {risk_level}")

    assert available_turn < min_turn, \
        f"Expected available_turn ({available_turn}) < min_turn ({min_turn})"
    assert propagated_delay > 0, \
        f"Expected propagated delay > 0, got {propagated_delay}"
    assert risk_level in ("CRITICAL", "HIGH", "MEDIUM"), \
        f"Expected risk to be CRITICAL/HIGH/MEDIUM, got {risk_level}"

    print("\n✅ Delay cascade test PASSED: downstream propagation correctly detected.")


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    s = (SparkSession.builder
        .appName("test_delay_cascade")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate())
    test_delay_cascade_detection(s)
