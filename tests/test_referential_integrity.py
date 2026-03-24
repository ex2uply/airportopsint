"""
Referential Integrity Report
Reports: airline codes without SLA contracts; aircraft without master records.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_referential_integrity_report(spark, bronze_dir):
    """Run referential integrity checks across Bronze tables."""
    print("=" * 70)
    print("  REFERENTIAL INTEGRITY REPORT — Skyhaven Airport")
    print("=" * 70)

    # ── Load Bronze tables ───────────────────────────────────────
    flights_df = spark.read.format("delta").load(f"{bronze_dir}/flight_schedule")
    sla_df = spark.read.format("delta").load(f"{bronze_dir}/airline_sla")
    aircraft_df = spark.read.format("delta").load(f"{bronze_dir}/aircraft_master")

    print(f"\nFlight schedule rows: {flights_df.count()}")
    print(f"Airline SLA rows: {sla_df.count()}")
    print(f"Aircraft master rows: {aircraft_df.count()}")

    # ── Check 1: Airline codes without SLA contracts ─────────────
    print("\n" + "-" * 70)
    print("CHECK 1: Airline codes in flight_schedule WITHOUT SLA contract")
    print("-" * 70)

    flight_airlines = flights_df.select("airline_code", "airline_name").distinct()
    sla_airlines = sla_df.select("airline_code").distinct()

    orphan_airlines = flight_airlines.join(sla_airlines, "airline_code", "left_anti")
    orphan_count = orphan_airlines.count()

    if orphan_count > 0:
        print(f"⚠️  Found {orphan_count} airline(s) WITHOUT SLA contract:")
        orphan_airlines.show(truncate=False)
    else:
        print("✅ All airline codes have valid SLA contracts.")

    # ── Check 2: Aircraft without master records ─────────────────
    print("\n" + "-" * 70)
    print("CHECK 2: Aircraft registrations in flights WITHOUT master record")
    print("-" * 70)

    flight_regs = flights_df.select("aircraft_registration").distinct()
    master_regs = aircraft_df.select("aircraft_registration").distinct()

    orphan_aircraft = flight_regs.join(master_regs, "aircraft_registration", "left_anti")
    orphan_ac_count = orphan_aircraft.count()

    if orphan_ac_count > 0:
        print(f"⚠️  Found {orphan_ac_count} aircraft WITHOUT master record:")
        orphan_aircraft.show(truncate=False)
    else:
        print("✅ All aircraft registrations have valid master records.")

    # ── Check 3: Gate events referencing unknown flights ──────────
    print("\n" + "-" * 70)
    print("CHECK 3: Gate events referencing unknown flight_ids")
    print("-" * 70)

    try:
        gate_df = spark.read.format("delta").load(f"{bronze_dir}/gate_events")
        gate_flights = gate_df.select("flight_id").distinct()
        valid_flights = flights_df.select("flight_id").distinct()
        orphan_gate_events = gate_flights.join(valid_flights, "flight_id", "left_anti")
        orphan_ge_count = orphan_gate_events.count()

        if orphan_ge_count > 0:
            print(f"⚠️  Found {orphan_ge_count} flight_id(s) in gate_events without schedule:")
            orphan_gate_events.show(truncate=False)
        else:
            print("✅ All gate event flight_ids have valid schedule records.")
    except Exception as e:
        print(f"⚠️  Could not check gate_events: {e}")

    # ── Check 4: Baggage scans referencing unknown flights ───────
    print("\n" + "-" * 70)
    print("CHECK 4: Baggage scans referencing unknown flight_ids")
    print("-" * 70)

    try:
        baggage_df = spark.read.format("delta").load(f"{bronze_dir}/baggage_scans")
        bag_flights = baggage_df.select("flight_id").distinct()
        orphan_bag = bag_flights.join(valid_flights, "flight_id", "left_anti")
        orphan_bag_count = orphan_bag.count()

        if orphan_bag_count > 0:
            print(f"⚠️  Found {orphan_bag_count} flight_id(s) in baggage_scans without schedule:")
            orphan_bag.show(truncate=False)
        else:
            print("✅ All baggage scan flight_ids have valid schedule records.")
    except Exception as e:
        print(f"⚠️  Could not check baggage_scans: {e}")

    # ── Summary ──────────────────────────────────────────────────
    print("\n" + "=" * 70)
    total_issues = orphan_count + orphan_ac_count
    if total_issues == 0:
        print("  ✅ ALL REFERENTIAL INTEGRITY CHECKS PASSED")
    else:
        print(f"  ⚠️  {total_issues} REFERENTIAL INTEGRITY ISSUE(S) FOUND")
    print("=" * 70)


if __name__ == "__main__":
    spark = (SparkSession.builder
        .appName("Referential_Integrity_Report")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate())

    BRONZE_DIR = "/FileStore/delta_lake/bronze"
    run_referential_integrity_report(spark, BRONZE_DIR)
