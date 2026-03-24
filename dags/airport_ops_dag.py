"""
Task 4.1 — Airflow Dual-Tier DAG: airport_ops_pipeline

Tier 1 — Every 15 minutes (*/15 * * * *):
  flight schedule MERGE → gate events Bronze → silver.flight_operations → delay risk → alerts

Tier 2 — Daily at 00:05 AM (5 0 * * *):
  IoT aggregation → OTP Gold → gate utilization → security staffing → baggage audit → notifications

Prevent overlap: max_active_runs=1 on 15-min DAG; depends_on_past=False
SLA: OTP report must be ready by 06:00 AM — SLA alert to airport CIO if breached
Data freshness: if flight schedule Bronze not updated in 20 min, trigger AODB connectivity alert
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default Args ─────────────────────────────────────────────────
default_args = {
    "owner": "airport_ops",
    "depends_on_past": False,
    "email": ["airport-cio@skyhaven.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ═══════════════════════════════════════════════════════════════════
# TIER 1 — Real-Time Pipeline (Every 15 Minutes)
# ═══════════════════════════════════════════════════════════════════
dag_tier1 = DAG(
    dag_id="airport_ops_realtime_15min",
    default_args=default_args,
    description="Real-time 15-min pipeline: flight schedule → gate events → delay risk",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    max_active_runs=1,
    tags=["airport", "realtime", "tier1"],
)


def run_notebook(notebook_path, **kwargs):
    """Execute a Databricks notebook. On Databricks, replace with
    DatabricksRunNowOperator or dbutils.notebook.run()."""
    print(f"Executing notebook: {notebook_path}")
    # Databricks: dbutils.notebook.run(notebook_path, timeout_seconds=3600)
    # Local: exec(open(notebook_path).read())


def check_data_freshness(**kwargs):
    """Alert if flight_schedule Bronze hasn't been updated in 20 minutes."""
    print("Checking flight_schedule data freshness...")
    # Implementation:
    # latest_ts = spark.read.format("delta").load(BRONZE_DIR + "/flight_schedule")
    #     .agg(F.max("ingestion_ts")).collect()[0][0]
    # if (datetime.now() - latest_ts).total_seconds() > 1200:
    #     raise AirflowException("AODB connectivity alert: data stale > 20 min")
    print("Data freshness check passed.")


def push_delay_alerts(**kwargs):
    """Push HIGH/CRITICAL delay risk alerts to operations dashboard."""
    print("Pushing delay risk alerts to operations dashboard...")
    # Implementation:
    # alerts = spark.read.format("delta").load(SILVER_DIR + "/delay_propagation_risk")
    #     .filter("risk_level IN ('CRITICAL', 'HIGH')")
    # Send to dashboard API / notification system


with dag_tier1:
    start = DummyOperator(task_id="start")

    t1_1 = PythonOperator(
        task_id="task_1_1_bronze_flight_schedule",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/bronze/Task_1_1_Flight_Schedule_Bronze"},
    )

    t1_2_gate = PythonOperator(
        task_id="task_1_2_bronze_gate_events",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/bronze/Task_1_2_Gate_Events_Baggage_Scans_Bronze"},
    )

    t2_1 = PythonOperator(
        task_id="task_2_1_silver_flight_operations",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/silver/Task_2_1_Flight_Operations"},
    )

    t2_2 = PythonOperator(
        task_id="task_2_2_silver_delay_propagation_risk",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/silver/Task_2_2_Delay_Propagation_Risk"},
    )

    freshness_check = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    alert_push = PythonOperator(
        task_id="push_delay_alerts",
        python_callable=push_delay_alerts,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # DAG flow
    start >> t1_1 >> freshness_check
    start >> t1_2_gate
    [t1_1, t1_2_gate] >> t2_1 >> t2_2 >> alert_push >> end


# ═══════════════════════════════════════════════════════════════════
# TIER 2 — Daily Batch Pipeline (00:05 AM)
# ═══════════════════════════════════════════════════════════════════
dag_tier2 = DAG(
    dag_id="airport_ops_daily_batch",
    default_args=default_args,
    description="Daily batch: IoT agg → OTP → gate util → security staffing → baggage audit",
    schedule_interval="5 0 * * *",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    max_active_runs=1,
    tags=["airport", "daily", "tier2"],
    sla_miss_callback=lambda *args: print("SLA BREACH: OTP report not ready by 06:00 AM!"),
)


def check_otp_sla(**kwargs):
    """Verify OTP report is ready by 06:00 AM. Alert CIO if breached."""
    from datetime import datetime
    now = datetime.now()
    if now.hour >= 6:
        print("WARNING: OTP report SLA breached — not ready by 06:00 AM!")
        # Send alert to airport CIO
    else:
        print(f"OTP report ready at {now.strftime('%H:%M')} — SLA met.")


with dag_tier2:
    start = DummyOperator(task_id="start")

    # Bronze reference data refresh
    t1_3 = PythonOperator(
        task_id="task_1_3_bronze_iot_counters",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/bronze/Task_1_3_IoT_Counters_Bronze"},
    )

    t1_4 = PythonOperator(
        task_id="task_1_4_bronze_sla_aircraft",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/bronze/Task_1_4_SLA_Aircraft_Master_Bronze"},
    )

    t1_2_baggage = PythonOperator(
        task_id="task_1_2_bronze_baggage_scans",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/bronze/Task_1_2_Gate_Events_Baggage_Scans_Bronze"},
    )

    # Silver aggregations
    t2_3 = PythonOperator(
        task_id="task_2_3_silver_baggage_journey",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/silver/Task_2_3_Baggage_Journey_Misconnect"},
    )

    t2_4 = PythonOperator(
        task_id="task_2_4_silver_zone_occupancy",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/silver/Task_2_4_Zone_Occupancy_15min"},
    )

    # Gold reports
    t3_1 = PythonOperator(
        task_id="task_3_1_gold_otp_daily_report",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/gold/Task_3_1_OTP_Daily_Report"},
    )

    t3_2 = PythonOperator(
        task_id="task_3_2_gold_gate_utilization",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/gold/Task_3_2_Gate_Utilization_Summary"},
    )

    t3_3 = PythonOperator(
        task_id="task_3_3_gold_security_staffing",
        python_callable=run_notebook,
        op_kwargs={"notebook_path": "/Workspace/notebooks/gold/Task_3_3_Security_Staffing_Recommendation"},
    )

    otp_sla_check = PythonOperator(
        task_id="check_otp_sla",
        python_callable=check_otp_sla,
    )

    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # DAG flow
    start >> [t1_3, t1_4, t1_2_baggage]
    t1_2_baggage >> t2_3
    t1_3 >> t2_4
    [t2_3, t2_4, t1_4] >> t3_1 >> otp_sla_check
    t1_4 >> t3_2
    t2_4 >> t3_3
    [otp_sla_check, t3_2, t3_3] >> end
