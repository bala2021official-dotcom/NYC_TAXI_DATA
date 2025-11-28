# Databricks notebook source
# ================================================================
# Orchestrate DLT Pipeline
# ================================================================

from databricks.sdk import WorkspaceClient
from datetime import datetime
import time
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# --------------------------------------------------------------
# INIT
# --------------------------------------------------------------
w = WorkspaceClient()
pipeline_id = "72b3a585-2e28-47ef-b8b4-6884aad8d288"
IST_TZ = "Asia/Kolkata"

# --------------------------------------------------------------
# IST timestamp helper
# --------------------------------------------------------------
def ist_now():
    return spark.sql(
        f"SELECT from_utc_timestamp(current_timestamp(), '{IST_TZ}')"
    ).collect()[0][0]


# --------------------------------------------------------------
# START PIPELINE
# --------------------------------------------------------------
run_id = f"RUN-{int(time.time())}"
start_time = ist_now()

print("============================================")
print(f" Starting DLT Pipeline Run")
print(f" RunId: {run_id}")
print("============================================")

update = w.pipelines.start_update(
    pipeline_id=pipeline_id,
    full_refresh=False
)

update_id = update.update_id
print(f" Update ID: {update_id}")


# --------------------------------------------------------------
# GET FIRST STATUS
# --------------------------------------------------------------
status = w.pipelines.get_update(pipeline_id, update_id)
state = status.update.state

print(f"⏳ Initial Pipeline State: {state}")


# --------------------------------------------------------------
# POLL UNTIL FINISHED
# --------------------------------------------------------------
running_states = ["INITIALIZING", "RUNNING", "QUEUED"]

while state in running_states:
    time.sleep(20)
    status = w.pipelines.get_update(pipeline_id, update_id)
    state = status.update.state
    print(f"⏳ Pipeline State: {state}")


# --------------------------------------------------------------
# DETERMINE FINAL STATUS
# --------------------------------------------------------------
if state == "FAILED":
    final_status = "FAILED"
    error_message = (
        status.update.error.message  
        if status.update.error else "Unknown error"
    )
else:
    final_status = "SUCCESS"
    error_message = None

end_time = ist_now()

print("============================================")
print(f" Final Pipeline Status: {final_status}")
print("============================================")


# --------------------------------------------------------------
# CREATE LOG TABLE (if not exists)
# --------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS nyc_taxi_cat.pipeline_log_sch.dlt_pipeline_run_log_tab (
    RunId STRING,
    PipelineId STRING,
    UpdateId STRING,
    Status STRING,
    StartTimeIST TIMESTAMP,
    EndTimeIST TIMESTAMP,
    ErrorMessage STRING
)
USING DELTA
""")


# --------------------------------------------------------------
# WRITE RUN LOG (with explicit schema)
# --------------------------------------------------------------
schema = StructType([
    StructField("RunId", StringType(), True),
    StructField("PipelineId", StringType(), True),
    StructField("UpdateId", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("StartTimeIST", TimestampType(), True),
    StructField("EndTimeIST", TimestampType(), True),
    StructField("ErrorMessage", StringType(), True)
])

log_df = spark.createDataFrame([(
    run_id,
    pipeline_id,
    update_id,
    final_status,
    start_time,
    end_time,
    error_message
)], schema)

log_df.write.mode("append").saveAsTable(
    "nyc_taxi_cat.pipeline_log_sch.dlt_pipeline_run_log_tab"
)

print(" Log written successfully")
print(" Orchestration complete")











# from pyspark.sql import functions as F
# from datetime import datetime

# IST_TZ = "Asia/Kolkata"
# CATALOG = "nyc_taxi_cat"
# PIPE_LOG_SCHEMA = "pipeline_log_sch"
# SILVER = f"{CATALOG}.silver_sch.silver_nyc_taxi_tab"
# ERROR  = f"{CATALOG}.error_lg_sch.error_log_tab"
# PIPE_LOG = f"{CATALOG}.{PIPE_LOG_SCHEMA}.pipeline_log_tab"

# dbutils.widgets.text("run_status", "SUCCESS")
# dbutils.widgets.text("file_name", "N/A")
# run_status = dbutils.widgets.get("run_status")
# file_name  = dbutils.widgets.get("file_name")

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {PIPE_LOG} (
#   RunId STRING,
#   Timestamp TIMESTAMP,
#   Status STRING,
#   ProcessedRecordCount LONG,
#   InvalidAnomalyCount LONG,
#   DuplicateCount LONG,
#   FileName STRING
# ) USING DELTA
# """)

# # -- Faster metrics (see B below)
# silver_cnt    = spark.table(SILVER).count()
# error_cnt     = spark.table(ERROR).count()
# duplicate_cnt = spark.table(ERROR).where(F.col("error_category")=="DUPLICATE").count()

# run_id = f"Run-{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
# ts = spark.sql(f"SELECT from_utc_timestamp(current_timestamp(), '{IST_TZ}') as ts").collect()[0]["ts"]

# log_df = spark.createDataFrame([(
#     run_id, ts, run_status, int(silver_cnt), int(error_cnt), int(duplicate_cnt), file_name
# )], ["RunId","Timestamp","Status","ProcessedRecordCount","InvalidAnomalyCount","DuplicateCount","FileName"])

# log_df.write.mode("append").saveAsTable(PIPE_LOG)
# print(f"APPENDED → {PIPE_LOG} :: {run_id} :: status={run_status}")
































# # Databricks notebook source
# # MAGIC %md
# # MAGIC # Orchestration — Append Pipeline Logs + Weekly Scheduler (Free Edition, No API)
# # MAGIC **Schedule:** Every Wednesday @ 12:00 PM IST (set schedule_mode=weekly, day_of_week=WED, run_time_ist=12:00)

# from pyspark.sql import functions as F
# from datetime import datetime, timedelta
# import time

# # -----------------------------
# # Widgets (Scheduling & Logging)
# # -----------------------------
# dbutils.widgets.dropdown("schedule_mode", "weekly", ["interval","hourly","daily","weekly"], "Schedule Mode")
# dbutils.widgets.text("interval_minutes", "15", "Interval Minutes (for schedule_mode=interval)")
# dbutils.widgets.text("run_time_ist", "12:00", "Run Time IST (HH:mm) for hourly/daily/weekly")
# dbutils.widgets.text("day_of_week", "WED", "Day of Week for weekly (SUN,MON,TUE,WED,THU,FRI,SAT)")
# dbutils.widgets.text("max_runs", "10", "Maximum Runs")
# dbutils.widgets.text("run_window_minutes", "10080", "Max Window Minutes (e.g., 10080 = 7 days)")
# dbutils.widgets.text("run_status", "SUCCESS", "Pipeline Run Status (SUCCESS/FAILED)")
# dbutils.widgets.text("file_name", "N/A", "Source File Name (optional)")

# schedule_mode      = dbutils.widgets.get("schedule_mode").lower()
# interval_minutes   = int(dbutils.widgets.get("interval_minutes"))
# run_time_ist       = dbutils.widgets.get("run_time_ist")
# day_of_week        = dbutils.widgets.get("day_of_week").upper()
# max_runs           = int(dbutils.widgets.get("max_runs"))
# run_window_minutes = int(dbutils.widgets.get("run_window_minutes"))
# run_status         = dbutils.widgets.get("run_status")
# file_name          = dbutils.widgets.get("file_name")

# IST_TZ = "Asia/Kolkata"

# CATALOG           = "nyc_taxi_cat"
# PIPE_LOG_SCHEMA   = "pipeline_log_sch"
# SILVER_TABLE_FQN  = f"{CATALOG}.silver_sch.silver_nyc_taxi_tab"
# ERROR_TABLE_FQN   = f"{CATALOG}.error_lg_sch.error_log_tab"
# PIPE_LOG_FQN      = f"{CATALOG}.{PIPE_LOG_SCHEMA}.pipeline_log_tab"

# # Ensure log table exists (APPEND ONLY)
# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {PIPE_LOG_FQN} (
#   RunId STRING,
#   Timestamp TIMESTAMP,
#   Status STRING,
#   ProcessedRecordCount LONG,
#   InvalidAnomalyCount LONG,
#   DuplicateCount LONG,
#   FileName STRING
# ) USING DELTA
# """)

# # ---------------
# # Log one run
# # ---------------
# def append_one_run(status: str, file_name: str):
#     silver_cnt    = spark.table(SILVER_TABLE_FQN).count()
#     error_cnt     = spark.table(ERROR_TABLE_FQN).count()
#     duplicate_cnt = spark.table(ERROR_TABLE_FQN).where(F.col("error_category") == "DUPLICATE").count()

#     run_id = f"Run-{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
#     ts = spark.sql(f"SELECT from_utc_timestamp(current_timestamp(), '{IST_TZ}') as ts").collect()[0]["ts"]

#     log_df = spark.createDataFrame([(
#         run_id, ts, status, int(silver_cnt), int(error_cnt), int(duplicate_cnt), file_name
#     )], ["RunId","Timestamp","Status","ProcessedRecordCount","InvalidAnomalyCount","DuplicateCount","FileName"])

#     (log_df.write.mode("append")
#           .saveAsTable(PIPE_LOG_FQN))
#     display(log_df)
#     print(f"APPENDED → {PIPE_LOG_FQN} :: {run_id} :: status={status}, silver={silver_cnt}, errors={error_cnt}, dup={duplicate_cnt}")

# # -----------------------------
# # Scheduler helpers (no REST/API)
# # -----------------------------
# def _ist_now() -> datetime:
#     """Return current IST time (approx by fixed offset UTC+5:30)."""
#     return datetime.utcnow() + timedelta(hours=5, minutes=30)

# def _weekday_index(name: str) -> int:
#     """Map three-letter day name to Python weekday index (Mon=0 ... Sun=6)."""
#     mapping = {"MON":0, "TUE":1, "WED":2, "THU":3, "FRI":4, "SAT":5, "SUN":6}
#     return mapping[name]

# def next_run_time(now_ist: datetime):
#     """Compute next run datetime in IST based on schedule_mode."""
#     hh, mm = [int(x) for x in run_time_ist.split(":")]

#     if schedule_mode == "interval":
#         return now_ist + timedelta(minutes=interval_minutes)

#     if schedule_mode in ("hourly", "daily"):
#         candidate = now_ist.replace(hour=hh, minute=mm, second=0, microsecond=0)
#         if schedule_mode == "hourly":
#             if candidate <= now_ist:
#                 candidate = candidate + timedelta(hours=1)
#         else:  # daily
#             if candidate <= now_ist:
#                 candidate = candidate + timedelta(days=1)
#         return candidate

#     if schedule_mode == "weekly":
#         target_wd = _weekday_index(day_of_week)  # WED -> 2
#         # Python weekday: Mon=0 ... Sun=6; our mapping follows this
#         curr_wd = now_ist.weekday()
#         days_ahead = (target_wd - curr_wd) % 7
#         candidate = now_ist.replace(hour=hh, minute=mm, second=0, microsecond=0) + timedelta(days=days_ahead)
#         # If the candidate is not strictly in the future, push to next week
#         if candidate <= now_ist:
#             candidate = candidate + timedelta(days=7)
#         return candidate

#     # Fallback: run immediately
#     return now_ist

# def sleep_until(target_ist: datetime):
#     """Sleep until target IST time safely."""
#     while True:
#         now_ist = _ist_now()
#         remaining = (target_ist - now_ist).total_seconds()
#         if remaining <= 0:
#             break
#         time.sleep(min(remaining, 60))  # sleep in 60s chunks to be responsive

# # -----------------------------
# # Main scheduling loop
# # -----------------------------
# start_utc = datetime.utcnow()
# deadline_utc = start_utc + timedelta(minutes=run_window_minutes)
# runs_done = 0

# print(f"Scheduler started at UTC={start_utc}, IST={_ist_now()}")
# print(f"Mode={schedule_mode}, interval={interval_minutes}m, time={run_time_ist}, weekly day={day_of_week}, max_runs={max_runs}, window={run_window_minutes}m")

# while runs_done < max_runs and datetime.utcnow() < deadline_utc:
#     now_ist = _ist_now()
#     target_ist = next_run_time(now_ist)
#     print(f"Next planned run at IST={target_ist}")

#     sleep_until(target_ist)

#     # Stop checks after waking
#     if runs_done >= max_runs or datetime.utcnow() >= deadline_utc:
#         print("Stop: reached max runs or time window.")
#         break

#     # ---- EXECUTE ONE LOG APPEND ----
#     try:
#         append_one_run(run_status, file_name)
#         runs_done += 1
#     except Exception as e:
#         print(f"Logging failed: {e}. Appending FAILURE row.")
#         try:
#             append_one_run("FAILED", file_name)
#             runs_done += 1
#         except Exception as e2:
#             print(f"Secondary append failed: {e2}")

# print(f"Scheduler finished: runs_done={runs_done}")






















# # Databricks notebook source
# # MAGIC %md
# # MAGIC # Orchestration — Append Pipeline Logs + Built‑in Scheduler (Free Edition, No API)

# from pyspark.sql import functions as F
# from datetime import datetime, timedelta
# import time

# # -----------------------------
# # Widgets (Scheduling & Logging)
# # -----------------------------
# dbutils.widgets.dropdown("schedule_mode", "interval", ["interval","hourly","daily"], "Schedule Mode")
# dbutils.widgets.text("interval_minutes", "15", "Interval Minutes (for schedule_mode=interval)")
# dbutils.widgets.text("run_time_ist", "12:00", "Run Time IST (HH:mm) for hourly/daily")
# dbutils.widgets.text("max_runs", "10", "Maximum Runs")
# dbutils.widgets.text("run_window_minutes", "120", "Max Window Minutes (stop after this duration)")
# dbutils.widgets.text("run_status", "SUCCESS", "Pipeline Run Status (SUCCESS/FAILED)")
# dbutils.widgets.text("file_name", "N/A", "Source File Name (optional)")

# schedule_mode      = dbutils.widgets.get("schedule_mode").lower()
# interval_minutes   = int(dbutils.widgets.get("interval_minutes"))
# run_time_ist       = dbutils.widgets.get("run_time_ist")
# max_runs           = int(dbutils.widgets.get("max_runs"))
# run_window_minutes = int(dbutils.widgets.get("run_window_minutes"))
# run_status         = dbutils.widgets.get("run_status")
# file_name          = dbutils.widgets.get("file_name")

# IST_TZ = "Asia/Kolkata"

# CATALOG           = "nyc_taxi_cat"
# PIPE_LOG_SCHEMA   = "pipeline_log_sch"
# SILVER_TABLE_FQN  = f"{CATALOG}.silver_sch.silver_nyc_taxi_tab"
# ERROR_TABLE_FQN   = f"{CATALOG}.error_lg_sch.error_log_tab"
# PIPE_LOG_FQN      = f"{CATALOG}.{PIPE_LOG_SCHEMA}.pipeline_log_tab"

# # Ensure log table exists (APPEND ONLY)
# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {PIPE_LOG_FQN} (
#   RunId STRING,
#   Timestamp TIMESTAMP,
#   Status STRING,
#   ProcessedRecordCount LONG,
#   InvalidAnomalyCount LONG,
#   DuplicateCount LONG,
#   FileName STRING
# ) USING DELTA
# """)

# # ---------------
# # Log one run
# # ---------------
# def append_one_run(status: str, file_name: str):
#     # Collect metrics from current snapshot of pipeline tables
#     silver_cnt    = spark.table(SILVER_TABLE_FQN).count()
#     error_cnt     = spark.table(ERROR_TABLE_FQN).count()
#     duplicate_cnt = spark.table(ERROR_TABLE_FQN).where(F.col("error_category") == "DUPLICATE").count()

#     run_id = f"Run-{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
#     # IST timestamp via SQL function for consistency
#     ts = spark.sql(f"SELECT from_utc_timestamp(current_timestamp(), '{IST_TZ}') as ts").collect()[0]["ts"]

#     log_df = spark.createDataFrame([(
#         run_id, ts, status, int(silver_cnt), int(error_cnt), int(duplicate_cnt), file_name
#     )], ["RunId","Timestamp","Status","ProcessedRecordCount","InvalidAnomalyCount","DuplicateCount","FileName"])

#     (log_df.write.mode("append")
#           .saveAsTable(PIPE_LOG_FQN))
#     display(log_df)
#     print(f"APPENDED → {PIPE_LOG_FQN} :: {run_id} :: status={status}, silver={silver_cnt}, errors={error_cnt}, dup={duplicate_cnt}")

# # -----------------------------
# # Scheduler helpers (no REST/API)
# # -----------------------------
# def next_run_time(now_ist: datetime):
#     """Compute next run datetime in IST based on schedule_mode."""
#     if schedule_mode == "interval":
#         return now_ist + timedelta(minutes=interval_minutes)
#     # run_time_ist expected as HH:mm
#     hh, mm = [int(x) for x in run_time_ist.split(":")]
#     candidate = now_ist.replace(hour=hh, minute=mm, second=0, microsecond=0)
#     if schedule_mode == "hourly":
#         # run at that minute each hour
#         if candidate <= now_ist:
#             candidate = candidate + timedelta(hours=1)
#     elif schedule_mode == "daily":
#         # run once per day at HH:mm
#         if candidate <= now_ist:
#             candidate = candidate + timedelta(days=1)
#     return candidate

# def sleep_until(target_ist: datetime):
#     """Sleep until target IST time safely."""
#     while True:
#         now_utc = datetime.utcnow()
#         now_ist = now_utc + timedelta(hours=5, minutes=30)
#         remaining = (target_ist - now_ist).total_seconds()
#         if remaining <= 0:
#             break
#         time.sleep(min(remaining, 60))  # sleep in 60s chunks to be responsive

# # -----------------------------
# # Main scheduling loop
# # -----------------------------
# start_utc = datetime.utcnow()
# deadline_utc = start_utc + timedelta(minutes=run_window_minutes)
# runs_done = 0

# print(f"Scheduler started at UTC={start_utc}, IST={start_utc + timedelta(hours=5, minutes=30)}")
# print(f"Mode={schedule_mode}, interval={interval_minutes}m, daily/hourly time={run_time_ist}, max_runs={max_runs}, window={run_window_minutes}m")

# while runs_done < max_runs and datetime.utcnow() < deadline_utc:
#     # Convert current UTC to IST for scheduling decisions
#     now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)

#     # Determine when to execute next
#     target_ist = next_run_time(now_ist)
#     print(f"Next planned run at IST={target_ist}")

#     # Sleep until target
#     sleep_until(target_ist)

#     # Stop checks
#     if runs_done >= max_runs or datetime.utcnow() >= deadline_utc:
#         print("Stop: reached max runs or time window.")
#         break

#     # ---- EXECUTE ONE LOG APPEND ----
#     try:
#         append_one_run(run_status, file_name)
#         runs_done += 1
#     except Exception as e:
#         # Still append a failure row with zero counts fallback
#         print(f"Logging failed: {e}. Appending FAILURE row.")
#         try:
#             append_one_run("FAILED", file_name)
#             runs_done += 1
#         except Exception as e2:
#             print(f"Secondary append failed: {e2}")

# print(f"Scheduler finished: runs_done={runs_done}")


















# # Databricks notebook cell
# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service import jobs

# # ======= Inputs you’ll customize =======
# PIPELINE_ID = "72b3a585-2e28-47ef-b8b4-6884aad8d288"  # raw UUID, no angle brackets
# JOB_NAME    = "NYC_Taxi_Job"

# CRON        = "0 30 12 ? * WED"
# TIMEZONE    = "Asia/Kolkata"

# ON_START    = ["balakrishna.boddu@protivitiglobal.in"]
# ON_SUCCESS  = ["balakrishna.boddu@protivitiglobal.in"]
# ON_FAILURE  = ["balakrishna.boddu@protivitiglobal.in"]

# # ======= Client =======
# w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# # ======= Create Job =======
# created_job = w.jobs.create(
#     name=JOB_NAME,
#     tasks=[
#         jobs.Task(
#             task_key="nyc_taxi_pipeline",
#             description="Run NYC Taxi DLT pipeline",
#             pipeline_task=jobs.PipelineTask(pipeline_id=PIPELINE_ID),
#             timeout_seconds=3600,
#             max_retries=3,
#             min_retry_interval_millis=300000,
#             retry_on_timeout=True
#         )
#     ],
#     schedule=jobs.CronSchedule(
#         quartz_cron_expression=CRON,
#         timezone_id=TIMEZONE,
#         pause_status=jobs.PauseStatus.UNPAUSED
#     ),
#     email_notifications=jobs.JobEmailNotifications(
#         on_start=ON_START,
#         on_success=ON_SUCCESS,
#         on_failure=ON_FAILURE
#     )
# )

# print(f" Job created. Job ID: {created_job.job_id}")














# # {
# #   "name": "NYC_Taxi_DLT_Pipeline1",
# #   "tasks": [
# #     {
# #       "task_key": "nyc_taxi_pipeline",
# #       "description": "Run NYC Taxi DLT pipeline",
# #       "pipeline_task": {
# #         "pipeline_id": "<72b3a585-2e28-47ef-b8b4-6884aad8d288>"
# #       },
# #       "timeout_seconds": 3600,
# #       "max_retries": 3,
# #       "min_retry_interval_millis": 300000,
# #       "retry_on_timeout": true
# #     }
# #   ],
# #   "schedule": {
# #     "quartz_cron_expression": "0 30 12 ? * WED",
# #     "timezone_id": "Asia/Kolkata",
# #     "pause_status": "UNPAUSED"
# #   },
# #   "email_notifications": {
# #     "on_start": [],
# #     "on_success": ["balakrishna.boddu@protivitiglobal.in"],
# #     "on_failure": ["balakrishna.boddu@protivitiglobal.in"]
# #   }
# # }
