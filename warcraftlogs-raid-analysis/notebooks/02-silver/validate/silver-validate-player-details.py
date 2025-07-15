# Databricks notebook source
# DBTITLE 1,Install Lobraries
# MAGIC %pip install databricks-labs-dqx==0.6.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQForEachColRule, DQDatasetRule
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,Read Table
df = spark.read.table("02_silver.staging.warcraftlogs_player_details")

# COMMAND ----------

# DBTITLE 1,Establish Checks
# Profile
ws = WorkspaceClient()
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(df)
checks = DQGenerator(ws).generate_dq_rules(profiles)

# Filter out is_in_range manually
checks = [
    c for c in checks
    if c.get("check", {}).get("function") != "is_in_range"
]

# COMMAND ----------

# DBTITLE 1,Run Validation and Write to Silver
engine = DQEngine(spark)
valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df, checks)

valid_df.write.mode("overwrite").saveAsTable("02_silver.warcraftlogs.f_player_details")
quarantine_df.write.mode("overwrite").saveAsTable("02_silver.dq_monitoring.warcraftlogs_quarantine_player_details")

# COMMAND ----------

# DBTITLE 1,Clean Staging Area
spark.sql("DROP TABLE IF EXISTS 02_silver.staging.warcraftlogs_player_details")
