# Databricks notebook source
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
df = spark.read.table("02_silver.staging.warcraftlogs_fights_boss_pulls")

# COMMAND ----------

# DBTITLE 1,Establish Checks
# Profile
ws = WorkspaceClient()
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(df)
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)

# Filter out is_in_range manually
checks = [
    c for c in checks
    if c.get("check", {}).get("function") != "is_in_range"
]

# COMMAND ----------

# DBTITLE 1,Run Validation and Write to Silver
# Run validation using metadata-based API
engine = DQEngine(spark)
valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df, checks)

# Save results
valid_df.write.mode("overwrite").saveAsTable("02_silver.warcraftlogs.fights_boss_pulls")
quarantine_df.write.mode("overwrite").saveAsTable("02_silver.dq_monitoring.warcraftlogs_quarantine_fights_boss_pulls")

# COMMAND ----------

# DBTITLE 1,Clean Staging Area
spark.sql("DROP TABLE IF EXISTS 02_silver.staging.warcraftlogs_fights_boss_pulls")
