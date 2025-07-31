# Databricks notebook source
# DBTITLE 1,Install Libraries
# MAGIC %pip install databricks-labs-dqx==0.6.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Dependencies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,Read Tables
# Include encounters in the validation set.  Each table represents a dimension
# extracted from world_data and written in the previous transform step.
tables = {
    "world_data_expansions": spark.read.table("02_silver.staging.warcraftlogs_world_data_expansions"),
    "world_data_zones": spark.read.table("02_silver.staging.warcraftlogs_world_data_zones"),
    "world_data_encounters": spark.read.table("02_silver.staging.warcraftlogs_world_data_encounters"),
}

# COMMAND ----------

# DBTITLE 1,Start Engines
ws = WorkspaceClient()
profiler = DQProfiler(ws)
generator = DQGenerator(ws)
engine = DQEngine(spark)

# COMMAND ----------

# DBTITLE 1,Run Validation and Write to Silver
for name, df in tables.items():
    # Profile the dataset to derive schema and quality checks
    _, profiles = profiler.profile(df)
    all_checks = generator.generate_dq_rules(profiles)
    # Remove problematic checks that are overly restrictive for this dataset
    checks = [
        c for c in all_checks
        if c.get("check", {}).get("function") != "is_in_range"
        and not (
            c.get("check", {}).get("function") == "is_in_list"
        )
    ]
    # Validate and split the dataset into valid rows and quarantine rows
    valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df, checks)
    # Persist the results to the silver and dq_monitoring areas
    valid_df.write.mode("overwrite").saveAsTable(f"02_silver.warcraftlogs.d_{name}")
    quarantine_df.write.mode("overwrite").saveAsTable(f"02_silver.dq_monitoring.warcraftlogs_quarantine_{name}")
    # Clean staging area
    spark.sql(f"""DROP TABLE IF EXISTS 02_silver.staging.warcraftlogs_{name}""")
