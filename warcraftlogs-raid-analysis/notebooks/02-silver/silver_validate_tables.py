# Databricks notebook source
# DBTITLE 1,Install Lobraries
%pip install databricks-labs-dqx==0.6.0
dbutils.library.restartPython()

# COMMAND ----------
# DBTITLE 1,Import Dependencies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,Read Tables
tables = {
    "tables_player_summary": spark.read.table("02_silver.staging.warcraftlogs_tables_player_summary"),
    "tables_player_abilities": spark.read.table("02_silver.staging.warcraftlogs_tables_player_abilities"),
    "tables_player_targets": spark.read.table("02_silver.staging.warcraftlogs_tables_player_targets"),
    "tables_player_pets": spark.read.table("02_silver.staging.warcraftlogs_tables_player_pets"),
    "tables_player_gear": spark.read.table("02_silver.staging.warcraftlogs_tables_player_gear"),
    "tables_healing_summary": spark.read.table("02_silver.staging.warcraftlogs_tables_healing_summary"),
    "tables_deaths_summary": spark.read.table("02_silver.staging.warcraftlogs_tables_deaths_summary")
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
    # Profile
    _, profiles = profiler.profile(df)
    all_checks = generator.generate_dq_rules(profiles)

    # Remove problematic checks
    checks = [
        c for c in all_checks
        if c.get("check", {}).get("function") != "is_in_range"
        and not (
            c.get("check", {}).get("function") == "is_in_list"
        )
    ]

    # Validate
    valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df, checks)

    # Save
    valid_df.write.mode("overwrite").saveAsTable(f"02_silver.warcraftlogs.{name}")
    quarantine_df.write.mode("overwrite").saveAsTable(f"02_silver.dq_monitoring.warcraftlogs_quarantine_{name}")

    # Clean staging area
    spark.sql(f"""DROP TABLE IF EXISTS 02_silver.staging.warcraftlogs_{name}""")
