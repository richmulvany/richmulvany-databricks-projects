# Databricks notebook source
# DBTITLE 1,Install Lobraries
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
tables = {
    "dispels": spark.read.table("02_silver.staging.warcraftlogs_tables_dispels"),
    "buffs": spark.read.table("02_silver.staging.warcraftlogs_tables_buffs"),
    "summary_healing": spark.read.table("02_silver.staging.warcraftlogs_tables_summary_healing"),
    "summary_damage": spark.read.table("02_silver.staging.warcraftlogs_tables_summary_damage"),
    "summary_damage_taken": spark.read.table("02_silver.staging.warcraftlogs_tables_summary_damage_taken"),
    "summary_deaths": spark.read.table("02_silver.staging.warcraftlogs_tables_summary_deaths"),
    "summary_metadata": spark.read.table("02_silver.staging.warcraftlogs_tables_summary_metadata"),
    "composition": spark.read.table("02_silver.staging.warcraftlogs_tables_composition"),
    "casts": spark.read.table("02_silver.staging.warcraftlogs_tables_casts"),
    "healing": spark.read.table("02_silver.staging.warcraftlogs_tables_healing"),
    "damage_done": spark.read.table("02_silver.staging.warcraftlogs_tables_damage_done")
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
    valid_df.write.mode("overwrite").saveAsTable(f"02_silver.warcraftlogs.f_tables_{name}")
    quarantine_df.write.mode("overwrite").saveAsTable(f"02_silver.dq_monitoring.warcraftlogs_quarantine_tables_{name}")

    # Clean staging area
    spark.sql(f"""DROP TABLE IF EXISTS 02_silver.staging.warcraftlogs_tables_{name}""")
    print(f"Validation complete for {name}.")
print(f"✅ All tables validated.")
