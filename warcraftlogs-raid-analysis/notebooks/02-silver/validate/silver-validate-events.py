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
    "all": spark.read.table("02_silver.staging.warcraftlogs_events_all"),
    "casts": spark.read.table("02_silver.staging.warcraftlogs_events_casts"),
    "aura": spark.read.table("02_silver.staging.warcraftlogs_events_aura"),
    "combatant": spark.read.table("02_silver.staging.warcraftlogs_events_combatant"),
    "damage": spark.read.table("02_silver.staging.warcraftlogs_events_damage"),
    "dispel": spark.read.table("02_silver.staging.warcraftlogs_events_dispel"),
    "heal": spark.read.table("02_silver.staging.warcraftlogs_events_heal"),
    "lifecycle": spark.read.table("02_silver.staging.warcraftlogs_events_lifecycle"),
    "resource": spark.read.table("02_silver.staging.warcraftlogs_events_resource")
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
    valid_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"02_silver.warcraftlogs.f_events_{name}")
    quarantine_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"02_silver.dq_monitoring.warcraftlogs_quarantine_events_{name}")

    # Clean staging area
    spark.sql(f"""DROP TABLE IF EXISTS 02_silver.staging.warcraftlogs_events_{name}""")    
    print(f"Validation complete for {name}.")
print(f"✅ All tables validated.")
