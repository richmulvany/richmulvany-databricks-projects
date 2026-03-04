# Databricks notebook source
# DBTITLE 1,Import Dependencies


# COMMAND ----------

# Get table list
spark.sql("USE 02_silver.warcraftlogs")
tables_df = spark.sql("SHOW TABLES")
d_tables = [row.tableName for row in tables_df.collect() if row.tableName.startswith("d_")]

# Load each table into a dictionary
dataframes = {}
for table_name in d_tables:
    var_name = table_name[2:]  # Remove 'd_' prefix
    df = spark.table(f"02_silver.warcraftlogs.{table_name}")
    dataframes[var_name] = df
    print(f"{var_name} loaded.")
print(f"✅ All tables loaded.")

# COMMAND ----------

for name, df in dataframes.items():
    df.write.mode("overwrite").saveAsTable(f"03_gold.warcraftlogs.{name}")
    print(f"{name} written to gold.")
print("✅ All tables written to gold.")
