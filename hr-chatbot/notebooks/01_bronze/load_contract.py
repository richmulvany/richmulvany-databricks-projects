# Databricks notebook source
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Load contract YAML
contract_path = "/Workspace/Users/you/contracts/hr_employees_v1.yaml"

with open(contract_path, "r") as f:
    contract_raw = f.read()

contract = yaml.safe_load(contract_raw)
dataset_name = contract["dataset"]
version = contract["version"]

# COMMAND ----------

# Upsert into Delta table
spark.sql(f"""
  UPDATE 00_governance.contracts.contract_registry
  SET is_active = false
  WHERE dataset_name = '{dataset_name}'
""")

spark.sql(f"""
  INSERT INTO 00_governance.contracts.contract_registry
  VALUES ('{dataset_name}', '{version}', '{contract_raw}', true, current_timestamp())
""")
