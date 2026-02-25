# Databricks notebook source
import hashlib
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Load contract YAML
contract_path = "/Workspace/Users/ricard.mulvany@gmail.com/richmulvany-databricks-projects/hr-chatbot/contracts/hr_employees_v0.yml"

with open(contract_path, "r") as f:
    contract_raw = f.read()

contract = yaml.safe_load(contract_raw)
dataset_name = contract["dataset"]
version = contract["version"]

def compute_schema_hash(contract_yaml):
    structural_schema = []

    for col in contract_yaml["columns"]:
        structural_schema.append({
            "name": col["name"],
            "type": col["type"],
            "nullable": col.get("nullable", True),
            "derived": col.get("derived", False)
        })

    schema_str = yaml.dump(structural_schema, sort_keys=True)
    return hashlib.md5(schema_str.encode()).hexdigest()

schema_hash = compute_schema_hash(contract)

# COMMAND ----------

# Upsert into Delta table
spark.sql(f"""
  UPDATE 00_governance.contracts.contract_registry
  SET is_active = false
  WHERE dataset_name = '{dataset_name}'
""")

spark.sql(f"""
  INSERT INTO 00_governance.contracts.contract_registry
  (dataset_name, version, contract_yaml, is_active, deployed_at, schema_hash)
  VALUES (
      '{dataset_name}',
      '{version}',
      '{contract_raw.replace("'", "''")}',
      true,
      current_timestamp(),
      '{schema_hash}'
      )
""")
