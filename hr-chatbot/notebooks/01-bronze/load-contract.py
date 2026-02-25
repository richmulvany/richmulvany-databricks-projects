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
dataset_name = contract["dataset"]["name"]
version = contract["evolution"]["version"]

def compute_schema_hash(contract_yaml):
    """
    Compute an MD5 hash of the structural columns (business + optionally derived)
    for schema drift detection.
    """
    structural_schema = []

    # Include business columns
    for col in contract_yaml["schema"].get("business_columns", []):
        structural_schema.append({
            "name": col["name"],
            "type": col["type"],
            "nullable": col.get("nullable", True)
        })

    # Optionally include derived columns for drift trigger
    for col in contract_yaml["schema"].get("derived_columns", []):
        structural_schema.append({
            "name": col["name"],
            "type": col["type"],
            "nullable": col.get("nullable", True)
        })

    # Dump to YAML sorted by keys for consistent hashing
    schema_str = yaml.dump(structural_schema, sort_keys=True)
    return hashlib.md5(schema_str.encode()).hexdigest()

schema_hash = compute_schema_hash(contract)

# COMMAND ----------

# DBTITLE 1,Upsert into Delta table
# Upsert into Delta table
spark.sql(f"""
  UPDATE 00_governance.contracts.contract_registry
  SET is_active = false
  WHERE dataset_name = '{dataset_name}'
""")

contract_raw_escaped = contract_raw.replace("'", "''")
values = (
    f"('{dataset_name}', '{version}', '{contract_raw_escaped}', "
    f"true, current_timestamp(), '{schema_hash}')"
)

spark.sql(f"""
INSERT INTO 00_governance.contracts.contract_registry
(dataset_name, version, contract_yaml, is_active, deployed_at, schema_hash)
VALUES {values}
""")
