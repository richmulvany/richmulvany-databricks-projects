import yaml
from pyspark.sql.functions import current_timestamp

# path after bundle deploy
contract_path = "/Workspace/.../contracts/hr_employees_v1.yaml"

with open(contract_path, "r") as f:
    contract_raw = f.read()

contract = yaml.safe_load(contract_raw)

dataset_name = contract["dataset"]
version = contract["version"]

# deactivate old versions
spark.sql(f"""
UPDATE contract_registry
SET is_active = false
WHERE dataset_name = '{dataset_name}'
""")

# insert new version
spark.sql(f"""
INSERT INTO contract_registry
VALUES (
    '{dataset_name}',
    '{version}',
    '{contract_raw}',
    true,
    current_timestamp()
)
""")
