# Databricks notebook source
# DBTITLE 1,Dependencies
import yaml
from pathlib import Path

# COMMAND ----------
# DBTITLE 1,Paths
contract_path = "/Workspace/Users/ricard.mulvany@gmail.com/richmulvany-databricks-projects/hr-chatbot/contracts/hr_employees_v0.yml"
docs_output_dir = "/Workspace/Users/ricard.mulvany@gmail.com/richmulvany-databricks-projects/hr-chatbot/docs/"

Path(docs_output_dir).mkdir(parents=True, exist_ok=True)

# COMMAND ----------
# DBTITLE 1,Load Contract
with open(contract_path, "r") as f:
    contract = yaml.safe_load(f)

dataset_name = contract["dataset"]["name"]
version = contract["evolution"]["version"]

business_cols = contract["schema"]["business_columns"]
derived_cols = contract["schema"]["derived_columns"]
all_cols = business_cols + derived_cols
primary_keys = contract["schema"]["primary_key"]

pii_columns = contract.get("governance", {}).get("pii_columns", [])

# COMMAND ----------
# DBTITLE 1,Generate Markdown for Employee Table
md_lines = [
    f"# Table: ibm_analytics_employees",
    f"**Dataset:** {dataset_name}  ",
    f"**Contract Version:** {version}  ",
    f"**Primary Keys:** {', '.join(primary_keys)}  ",
    f"**PII Columns:** {', '.join(pii_columns) if pii_columns else 'None'}  ",
    "",
    "| Column Name | Type | Nullable | Derived | Description | Sensitivity |",
    "|-------------|------|---------|---------|-------------|-------------|"
]

for col in all_cols:
    name = col["name"]
    col_type = col["type"]
    nullable = col.get("nullable", True)
    derived = col.get("derived", False)
    description = col.get("description", "").replace("\n", " ")
    sensitivity = col.get("sensitivity", "")
    md_lines.append(f"| {name} | {col_type} | {nullable} | {derived} | {description} | {sensitivity} |")

# Add metadata columns
for meta_col in ["_load_id", "_generated_at"]:
    md_lines.append(f"| {meta_col} | metadata | True | False | Metadata column {meta_col} | internal |")

# Write Markdown
emp_md_path = Path(docs_output_dir) / "ibm_analytics_employees.md"
with open(emp_md_path, "w") as f:
    f.write("\n".join(md_lines))

print(f"✅ Employee table documentation written to {emp_md_path}")

# COMMAND ----------
# DBTITLE 1,Generate Markdown for Department Summary
dept_cols = df_dept_gold.columns
md_lines = [
    f"# Table: ibm_analytics_department_summary",
    f"**Dataset:** {dataset_name}  ",
    f"**Aggregated by:** Department  ",
    "",
    "| Column Name | Description |",
    "|-------------|-------------|"
]

for col in dept_cols:
    md_lines.append(f"| {col} | {col.replace('_', ' ').capitalize()} |")

dept_md_path = Path(docs_output_dir) / "ibm_analytics_department_summary.md"
with open(dept_md_path, "w") as f:
    f.write("\n".join(md_lines))

print(f"✅ Department summary documentation written to {dept_md_path}")

# COMMAND ----------
# DBTITLE 1,Generate Markdown for Job Summary
job_cols = df_job_gold.columns
md_lines = [
    f"# Table: ibm_analytics_job_summary",
    f"**Dataset:** {dataset_name}  ",
    f"**Aggregated by:** JobRole  ",
    "",
    "| Column Name | Description |",
    "|-------------|-------------|"
]

for col in job_cols:
    md_lines.append(f"| {col} | {col.replace('_', ' ').capitalize()} |")

job_md_path = Path(docs_output_dir) / "ibm_analytics_job_summary.md"
with open(job_md_path, "w") as f:
    f.write("\n".join(md_lines))

print(f"✅ Job summary documentation written to {job_md_path}")

# COMMAND ----------
# DBTITLE 1,Generate Markdown for KPI / Metrics Table
kpi_cols = df_kpi_gold.columns
md_lines = [
    f"# Table: ibm_analytics_kpi_metrics",
    f"**Dataset:** {dataset_name}  ",
    f"**Aggregated Metrics**  ",
    "",
    "| Column Name | Description |",
    "|-------------|-------------|"
]

for col in kpi_cols:
    md_lines.append(f"| {col} | {col.replace('_', ' ').capitalize()} |")

kpi_md_path = Path(docs_output_dir) / "ibm_analytics_kpi_metrics.md"
with open(kpi_md_path, "w") as f:
    f.write("\n".join(md_lines))

print(f"✅ KPI/metrics documentation written to {kpi_md_path}")

# COMMAND ----------
print("✅ All Gold layer tables documented in Markdown.")
