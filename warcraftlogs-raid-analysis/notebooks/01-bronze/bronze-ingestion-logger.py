{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "ff3d0f77-3f0e-4143-8b5f-984f476a3d75",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Log Ingested Reports"
    }
   },
   "outputs": [],
   "source": [
    " log_df = spark.createDataFrame([(report_id,)], [\"report_id\"]) \\\n",
    "    .withColumn(\"ingested_at\", current_timestamp())\n",
    " \n",
    "log_df.write.mode(\"append\").saveAsTable(\"raid_report_tracking\")\n",
    "print(f\"📌 Logged report {report_id} as ingested.\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "2"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "bronze-ingestion-logger",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
