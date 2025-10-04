import os
from airflow.exceptions import AirflowSkipException

def check_file_exists():
    filepath = "/opt/airflow/dags/scripts/include/watch-history.json"
    if os.path.exists(filepath):
        print("File exists, proceeding with task execution")
    else:
        print("File not found, skipping the rest of the pipeline")
        raise AirflowSkipException("watch-history.json not found. Skipping DAG run.")
  