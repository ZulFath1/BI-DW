# dags/

DAG Apache Airflow 3.3.2. Folder ini di-mount ke `/opt/airflow/dags` di container;
kode di `ingestion/`, `etl/`, `quality/`, `ml/` dapat di-import karena
`PYTHONPATH=/opt/airflow/project`.

Status: belum ada DAG.
