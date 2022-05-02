from unittest import result
from pandas import read_json
import requests
from airflow import DAG
import pathlib
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import airflow.utils.dates as dt
import json

from sqlalchemy import null

WORK_DIR = pathlib.Path(__file__).parent.resolve()

dag = DAG(
    dag_id="dag_test_01",
    start_date=dt.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/tmp",
)

start = DummyOperator(
    task_id="Start",
    dag=dag,
)


def _get_data():
    url = "http://172.17.0.1:5000"
    res = requests.get(url)

    json_obj = json.dumps(res.json(), indent=3)

    with open("/tmp/example.json", "w") as f:
        f.write(json_obj)

    if f is not None:
        print("SUCCESS")
        print(res.text)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "output_path": "/tmp/example.json",
    },
    dag=dag,
)

def _fetch_data(name, message):
    result = {}
    with open("/tmp/example.json", "r") as f:
        result = json.load()

    with open("/tmp/postgres_sql.sql", "w") as f:
        for name, message in result.items:
            f.write(
                "INSERT INTO test_table VALUES ("
                f"'{name}', '{message}'"
                ");\n"
            )

fetch_data = PythonOperator(
    task_id="fetch_data",
    python_callable=_fetch_data,
    op_kwargs={
        "name": "name",
        "message": "message",
    },
    dag=dag,
)
            
start >> get_data >> fetch_data
