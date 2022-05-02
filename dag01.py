import json
import airflow
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
import airflow.utils.dates
import requests



dag = DAG(
    dag_id="dag01",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/tmp"
)

start = DummyOperator(
    task_id='Start',
    dag=dag,
)

def _save_data(output_path, year, month, day, hour):
    url = "http://172.17.0.1:5000"
    res = requests.get(url=url)

    json_obj = json.dumps(res.json(), indent=3)

    with open(f"{output_path}/{year}_{month}_{day}_{hour}.json", "w") as f:
        f.write(json_obj)

save_data = PythonOperator(
    task_id="save_data",
    python_callable=_save_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "/tmp"
    },
    dag=dag,
)

def _fetch_titleviews(companies, output_path):
    results = dict.fromkeys(companies, 0)
    with open(f"{output_path}.json", "r") as f:
        data = json.load(f)
        for line in data["Companies"]:
            company = line
            if company in companies:
                results[company] = data["Companies"][company]["title"]

    with open("/tmp/query.sql", "w") as f:
        for company, title in results.items():
            f.write(
                "INSERT INTO test_table VALUES ("
                    f"'{company}', {title}"
                ");\n"
        )

    

titleviews = PythonOperator(
    task_id="titleviews",
    python_callable=_fetch_titleviews,
    op_kwargs={
        "companies": {"Apple", "Facebook", "Google"},
        "output_path": "/tmp/{{ execution_date.year }}_{{ execution_date.month }}_{{ execution_date.day }}_{{ execution_date.hour }}",
    },
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id='postgres_conn_01',
    sql="query.sql",
    dag=dag,
)


start >> save_data >> titleviews >> write_to_postgres