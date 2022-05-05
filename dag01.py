from asyncio import tasks
from http.client import PROXY_AUTHENTICATION_REQUIRED
import json
import airflow
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException
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

def _pick_what_company(company):
    if company == 'Google':
        return google_operator
    else:
        return save_data
        

pick_what_company = BranchPythonOperator(
    task_id="pick_google_company",
    python_callable=_pick_what_company,
    dag=dag,
)

def _save_google_data(output_path, year, month, day, hour):
    url = "http://172.17.0.1:5000"
    res = requests.get(url)
    json_obj = json.dumps(res.json(), indent=3)
    google = json_obj["Companies"]["Google"]
    if google is None:
        raise AirflowSkipException
    else:
        with open(f"{output_path}_{year}_{month}_{day}_{hour}_GOOGLE.json", "w") as f:
            google = json_obj["Compnaies"]["Google"]
            f.write(google)

google_operator = DummyOperator(
    task_id="google_operator",
    dag=dag,
)

save_google_data = PythonOperator(
    task_id="save_google_data",
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "/tmp",
    },
    dag=dag,
)

def _fetch_google_data(output_path, company):
    results = dict.fromkeys(company, 0)
    with open(f"{output_path}.json", "r") as f:
        data = json.load(f)
        for line in data["Companies"]:
            google = line
            if google in company:
                results[google] = data["Companies"][google]["title"]

    with open("/tmp/query_google.sql", "w") as f:
        for company, title in results.items():
            f.write(
                "INSERT INTO test_table VALUES ("
                    f"'{company}', {title}"
                ");\n"
        )

fetch_google_data = PythonOperator(
    task_id = "fetch_google_data",
    python_callable=_fetch_google_data,
    op_kwargs={
        "company": {"Google"},
        "output_path": "/tmp/{{ execution_date.year }}_{{ execution_date.month }}_{{ execution_date.day }}_{{ execution_date.hour }}_GOOGLE",
    },
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


start >> google_operator
google_operator >> [save_data, save_google_data]
save_google_data >> fetch_google_data
save_data >> titleviews >> write_to_postgres