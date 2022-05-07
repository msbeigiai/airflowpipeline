import json
import airflow
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException
from airflow import DAG
import airflow.utils.dates
import requests
from pathlib import Path



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

# def _wait_for_google(year, month, day, hour):
#     google_path = Path("/tmp/" + f"{year}_{month}_{day}_{hour}_GOOGLE")


# wait_for_google = PythonSensor(
#     task_id="wait_for_google",
#     python_callable=_wait_for_google,
#     op_kwargs={
#         "year": "{{ execution_date.year }}",
#         "month": "{{ execution_date.month }}",
#         "day": "{{ execution_date.day }}",
#         "hour": "{{ execution_date.hour }}",
#     },
#     dag=dag,
# )

def _pick_what_company():
    url = "http://172.17.0.1:5000"
    res = requests.get(url)
    data = res.json()["Companies"]
    for company in data:
        if company == "Google":
            return "save_google_data"
        else:
            return "save_data"
        

pick_what_company = BranchPythonOperator(
    task_id="pick_google_company",
    python_callable=_pick_what_company,
    dag=dag,
)

def _save_google_data(output_path, year, month, day, hour):
     
    url = "http://172.17.0.1:5000"
    res = requests.get(url)
    data = res.json()
    remove_entities = [key for key in data["Companies"]]
    for k in remove_entities:
        if k == "Google":
            continue
        data["Companies"].pop(k)
    
    if data["Companies"] is None:
        raise AirflowSkipException
    else:
        with open(f"{output_path}/{year}_{month}_{day}_{hour}_GOOGLE.json", "w") as f:
            f.write(json.dumps(data))

# google_operator = DummyOperator(
#     task_id="google_operator",
#     dag=dag,
# )

save_google_data = PythonOperator(
    task_id="save_google_data",
    python_callable=_save_google_data,
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
        for google, title in results.items():
            f.write(
                "INSERT INTO google_table VALUES ("
                    f"'{google}', {title}"
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
        "companies": {"Apple", "Facebook", "Tesla"},
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

write_google_to_postgres = PostgresOperator(
    task_id="write_google_postgres",
    postgres_conn_id='postgres_conn_01',
    sql='query_google.sql',
    dag=dag,
)


start >> pick_what_company
pick_what_company >> [save_data, save_google_data]
save_google_data >> fetch_google_data >> write_google_to_postgres
save_data >> titleviews >> write_to_postgres