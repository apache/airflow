import pendulum

from airflow import DAG
from airflow.example_dags.plugins.workday import AfterWorkdayTimetable
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="example_workday_timetable",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=AfterWorkdayTimetable(),
    tags=["example", "timetable"],
):
    EmptyOperator(task_id="run_this")
