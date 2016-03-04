from datetime import datetime

from airflow.models import DAG
from airflow.operators import DummyOperator, SubDagOperator

from airflow.example_dags.subdags.subdag import subdag


DAG_NAME = 'example_subdag_operator'

args = {
    'owner': 'airflow',
    'start_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval="@once",
)

start = DummyOperator(
    task_id='start',
    default_args=args,
    dag=dag,
)

section_1 = SubDagOperator(
    task_id='section-1',
    subdag=subdag(DAG_NAME, 'section-1', args),
    default_args=args,
    dag=dag,
)

some_other_task = DummyOperator(
    task_id='some-other-task',
    default_args=args,
    dag=dag,
)

section_2 = SubDagOperator(
    task_id='section-2',
    subdag=subdag(DAG_NAME, 'section-2', args),
    default_args=args,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    default_args=args,
    dag=dag,
)

start.set_downstream(section_1)
section_1.set_downstream(some_other_task)
some_other_task.set_downstream(section_2)
section_2.set_downstream(end)
