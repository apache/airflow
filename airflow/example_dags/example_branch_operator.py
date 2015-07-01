from airflow.operators import BranchPythonOperator, DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import random

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())
args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(dag_id='example_branch_operator', default_args=args)

cmd = 'ls -l'
run_this_first = DummyOperator(task_id='run_this_first', dag=dag)

options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: random.choice(options),
    dag=dag)
branching.set_upstream(run_this_first)

for option in options:
    t = DummyOperator(task_id=option, dag=dag)
    t.set_upstream(branching)
    dummy_follow = DummyOperator(task_id='follow_' + option, dag=dag)
    t.set_downstream(dummy_follow)
