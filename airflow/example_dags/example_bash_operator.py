from airflow.operators import BashOperator, DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())
args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(dag_id='example_bash_operator', default_args=args)

cmd = 'ls -l'
run_this_last = DummyOperator(task_id='run_this_last', dag=dag)

run_this = BashOperator(
    task_id='run_after_loop', bash_command='echo 1', dag=dag)
run_this.set_downstream(run_this_last)

for i in range(3):
    i = str(i)
    task = BashOperator(
        task_id='runme_'+i,
        bash_command='echo "{{ task_instance_key_str }}" && sleep ' + str(i),
        dag=dag)
    task.set_downstream(run_this)

task = BashOperator(
    task_id='also_run_this',
    bash_command='echo "{{ macros.uuid.uuid1() }}"',
    dag=dag)
task.set_downstream(run_this_last)
