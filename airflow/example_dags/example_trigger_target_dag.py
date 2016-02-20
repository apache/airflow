from airflow.operators import *
from airflow.models import DAG
from datetime import date, datetime, time, timedelta

import pprint
pp = pprint.PrettyPrinter(indent=4)

# This example illustrates the use of the TriggerDagRunOperator. There are 2
# entities at work in this scenario:
# 1. The Controller DAG - the DAG that conditionally executes the trigger
#    (in example_trigger_controller.py)
# 2. The Target DAG - DAG being triggered
#
# This example illustrates the following features :
# 1. A TriggerDagRunOperator that takes:
#   a. A python callable that decides whether or not to trigger the Target DAG
#   b. An optional params dict passed to the python callable to help in
#      evaluating whether or not to trigger the Target DAG
#   c. The id (name) of the Target DAG
#   d. The python callable can add contextual info to the DagRun created by
#      way of adding a Pickleable payload (e.g. dictionary of primitives). This
#      state is then made available to the TargetDag
# 2. A Target DAG : c.f. example_trigger_target_dag.py

args = {
    'start_date': datetime.now(),
    'owner': 'airflow',
}

dag = DAG(
    dag_id='example_trigger_target_dag',
    default_args=args,
    schedule_interval=None)


def run_this_func(ds, **kwargs):
    print( "Remotely received value of {} for key=message".format(kwargs['dag_run'].conf['message']))

run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag)
