
"""This example illustrates the use of the TriggerDagRunOperator. There are 2
entities at work in this scenario:
1. The Controller DAG - the DAG that conditionally executes the trigger
2. The Target DAG - DAG being triggered (in example_trigger_target_dag.py)

This example illustrates the following features :
1. A TriggerDagRunOperator that takes:
  a. A python callable that decides whether or not to trigger the Target DAG
  b. An optional params dict passed to the python callable to help in
     evaluating whether or not to trigger the Target DAG
  c. The id (name) of the Target DAG
  d. The python callable can add contextual info to the DagRun created by
     way of adding a Pickleable payload (e.g. dictionary of primitives). This
     state is then made available to the TargetDag
2. A Target DAG : c.f. example_trigger_target_dag.py
"""

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

import pprint

pp = pprint.PrettyPrinter(indent=4)


def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    # Demonstrates use of the custom execution date in the conditional trigger
    if dag_run_obj.execution_date < datetime(2016, 1, 1):
        dag_run_obj.payload = {'message': context['params']['message']}
        return dag_run_obj
    return None

# Define the DAG
dag = DAG(dag_id='example_trigger_date_dag',
          default_args={"owner": "airflow",
                        "start_date": datetime.now()},
          schedule_interval='@once')

trigger = TriggerDagRunOperator(task_id='test_trigger_date_dagrun',
                                trigger_dag_id="example_trigger_target_dag",
                                python_callable=conditionally_trigger,
                                execution_date=datetime(2015, 1, 1),
                                params={'condition_param': True,
                                        'message': 'Hello World'},
                                dag=dag)

no_trigger = TriggerDagRunOperator(task_id='test_no_trigger_date_dagrun',
                                   trigger_dag_id="example_trigger_target_dag",
                                   python_callable=conditionally_trigger,
                                   params={'condition_param': True,
                                           'message': 'Hello World'},
                                   dag=dag)
