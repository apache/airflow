from airflow.operators import ShortCircuitOperator, DummyOperator
from airflow.models import DAG
import airflow.utils.helpers
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())
args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(dag_id='example_short_circuit_operator', default_args=args)

cond_true = ShortCircuitOperator(
    task_id='condition_is_True', python_callable=lambda: True, dag=dag)

cond_false = ShortCircuitOperator(
    task_id='condition_is_False', python_callable=lambda: False, dag=dag)

ds_true = [DummyOperator(task_id='true_' + str(i), dag=dag) for i in [1, 2]]
ds_false = [DummyOperator(task_id='false_' + str(i), dag=dag) for i in [1, 2]]

airflow.utils.helpers.chain(cond_true, *ds_true)
airflow.utils.helpers.chain(cond_false, *ds_false)
