from airflow.models import DAG
from airflow.operators import DummyOperator


def subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
      dag_id='%s.%s' % (parent_dag_name, child_dag_name),
      default_args=args,
      schedule_interval="@daily",
    )

    for i in range(5):
        DummyOperator(
            task_id='%s-task-%s' % (child_dag_name, i + 1),
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag
