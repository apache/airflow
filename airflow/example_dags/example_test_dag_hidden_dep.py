from airflow import DAG
from airflow.models.param import Param
from airflow.operators.dummy import DummyOperator
from datetime import datetime

def waiter(secs):
    @task(task_id=str(secs))
    def wait():
        sleep(secs)
    return wait()

@task_group
def two():
    t1 = waiter(60)
    t2 = waiter(65)
    bookend = DummyOperator(task_id="bookend")
    [t1, t2] >> bookend
    return bookend

with DAG(
    dag_id="hidden_dep",
    schedule_interval=None,
) as dag:
    # based on the dag image, you'd expect this task group finish last,
    # since it has a 100 second task
    with TaskGroup(group_id="group1") as tg1:
        t1 = waiter(5)
        t50 = waiter(100)
    # but actually, the blue dot at the end of the group means nothing
    # the dummy below runs after just five seconds
    tg2 = two()

    [tg1, tg2] >> DummyOperator(task_id="done", trigger_rule=TriggerRule.ONE_SUCCESS)
