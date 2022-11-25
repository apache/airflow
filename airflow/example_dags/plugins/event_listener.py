from airflow.listeners import hookimpl
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.state import TaskInstanceState



@hookimpl
def on_task_instance_running(
    previous_state: "TaskInstanceState", task_instance: "TaskInstance", session):
    """
        This method is called when task state changes to RUNNING.
        Through callback, parameters like previous_task_state, task_instance object can be accessed.
        This will give more information about current task_instance that is running its dag_run, task and dag information. 
    """
    print("Task instance is in running state")
    print(" Previous state of the Task instance:", previous_state)
    
    current_task_instance_state: TaskInstanceState = task_instance.state
    task_instance_name: str = task_instance.task_id
    task_instance_start_date = task_instance.start_date

    dagrun = task_instance.dag_run
    dagrun_status = dagrun.state

    task = task_instance.task

    dag = task_instance.task.dag
    dag_name = dag.dag_id
    
    
    pass

@hookimpl
def on_task_instance_success(
    previous_state: "TaskInstanceState", task_instance: "TaskInstance", session):
    """
        This method is called when task state changes to SUCCESS.
        Through callback, parameters like previous_task_state, task_instance object can be accessed.
        This will give more information about current task_instance that has succeeded its dag_run, task and dag information. 
    """
    print("Task instance in success state")
    print(" Previous state of the Task instance:", previous_state)
    
    dag_id = task_instance.dag_id
    hostname = task_instance.hostname
    operator = task_instance.operator
    
    dagrun = task_instance.dag_run
    queued_at = dagrun.queued_at

    pass

@hookimpl
def on_task_instance_failed(
    previous_state: "TaskInstanceState", task_instance: "TaskInstance", session):
    """
        This method is called when task state changes to FAILED.
        Through callback, parameters like previous_task_state, task_instance object can be accessed.
        This will give more information about current task_instance that has failed its dag_run, task and dag information. 
    """
    print("Task instance in failure state")
    
    task_instance_start_date = task_instance.start_date
    task_instance_end_date = task_instance.end_date
    task_instance_duration = task_instance.duration

    dagrun = task_instance.dag_run

    task = task_instance.task
    
    dag = task_instance.task.dag
    pass