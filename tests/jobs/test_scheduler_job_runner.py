import pytest
from unittest import mock
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.operators.empty import EmptyOperator

def test_should_update_dag_next_dagruns_logging(dag_maker, session):
    # 1. Create a DAG with max_active_runs=1
    with dag_maker(dag_id='test_max_active_runs_logging', max_active_runs=1):
        task1 = EmptyOperator(task_id='task1')

    # 2. Create a RUNNING DagRun and TaskInstance in the DB
    dr = dag_maker.create_dagrun(state=DagRunState.RUNNING)
    ti = dr.get_task_instance(task_id='task1')
    ti.state = TaskInstanceState.RUNNING
    session.merge(ti)
    session.flush()
    
    # 3. Initialize the SchedulerJobRunner
    job = Job()
    scheduler_job_runner = SchedulerJobRunner(job=job)
    
    # 4. Mock the logger and call the method
    with mock.patch.object(scheduler_job_runner.log, 'info') as mock_info:
        dag = dag_maker.dag
        dag_model = dag_maker.dag_model
        
        result = scheduler_job_runner._should_update_dag_next_dagruns(
            dag=dag,
            dag_model=dag_model,
            total_active_runs=1,
            session=session
        )
        
        # 5. Assert the return value and the exact log message formatting
        assert result is False
        
        expected_active_tasks_str = f"[{ti.task_id} in {dr.run_id} ({ti.state})]"
        mock_info.assert_called_with(
            "DAG %s is at (or above) max_active_runs (%d of %d), not creating any"
            " more runs. Active Runs: %s | Active Tasks: %s",
            'test_max_active_runs_logging',
            1,
            1,
            dr.run_id,
            expected_active_tasks_str,
        )
