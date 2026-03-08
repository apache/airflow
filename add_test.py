content = open('providers/celery/tests/unit/celery/executors/test_celery_executor.py', encoding='utf-8').read()
new_test = '''

def test_celery_tasks_registered_on_import():
    """
    Ensure execute_workload (and execute_command for Airflow 2.x) are registered
    with the Celery app when celery_executor is imported.

    Regression test for https://github.com/apache/airflow/issues/63043
    Celery provider 3.17.0 exposed that celery_executor_utils was never imported
    at module level, so tasks were never registered at worker startup.
    """
    from airflow.providers.celery.executors.celery_executor_utils import app

    registered_tasks = list(app.tasks.keys())
    assert "execute_workload" in registered_tasks, (
        "execute_workload must be registered with the Celery app at import time. "
        "Workers need this to receive tasks without KeyError."
    )
    if not AIRFLOW_V_3_0_PLUS:
        assert "execute_command" in registered_tasks, (
            "execute_command must be registered for Airflow 2.x compatibility."
        )
'''
result = content + new_test
open('providers/celery/tests/unit/celery/executors/test_celery_executor.py', 'w', encoding='utf-8', newline='\n').write(result)
print('Done')
