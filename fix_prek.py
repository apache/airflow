content = open('airflow/providers/celery/executors/celery_executor.py', encoding='utf-8').read()
old = 'from airflow.providers.celery.executors import celery_executor_utils as _celery_executor_utils  # noqa: F401 # Needed to register execute_command with Celery app at worker startup, see #63043'
new = 'from airflow.providers.celery.executors import (\n    celery_executor_utils as _celery_executor_utils,  # noqa: F401 # Needed to register execute_command with Celery app at worker startup, see #63043\n)'
assert old in content, 'Pattern not found!'
result = content.replace(old, new)
open('airflow/providers/celery/executors/celery_executor.py', 'w', encoding='utf-8', newline='\n').write(result)
print('Done')
