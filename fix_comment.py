content = open('airflow/providers/celery/executors/celery_executor.py', encoding='utf-8').read()
old = 'from airflow.executors.base_executor import BaseExecutor\n# Must be imported at module level to register execute_command with the Celery app\n# and connect the celery_import_modules signal handler at worker startup.\n# See: https://github.com/apache/airflow/issues/63043\nfrom airflow.providers.celery.executors import celery_executor_utils as _celery_executor_utils  # noqa: F401\nfrom airflow.stats import Stats\nfrom airflow.utils.state import TaskInstanceState'
new = 'from airflow.executors.base_executor import BaseExecutor\nfrom airflow.providers.celery.executors import celery_executor_utils as _celery_executor_utils  # noqa: F401 # Needed to register execute_command with Celery app at worker startup, see #63043\nfrom airflow.stats import Stats\nfrom airflow.utils.state import TaskInstanceState'
assert old in content, 'Pattern not found!'
result = content.replace(old, new)
open('airflow/providers/celery/executors/celery_executor.py', 'w', encoding='utf-8', newline='\n').write(result)
print('Done')
