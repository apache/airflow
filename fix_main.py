content = open('providers/celery/src/airflow/providers/celery/executors/celery_executor.py', encoding='utf-8').read()
old = 'from airflow.executors.base_executor import BaseExecutor\nfrom airflow.providers.celery.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS\nfrom airflow.providers.common.compat.sdk import AirflowTaskTimeout, Stats'
new = 'from airflow.executors.base_executor import BaseExecutor\nfrom airflow.providers.celery.executors import (\n    celery_executor_utils as _celery_executor_utils,  # noqa: F401 # Needed to register Celery tasks at worker startup, see #63043\n)\nfrom airflow.providers.celery.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS\nfrom airflow.providers.common.compat.sdk import AirflowTaskTimeout, Stats'
assert old in content, 'Pattern not found!'
result = content.replace(old, new)
open('providers/celery/src/airflow/providers/celery/executors/celery_executor.py', 'w', encoding='utf-8', newline='\n').write(result)
print('Done')
