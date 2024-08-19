from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook


class WorkloadIdentitySupportDatabricksHook(BaseDatabricksHook):
    ...

def test_x():
    hook = WorkloadIdentitySupportDatabricksHook(databricks_conn_id='databricks_default')
    
    
    hook._do_api_call(('GET', '/api/2.1/jobs/list'))
    print(x)