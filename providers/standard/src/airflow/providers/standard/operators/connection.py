from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

class TestConnectionOperator(BaseOperator):
    def __init__(self, conn_id: str, *args, **kwargs):
        self.conn_id = conn_id
        super().__init__(*args, **kwargs)

    def execute(self, context):
        hook = BaseHook.get_hook(conn_id=self.conn_id)
        if test_conn := getattr(hook, "test_connection"):
            (is_success, status) = test_conn()
            if not is_success:
                raise RuntimeError(status)
            return status
        raise RuntimeError(f"Connection {self.conn_id} does not support test_connection method")
