
from __future__ import annotations

from airflow.models.baseoperator import BaseOperator

class DummySuccessOperator(BaseOperator):
    """Very small operator used only for unit test examples."""

    def execute(self, context):
        return {"ok": True}
