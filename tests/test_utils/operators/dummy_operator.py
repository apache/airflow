from __future__ import annotations
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class DummySuccessOperator(BaseOperator):
    """Very small operator used only for unit test examples."""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return {"ok": True}
