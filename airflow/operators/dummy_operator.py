from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DummyOperator(BaseOperator):
    """
    Operator that does literally nothing. It can be used to group tasks in a
    DAG.
    """

    template_fields = tuple()
    ui_color = '#e8f7e4'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DummyOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        pass
