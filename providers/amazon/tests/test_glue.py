import pytest
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.exceptions import TaskDeferred
from unittest.mock import patch

@patch("airflow.providers.amazon.aws.hooks.glue.GlueJobHook.get_or_create_glue_job", return_value="dummy-job")
@patch("airflow.providers.amazon.aws.hooks.glue.GlueJobHook.initialize_job", return_value={"JobRunId": "run-id-123"})
def test_gluejoboperator_deferrable(mock_init_job, mock_get_job):
    operator = GlueJobOperator(
        task_id="test_deferrable",
        job_name="dummy-job",
        deferrable=True
    )
    with pytest.raises(TaskDeferred):
        operator.execute(context={})