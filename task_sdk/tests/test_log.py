from __future__ import annotations

import logging
import unittest.mock

import orjson
import pytest
import structlog
from uuid6 import UUID

from airflow.sdk.api.datamodels._generated import TaskInstance


@pytest.mark.parametrize(
    "captured_logs", [(logging.INFO, "json")], indirect=True, ids=["log_level=info,formatter=json"]
)
def test_json_rendering(captured_logs):
    """
    Test that the JSON formatter renders correctly.
    """
    logger = structlog.get_logger()
    logger.info(
        "A test message with a Pydantic class",
        pydantic_class=TaskInstance(
            id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=1,
        ),
    )
    assert captured_logs
    assert isinstance(captured_logs[0], bytes)
    assert orjson.loads(captured_logs[0]) == {
        "event": "A test message with a Pydantic class",
        "pydantic_class": "TaskInstance(id=UUID('ffec3c8e-2898-46f8-b7d5-3cc571577368'), task_id='test_task', dag_id='test_dag', run_id='test_run', try_number=1, map_index=-1, hostname=None)",
        "timestamp": unittest.mock.ANY,
        "level": "info",
    }
