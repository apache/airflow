# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import sys
from datetime import timedelta
from unittest import mock

import pytest
from uuid6 import uuid7

from airflow.sdk import BaseAsyncOperator, BaseOperator, timezone
from airflow.sdk.api.datamodels._generated import TaskInstanceState
from airflow.sdk.execution_time.task_runner import IndexedTaskInstance


@pytest.fixture
def disable_capturing():
    old_in, old_out, old_err = sys.stdin, sys.stdout, sys.stderr

    sys.stdin = sys.__stdin__
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    yield
    sys.stdin, sys.stdout, sys.stderr = old_in, old_out, old_err


@pytest.fixture
def make_indexed_ti():
    """Factory for creating IndexedTaskInstance objects for testing."""

    def _make_indexed_ti(
        *,
        task_id: str = "my_task",
        dag_id: str = "my_dag",
        run_id: str = "run_1",
        map_index: int = -1,
        index: int | None = 0,
        try_number: int = 0,
        max_tries: int = 3,
        is_async: bool = False,
        retry_delay: timedelta = None,
        retry_exponential_backoff: bool = False,
        max_retry_delay: timedelta | None = None,
        end_date=None,
        start_date=None,
        logical_date=None,
        do_xcom_push: bool = True,
    ) -> IndexedTaskInstance:
        """Create a IndexedTaskInstance via model_construct to bypass full Pydantic validation."""
        if retry_delay is None:
            retry_delay = timedelta(seconds=300)

        # Set defaults for dates if not provided
        if end_date is None:
            end_date = timezone.datetime(2024, 12, 3, 10, 0, 0)
        if start_date is None:
            start_date = timezone.datetime(2024, 12, 3, 9, 55, 0)
        if logical_date is None:
            logical_date = timezone.datetime(2024, 12, 3, 0, 0, 0)

        operator_cls = BaseAsyncOperator if is_async else BaseOperator
        operator = mock.create_autospec(operator_cls, instance=True)
        operator.task_id = task_id
        operator.dag_id = dag_id
        operator.is_async = is_async
        operator.retries = max_tries
        operator.retry_delay = retry_delay
        operator.retry_exponential_backoff = retry_exponential_backoff
        operator.max_retry_delay = max_retry_delay
        operator.do_xcom_push = do_xcom_push

        return IndexedTaskInstance.model_construct(
            id=uuid7(),
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            map_index=map_index,
            index=index,
            try_number=try_number,
            max_tries=max_tries,
            state=TaskInstanceState.SCHEDULED,
            is_mapped=True,
            task=operator,
            xcom_pushed=False,
            dag_version_id=uuid7(),
            end_date=end_date,
            start_date=start_date,
            logical_date=logical_date,
        )

    return _make_indexed_ti
