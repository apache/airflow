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

import uuid
from datetime import datetime

import pytest

from airflow._shared.timezones import timezone
from airflow.callbacks.callback_requests import (
    DagCallbackRequest,
    TaskCallbackRequest,
)
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.state import State, TaskInstanceState

pytestmark = pytest.mark.db_test


class TestCallbackRequest:
    @pytest.mark.parametrize(
        "input,request_class",
        [
            (
                None,  # to be generated when test is run
                TaskCallbackRequest,
            ),
            (
                DagCallbackRequest(
                    filepath="filepath",
                    dag_id="fake_dag",
                    run_id="fake_run",
                    is_failure_callback=False,
                    bundle_name="testing",
                    bundle_version=None,
                ),
                DagCallbackRequest,
            ),
        ],
    )
    def test_from_json(self, input, request_class):
        if input is None:
            ti = TaskInstance(
                task=BashOperator(
                    task_id="test",
                    bash_command="true",
                    start_date=datetime.now(),
                    dag=DAG(dag_id="id", schedule=None),
                ),
                run_id="fake_run",
                state=State.RUNNING,
                dag_version_id=uuid.uuid4(),
            )
            ti.start_date = timezone.utcnow()

            input = TaskCallbackRequest(
                filepath="filepath", ti=ti, bundle_name="testing", bundle_version=None
            )
        json_str = input.to_json()
        result = request_class.from_json(json_str)
        assert result == input

    def test_taskcallback_to_json_with_start_date_and_end_date(self, session, create_task_instance):
        ti = create_task_instance()
        ti.start_date = timezone.utcnow()
        ti.end_date = timezone.utcnow()
        session.merge(ti)
        session.flush()
        input = TaskCallbackRequest(filepath="filepath", ti=ti, bundle_name="testing", bundle_version=None)
        json_str = input.to_json()
        result = TaskCallbackRequest.from_json(json_str)
        assert input == result

    @pytest.mark.parametrize(
        "task_callback_type,expected_is_failure",
        [
            (None, True),
            (TaskInstanceState.FAILED, True),
            (TaskInstanceState.UP_FOR_RETRY, True),
            (TaskInstanceState.UPSTREAM_FAILED, True),
            (TaskInstanceState.SUCCESS, False),
            (TaskInstanceState.RUNNING, False),
        ],
    )
    def test_is_failure_callback_property(
        self, task_callback_type, expected_is_failure, create_task_instance
    ):
        """Test is_failure_callback property with different task callback types"""
        ti = create_task_instance()

        request = TaskCallbackRequest(
            filepath="filepath",
            ti=ti,
            bundle_name="testing",
            bundle_version=None,
            task_callback_type=task_callback_type,
        )

        assert request.is_failure_callback == expected_is_failure


class TestDagCallbackRequest:
    """Test the DagCallbackRequest class with the new dag_run field."""

    def test_dag_callback_request_with_dag_run(self):
        """Test DagCallbackRequest creation with dag_run field."""
        dag_run_data = {
            "dag_id": "test_dag",
            "run_id": "test_run_2024-01-01T00:00:00+00:00",
            "state": "success",
            "logical_date": "2024-01-01T00:00:00+00:00",
            "start_date": "2024-01-01T00:00:00+00:00",
            "end_date": "2024-01-01T01:00:00+00:00",
            "run_type": "manual",
            "run_after": "2024-01-01T00:00:00+00:00",
            "conf": {"key": "value"},
            "data_interval_start": "2024-01-01T00:00:00+00:00",
            "data_interval_end": "2024-01-01T01:00:00+00:00",
        }

        request = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run_2024-01-01T00:00:00+00:00",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
            dag_run=dag_run_data,
        )

        assert request.dag_run == dag_run_data
        assert request.dag_run["dag_id"] == "test_dag"
        assert request.dag_run["run_id"] == "test_run_2024-01-01T00:00:00+00:00"
        assert request.dag_run["state"] == "success"
        assert request.dag_run["conf"]["key"] == "value"

    def test_dag_callback_request_without_dag_run(self):
        """Test DagCallbackRequest creation without dag_run field (backward compatibility)."""
        request = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
        )

        assert request.dag_run is None
        assert request.dag_id == "test_dag"
        assert request.run_id == "test_run"

    def test_dag_callback_request_serialization_with_dag_run(self):
        """Test DagCallbackRequest serialization and deserialization with dag_run field."""
        dag_run_data = {
            "dag_id": "test_dag",
            "run_id": "test_run_2024-01-01T00:00:00+00:00",
            "state": "success",
            "logical_date": "2024-01-01T00:00:00+00:00",
            "start_date": "2024-01-01T00:00:00+00:00",
            "end_date": "2024-01-01T01:00:00+00:00",
            "run_type": "manual",
            "run_after": "2024-01-01T00:00:00+00:00",
            "conf": {"key": "value", "nested": {"inner": "data"}},
            "data_interval_start": "2024-01-01T00:00:00+00:00",
            "data_interval_end": "2024-01-01T01:00:00+00:00",
        }

        original_request = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run_2024-01-01T00:00:00+00:00",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
            dag_run=dag_run_data,
        )

        # Serialize to JSON
        json_str = original_request.to_json()

        # Deserialize from JSON
        deserialized_request = DagCallbackRequest.from_json(json_str)

        # Verify all fields are preserved
        assert deserialized_request == original_request
        assert deserialized_request.dag_run == dag_run_data
        assert deserialized_request.dag_run["conf"]["nested"]["inner"] == "data"

    def test_dag_callback_request_serialization_without_dag_run(self):
        """Test DagCallbackRequest serialization and deserialization without dag_run field."""
        original_request = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=True,
            bundle_name="testing",
            bundle_version=None,
            msg="task_failure",
        )

        # Serialize to JSON
        json_str = original_request.to_json()

        # Deserialize from JSON
        deserialized_request = DagCallbackRequest.from_json(json_str)

        # Verify all fields are preserved
        assert deserialized_request == original_request
        assert deserialized_request.dag_run is None

    def test_dag_callback_request_with_none_dag_run(self):
        """Test DagCallbackRequest with explicitly None dag_run field."""
        request = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
            dag_run=None,
        )

        assert request.dag_run is None

    def test_dag_callback_request_with_minimal_dag_run_data(self):
        """Test DagCallbackRequest with minimal dag_run data."""
        minimal_dag_run_data = {
            "dag_id": "test_dag",
            "run_id": "test_run",
            "state": "success",
        }

        request = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
            dag_run=minimal_dag_run_data,
        )

        assert request.dag_run == minimal_dag_run_data
        assert request.dag_run["dag_id"] == "test_dag"
        assert request.dag_run["run_id"] == "test_run"
        assert request.dag_run["state"] == "success"

    def test_dag_callback_request_with_null_values_in_dag_run(self):
        """Test DagCallbackRequest with null values in dag_run data."""
        dag_run_data_with_nulls = {
            "dag_id": "test_dag",
            "run_id": "test_run",
            "state": "success",
            "logical_date": None,
            "start_date": "2024-01-01T00:00:00+00:00",
            "end_date": None,
            "run_type": "manual",
            "run_after": "2024-01-01T00:00:00+00:00",
            "conf": None,
            "data_interval_start": None,
            "data_interval_end": None,
        }

        request = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
            dag_run=dag_run_data_with_nulls,
        )

        assert request.dag_run == dag_run_data_with_nulls
        assert request.dag_run["logical_date"] is None
        assert request.dag_run["end_date"] is None
        assert request.dag_run["conf"] is None

    def test_dag_callback_request_equality_with_dag_run(self):
        """Test DagCallbackRequest equality comparison with dag_run field."""
        dag_run_data = {
            "dag_id": "test_dag",
            "run_id": "test_run",
            "state": "success",
            "conf": {"key": "value"},
        }

        request1 = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
            dag_run=dag_run_data,
        )

        request2 = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
            dag_run=dag_run_data,
        )

        request3 = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
            dag_run={"dag_id": "different_dag", "run_id": "test_run", "state": "success"},
        )

        assert request1 == request2
        assert request1 != request3
        assert request2 != request3

    def test_dag_callback_request_equality_without_dag_run(self):
        """Test DagCallbackRequest equality comparison without dag_run field."""
        request1 = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
        )

        request2 = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
        )

        request3 = DagCallbackRequest(
            filepath="test_dag.py",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
            dag_run={"dag_id": "test_dag", "run_id": "test_run"},
        )

        assert request1 == request2
        assert request1 != request3  # Different because one has dag_run and the other doesn't
