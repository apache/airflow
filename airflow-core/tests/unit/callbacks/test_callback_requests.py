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
from pydantic import TypeAdapter

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    DagRun as DRDataModel,
    TaskInstance as TIDataModel,
    TIRunContext,
)
from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    DagRunContext,
    EmailRequest,
    TaskCallbackRequest,
)
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.state import State, TaskInstanceState

pytestmark = pytest.mark.db_test


class TestCallbackRequest:
    @pytest.mark.parametrize(
        ("input", "request_class"),
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
        ("task_callback_type", "expected_is_failure"),
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


class TestDagRunContext:
    def test_dagrun_context_creation(self):
        """Test DagRunContext can be created with dag_run and first_ti"""
        current_time = timezone.utcnow()
        dag_run_data = DRDataModel(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=current_time,
            data_interval_start=current_time,
            data_interval_end=current_time,
            run_after=current_time,
            start_date=current_time,
            end_date=None,
            run_type="manual",
            state="running",
            consumed_asset_events=[],
        )

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        context = DagRunContext(dag_run=dag_run_data, last_ti=ti_data)

        assert context.dag_run == dag_run_data
        assert context.last_ti == ti_data

    def test_dagrun_context_none_values(self):
        """Test DagRunContext can be created with None values"""
        context = DagRunContext()
        assert context.dag_run is None
        assert context.last_ti is None

    def test_dagrun_context_serialization(self):
        """Test DagRunContext can be serialized and deserialized"""
        current_time = timezone.utcnow()
        dag_run_data = DRDataModel(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=current_time,
            data_interval_start=current_time,
            data_interval_end=current_time,
            run_after=current_time,
            start_date=current_time,
            end_date=None,
            run_type="manual",
            state="running",
            consumed_asset_events=[],
        )

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        context = DagRunContext(dag_run=dag_run_data, last_ti=ti_data)

        # Test serialization
        serialized = context.model_dump_json()

        # Test deserialization
        deserialized = DagRunContext.model_validate_json(serialized)

        assert deserialized.dag_run.dag_id == context.dag_run.dag_id
        assert deserialized.last_ti.task_id == context.last_ti.task_id


class TestDagCallbackRequestWithContext:
    def test_dag_callback_request_with_context_from_server(self):
        """Test DagCallbackRequest with context_from_server field"""
        current_time = timezone.utcnow()
        dag_run_data = DRDataModel(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=current_time,
            data_interval_start=current_time,
            data_interval_end=current_time,
            run_after=current_time,
            start_date=current_time,
            end_date=None,
            run_type="manual",
            state="running",
            consumed_asset_events=[],
        )

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        context_from_server = DagRunContext(dag_run=dag_run_data, last_ti=ti_data)

        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            context_from_server=context_from_server,
            is_failure_callback=True,
            msg="test_failure",
        )

        assert request.context_from_server is not None
        assert request.context_from_server.dag_run.dag_id == "test_dag"
        assert request.context_from_server.last_ti.task_id == "test_task"

    def test_dag_callback_request_without_context_from_server(self):
        """Test DagCallbackRequest without context_from_server field"""
        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            is_failure_callback=True,
            msg="test_failure",
        )

        assert request.context_from_server is None

    def test_dag_callback_request_serialization_with_context(self):
        """Test DagCallbackRequest can be serialized and deserialized with context_from_server"""
        current_time = timezone.utcnow()
        dag_run_data = DRDataModel(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=current_time,
            data_interval_start=current_time,
            data_interval_end=current_time,
            run_after=current_time,
            start_date=current_time,
            end_date=None,
            run_type="manual",
            state="running",
            consumed_asset_events=[],
        )

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        context_from_server = DagRunContext(dag_run=dag_run_data, last_ti=ti_data)

        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            context_from_server=context_from_server,
            is_failure_callback=True,
            msg="test_failure",
        )

        # Test serialization
        json_str = request.to_json()

        # Test deserialization
        result = DagCallbackRequest.from_json(json_str)

        assert result == request
        assert result.context_from_server is not None
        assert result.context_from_server.dag_run.dag_id == "test_dag"
        assert result.context_from_server.last_ti.task_id == "test_task"


class TestEmailRequest:
    def test_email_notification_request_serialization(self):
        """Test EmailRequest can be serialized and used in CallbackRequest union."""
        ti_data = TIDataModel(
            id=str(uuid.uuid4()),
            task_id="test_task",
            dag_id="test_dag",
            run_id="test_run",
            logical_date="2023-01-01T00:00:00Z",
            try_number=1,
            attempt_number=1,
            state="failed",
            dag_version_id=str(uuid.uuid4()),
        )

        current_time = timezone.utcnow()

        # Create EmailRequest
        email_request = EmailRequest(
            filepath="/path/to/dag.py",
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            ti=ti_data,
            context_from_server=TIRunContext(
                dag_run=DRDataModel(
                    dag_id="test_dag",
                    run_id="test_run",
                    logical_date="2023-01-01T00:00:00Z",
                    data_interval_start=current_time,
                    data_interval_end=current_time,
                    run_after=current_time,
                    start_date=current_time,
                    end_date=None,
                    run_type="manual",
                    state="running",
                    consumed_asset_events=[],
                ),
                max_tries=2,
            ),
            email_type="failure",
            msg="Task failed",
        )

        # Test serialization
        json_str = email_request.to_json()
        assert "EmailRequest" in json_str
        assert "failure" in json_str

        # Test deserialization
        result = EmailRequest.from_json(json_str)
        assert result == email_request
        assert result.email_type == "failure"
        assert result.ti.task_id == "test_task"

    def test_callback_request_union_with_email_notification(self):
        """Test EmailRequest works in CallbackRequest union type."""
        ti_data = TIDataModel(
            id=str(uuid.uuid4()),
            task_id="test_task",
            dag_id="test_dag",
            run_id="test_run",
            logical_date="2023-01-01T00:00:00Z",
            try_number=1,
            attempt_number=1,
            state="failed",
            dag_version_id=str(uuid.uuid4()),
        )

        current_time = timezone.utcnow()

        context_from_server = TIRunContext(
            dag_run=DRDataModel(
                dag_id="test_dag",
                run_id="test_run",
                logical_date="2023-01-01T00:00:00Z",
                data_interval_start=current_time,
                data_interval_end=current_time,
                run_after=current_time,
                start_date=current_time,
                end_date=None,
                run_type="manual",
                state="running",
                consumed_asset_events=[],
            ),
            max_tries=2,
        )

        email_data = {
            "type": "EmailRequest",
            "filepath": "/path/to/dag.py",
            "bundle_name": "test_bundle",
            "bundle_version": "1.0.0",
            "ti": ti_data.model_dump(),
            "context_from_server": context_from_server.model_dump(),
            "email_type": "retry",
            "msg": "Task retry",
        }

        # Validate as CallbackRequest union
        adapter = TypeAdapter(CallbackRequest)
        callback_request = adapter.validate_python(email_data)

        # Verify it's correctly identified as EmailRequest
        assert isinstance(callback_request, EmailRequest)
        assert callback_request.email_type == "retry"
        assert callback_request.ti.task_id == "test_task"
