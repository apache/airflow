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

import functools
import itertools
import warnings
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest import mock
from unittest.mock import MagicMock, patch

import pendulum
import pytest
from azure.core.exceptions import ResourceNotFoundError

from airflow.models import DAG, Connection
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred, timezone
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.providers.microsoft.azure.operators import data_factory as data_factory_module
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.triggers.data_factory import AzureDataFactoryTrigger
from airflow.utils.types import DagRunType

from tests_common.test_utils.taskinstance import create_task_instance as _create_task_instance
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_3_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.sdk import Context

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
SUBSCRIPTION_ID = "my-subscription-id"
TASK_ID = "run_pipeline_op"
AZURE_DATA_FACTORY_CONN_ID = "azure_data_factory_test"
PIPELINE_NAME = "pipeline1"
CONN_EXTRAS = {
    "subscriptionId": SUBSCRIPTION_ID,
    "tenantId": "my-tenant-id",
    "resource_group_name": "my-resource-group-name-from-conn",
    "factory_name": "my-factory-name-from-conn",
}
PIPELINE_RUN_RESPONSE = {"additional_properties": {}, "run_id": "run_id"}
EXPECTED_PIPELINE_RUN_OP_EXTRA_LINK = (
    "https://adf.azure.com/en-us/monitoring/pipelineruns/{run_id}"
    "?factory=/subscriptions/{subscription_id}/"
    "resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/"
    "factories/{factory_name}"
)
AZ_PIPELINE_RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"


class FakeTaskInstance:
    def __init__(self) -> None:
        self.stats_tags: dict[str, str] = {}
        self.xcom_values: list[tuple[str, Any]] = []

    def xcom_push(self, *, key: str, value: Any) -> None:
        self.xcom_values.append((key, value))


class FakeTaskStateStore:
    def __init__(self, stored: dict[str, Any] | None = None) -> None:
        self._store: dict[str, Any] = dict(stored or {})
        self.get_calls: list[str] = []
        self.set_calls: list[tuple[str, Any]] = []

    def get(self, key: str) -> Any:
        self.get_calls.append(key)
        return self._store.get(key)

    def set(self, key: str, value: Any) -> None:
        self.set_calls.append((key, value))
        self._store[key] = value


class TestAzureDataFactoryRunPipelineOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connection):
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}
        self.config = {
            "task_id": TASK_ID,
            "azure_data_factory_conn_id": AZURE_DATA_FACTORY_CONN_ID,
            "pipeline_name": PIPELINE_NAME,
            "resource_group_name": "resource-group-name",
            "factory_name": "factory-name",
            "check_interval": 1,
            "timeout": 3,
        }

        create_mock_connection(
            Connection(
                conn_id="azure_data_factory_test",
                conn_type="azure_data_factory",
                login="client-id",
                password="client-secret",
                extra=CONN_EXTRAS,
            )
        )

    @staticmethod
    def create_pipeline_run(status: str):
        """Helper function to create a mock pipeline run with a given execution status."""

        run = MagicMock()
        run.status = status

        return run

    @patch.object(AzureDataFactoryHook, "run_pipeline", return_value=MagicMock(**PIPELINE_RUN_RESPONSE))
    @pytest.mark.parametrize(
        ("pipeline_run_status", "expected_output"),
        [
            (AzureDataFactoryPipelineRunStatus.SUCCEEDED, None),
            (AzureDataFactoryPipelineRunStatus.FAILED, "exception"),
            (AzureDataFactoryPipelineRunStatus.CANCELLED, "exception"),
            (AzureDataFactoryPipelineRunStatus.IN_PROGRESS, "timeout"),
            (AzureDataFactoryPipelineRunStatus.QUEUED, "timeout"),
            (AzureDataFactoryPipelineRunStatus.CANCELING, "timeout"),
        ],
    )
    def test_execute_wait_for_termination(self, mock_run_pipeline, pipeline_run_status, expected_output):
        operator = AzureDataFactoryRunPipelineOperator(**self.config)

        assert operator.azure_data_factory_conn_id == self.config["azure_data_factory_conn_id"]
        assert operator.pipeline_name == self.config["pipeline_name"]
        assert operator.resource_group_name == self.config["resource_group_name"]
        assert operator.factory_name == self.config["factory_name"]
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert operator.wait_for_termination

        with patch.object(AzureDataFactoryHook, "get_pipeline_run") as mock_get_pipeline_run:
            mock_get_pipeline_run.return_value = TestAzureDataFactoryRunPipelineOperator.create_pipeline_run(
                pipeline_run_status
            )

            if not expected_output:
                # A successful operator execution should not return any values.
                assert not operator.execute(context=self.mock_context)
            elif expected_output == "exception":
                # The operator should fail if the pipeline run fails or is canceled.
                with pytest.raises(
                    AzureDataFactoryPipelineRunException,
                    match=f"Pipeline run {PIPELINE_RUN_RESPONSE['run_id']} has failed or has been cancelled.",
                ):
                    operator.execute(context=self.mock_context)
            else:
                # Demonstrating the operator timing out after surpassing the configured timeout value.
                # Mock time.monotonic and time.sleep so the poll count is deterministic regardless of
                # CI load; real sleep durations can vary enough to change how many iterations complete.
                with (
                    patch(
                        "airflow.providers.microsoft.azure.hooks.data_factory.time.monotonic",
                        side_effect=itertools.count(0.0, 1.0),
                    ),
                    patch("airflow.providers.microsoft.azure.hooks.data_factory.time.sleep"),
                    pytest.raises(
                        AzureDataFactoryPipelineRunException,
                        match=(
                            f"Pipeline run {PIPELINE_RUN_RESPONSE['run_id']} has not reached a terminal status "
                            f"after {self.config['timeout']} seconds."
                        ),
                    ),
                ):
                    operator.execute(context=self.mock_context)

            # Check the ``run_id`` attr is assigned after executing the pipeline.
            assert operator.run_id == PIPELINE_RUN_RESPONSE["run_id"]

            # Check to ensure an `XCom` is pushed regardless of pipeline run result.
            self.mock_ti.xcom_push.assert_called_once_with(
                key="run_id", value=PIPELINE_RUN_RESPONSE["run_id"]
            )

            mock_run_pipeline.assert_called_once_with(
                pipeline_name=self.config["pipeline_name"],
                resource_group_name=self.config["resource_group_name"],
                factory_name=self.config["factory_name"],
                reference_pipeline_run_id=None,
                is_recovery=None,
                start_activity_name=None,
                start_from_failure=None,
                parameters=None,
            )

            if pipeline_run_status in AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES:
                mock_get_pipeline_run.assert_called_once_with(
                    mock_run_pipeline.return_value.run_id,
                    self.config["resource_group_name"],
                    self.config["factory_name"],
                )
            else:
                # When the pipeline run status is not in a terminal status or "Succeeded", the operator will
                # continue to call ``get_pipeline_run()`` until a ``timeout`` number of seconds has passed
                # (3 seconds for this test).  Therefore, there should be 4 calls of this function: one
                # initially and 3 for each check done at a 1 second interval.
                assert mock_get_pipeline_run.call_count == 4

                mock_get_pipeline_run.assert_called_with(
                    mock_run_pipeline.return_value.run_id,
                    self.config["resource_group_name"],
                    self.config["factory_name"],
                )

    @patch.object(AzureDataFactoryHook, "run_pipeline", return_value=MagicMock(**PIPELINE_RUN_RESPONSE))
    def test_execute_no_wait_for_termination(self, mock_run_pipeline):
        operator = AzureDataFactoryRunPipelineOperator(wait_for_termination=False, **self.config)

        assert operator.azure_data_factory_conn_id == self.config["azure_data_factory_conn_id"]
        assert operator.pipeline_name == self.config["pipeline_name"]
        assert operator.resource_group_name == self.config["resource_group_name"]
        assert operator.factory_name == self.config["factory_name"]
        assert operator.check_interval == self.config["check_interval"]
        assert not operator.wait_for_termination

        with patch.object(AzureDataFactoryHook, "get_pipeline_run", autospec=True) as mock_get_pipeline_run:
            operator.execute(context=self.mock_context)

            # Check the ``run_id`` attr is assigned after executing the pipeline.
            assert operator.run_id == PIPELINE_RUN_RESPONSE["run_id"]

            # Check to ensure an `XCom` is pushed regardless of pipeline run result.
            self.mock_ti.xcom_push.assert_called_once_with(
                key="run_id", value=PIPELINE_RUN_RESPONSE["run_id"]
            )

            mock_run_pipeline.assert_called_once_with(
                pipeline_name=self.config["pipeline_name"],
                resource_group_name=self.config["resource_group_name"],
                factory_name=self.config["factory_name"],
                reference_pipeline_run_id=None,
                is_recovery=None,
                start_activity_name=None,
                start_from_failure=None,
                parameters=None,
            )

            # Checking the pipeline run status should _not_ be called when ``wait_for_termination`` is False.
            mock_get_pipeline_run.assert_not_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.run_pipeline")
    def test_run_id_extracted_from_hybrid_model_response(self, mock_run_pipeline):
        """Regression test: azure-mgmt-datafactory v10 hybrid models don't expose attributes via vars().

        Hybrid models use property descriptors, so vars(response)["run_id"] raises KeyError.
        The operator must use attribute access (response.run_id) which works with both old and new models.
        """

        class HybridModelResponse(dict):
            """Simulates an azure-mgmt-datafactory v10 hybrid model response."""

            def __init__(self):
                super().__init__({"runId": "hybrid-run-id-123"})

            @property
            def run_id(self):
                return self["runId"]

        mock_run_pipeline.return_value = HybridModelResponse()

        operator = AzureDataFactoryRunPipelineOperator(wait_for_termination=False, **self.config)
        operator.execute(context=self.mock_context)

        assert operator.run_id == "hybrid-run-id-123"
        self.mock_ti.xcom_push.assert_called_once_with(key="run_id", value="hybrid-run-id-123")

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("resource_group", "factory"),
        [
            # Both resource_group_name and factory_name are passed to the operator.
            ("op-resource-group", "op-factory-name"),
            # Only factory_name is passed to the operator; resource_group_name should fallback to Connection.
            (None, "op-factory-name"),
            # Only resource_group_name is passed to the operator; factory_nmae should fallback to Connection.
            ("op-resource-group", None),
            # Both resource_group_name and factory_name should fallback to Connection.
            (None, None),
        ],
    )
    def test_run_pipeline_operator_link(
        self, resource_group, factory, dag_maker, create_task_instance_of_operator, mock_supervisor_comms
    ):
        ti = create_task_instance_of_operator(
            AzureDataFactoryRunPipelineOperator,
            dag_id="test_adf_run_pipeline_op_link",
            task_id=TASK_ID,
            azure_data_factory_conn_id=AZURE_DATA_FACTORY_CONN_ID,
            pipeline_name=PIPELINE_NAME,
            resource_group_name=resource_group,
            factory_name=factory,
        )
        ti.xcom_push(key="run_id", value=PIPELINE_RUN_RESPONSE["run_id"])

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="run_id",
                value=PIPELINE_RUN_RESPONSE["run_id"],
            )

        task = dag_maker.dag.get_task(ti.task_id)
        url = task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key)
        EXPECTED_PIPELINE_RUN_OP_EXTRA_LINK = (
            "https://adf.azure.com/en-us/monitoring/pipelineruns/{run_id}"
            "?factory=/subscriptions/{subscription_id}/"
            "resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/"
            "factories/{factory_name}"
        )

        conn = AzureDataFactoryHook.get_connection("azure_data_factory_test")
        conn_resource_group_name = conn.extra_dejson["resource_group_name"]
        conn_factory_name = conn.extra_dejson["factory_name"]

        assert url == (
            EXPECTED_PIPELINE_RUN_OP_EXTRA_LINK.format(
                run_id=PIPELINE_RUN_RESPONSE["run_id"],
                subscription_id=SUBSCRIPTION_ID,
                resource_group_name=resource_group or conn_resource_group_name,
                factory_name=factory or conn_factory_name,
            )
        )


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS,
    reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
)
class TestAzureDataFactoryRunPipelineOperatorResumable:
    def make_operator(self, **kwargs: Any) -> AzureDataFactoryRunPipelineOperator:
        return AzureDataFactoryRunPipelineOperator(
            task_id=TASK_ID,
            azure_data_factory_conn_id=AZURE_DATA_FACTORY_CONN_ID,
            pipeline_name=PIPELINE_NAME,
            resource_group_name="resource-group-name",
            factory_name="factory-name",
            check_interval=1,
            timeout=3,
            **kwargs,
        )

    def make_hook(self, *, run_id: str = "new-run-id") -> MagicMock:
        hook: MagicMock = MagicMock(spec=AzureDataFactoryHook)
        hook.run_pipeline.return_value = SimpleNamespace(run_id=run_id)
        hook.wait_for_pipeline_run_status.return_value = True
        return hook

    def make_context(self, task_state_store: FakeTaskStateStore) -> tuple[Context, FakeTaskInstance]:
        task_instance = FakeTaskInstance()
        return cast("Context", {"task_state_store": task_state_store, "ti": task_instance}), task_instance

    def test_retry_reconnects_to_first_submission(self) -> None:
        operator = self.make_operator()
        hook = self.make_hook()
        operator.hook = hook
        task_state_store = FakeTaskStateStore()
        context, first_task_instance = self.make_context(task_state_store)
        persisted_before_poll: list[Any] = []

        def record_persisted_run_id(*args: Any, **kwargs: Any) -> bool:
            persisted_before_poll.append(task_state_store._store.get("azure_data_factory_run_id"))
            return True

        hook.wait_for_pipeline_run_status.side_effect = record_persisted_run_id

        operator.execute(context=context)

        retry_operator = self.make_operator()
        retry_operator.hook = hook
        retry_context, retry_task_instance = self.make_context(task_state_store)
        hook.get_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.IN_PROGRESS

        retry_operator.execute(context=retry_context)

        hook.run_pipeline.assert_called_once()
        hook.get_pipeline_run_status.assert_called_once_with(
            run_id="new-run-id",
            resource_group_name="resource-group-name",
            factory_name="factory-name",
        )
        assert persisted_before_poll == ["new-run-id", "new-run-id"]
        assert task_state_store.set_calls == [("azure_data_factory_run_id", "new-run-id")]
        assert first_task_instance.xcom_values == [("run_id", "new-run-id")]
        assert retry_task_instance.xcom_values == [("run_id", "new-run-id")]

    @pytest.mark.parametrize(
        ("prior_status", "expected_run_id", "should_submit", "should_poll"),
        [
            (AzureDataFactoryPipelineRunStatus.QUEUED, "prior-run-id", False, True),
            (AzureDataFactoryPipelineRunStatus.IN_PROGRESS, "prior-run-id", False, True),
            (AzureDataFactoryPipelineRunStatus.CANCELING, "prior-run-id", False, True),
            (AzureDataFactoryPipelineRunStatus.SUCCEEDED, "prior-run-id", False, False),
            (AzureDataFactoryPipelineRunStatus.FAILED, "new-run-id", True, True),
            (AzureDataFactoryPipelineRunStatus.CANCELLED, "new-run-id", True, True),
        ],
    )
    def test_retry_uses_prior_pipeline_status(
        self,
        prior_status: str,
        expected_run_id: str,
        should_submit: bool,
        should_poll: bool,
    ) -> None:
        operator = self.make_operator()
        hook = self.make_hook()
        hook.get_pipeline_run_status.return_value = prior_status
        operator.hook = hook
        task_state_store = FakeTaskStateStore({"azure_data_factory_run_id": "prior-run-id"})
        context, task_instance = self.make_context(task_state_store)

        operator.execute(context=context)

        assert hook.run_pipeline.called is should_submit
        assert hook.wait_for_pipeline_run_status.called is should_poll
        if should_poll:
            assert hook.wait_for_pipeline_run_status.call_args.kwargs["run_id"] == expected_run_id
        assert operator.run_id == expected_run_id
        expected_xcom_values = [("run_id", "prior-run-id")]
        if should_submit:
            expected_xcom_values.append(("run_id", "new-run-id"))
        assert task_instance.xcom_values == expected_xcom_values
        expected_set_calls = [("azure_data_factory_run_id", "new-run-id")] if should_submit else []
        assert task_state_store.set_calls == expected_set_calls

    def test_retry_restores_run_id_xcom_before_status_lookup(self) -> None:
        operator = self.make_operator()
        hook = self.make_hook()
        hook.get_pipeline_run_status.side_effect = RuntimeError("status unavailable")
        operator.hook = hook
        context, task_instance = self.make_context(
            FakeTaskStateStore({"azure_data_factory_run_id": "prior-run-id"})
        )

        with pytest.raises(RuntimeError, match="status unavailable"):
            operator.execute(context=context)

        assert operator.run_id == "prior-run-id"
        assert task_instance.xcom_values == [("run_id", "prior-run-id")]
        hook.run_pipeline.assert_not_called()

    def test_retry_replaces_missing_pipeline_run(self) -> None:
        operator = self.make_operator()
        hook = self.make_hook()
        hook.get_pipeline_run_status.side_effect = ResourceNotFoundError("run not found")
        operator.hook = hook
        task_state_store = FakeTaskStateStore({"azure_data_factory_run_id": "prior-run-id"})
        context, task_instance = self.make_context(task_state_store)

        operator.execute(context=context)

        hook.run_pipeline.assert_called_once()
        assert operator.run_id == "new-run-id"
        assert task_instance.xcom_values == [
            ("run_id", "prior-run-id"),
            ("run_id", "new-run-id"),
        ]
        assert task_state_store.set_calls == [("azure_data_factory_run_id", "new-run-id")]

    @pytest.mark.parametrize(
        ("status", "is_active", "is_succeeded"),
        [
            (AzureDataFactoryPipelineRunStatus.QUEUED, True, False),
            (AzureDataFactoryPipelineRunStatus.IN_PROGRESS, True, False),
            (AzureDataFactoryPipelineRunStatus.CANCELING, True, False),
            (AzureDataFactoryPipelineRunStatus.SUCCEEDED, False, True),
            (AzureDataFactoryPipelineRunStatus.FAILED, False, False),
            (AzureDataFactoryPipelineRunStatus.CANCELLED, False, False),
        ],
    )
    def test_pipeline_status_predicates(
        self,
        status: str,
        is_active: bool,
        is_succeeded: bool,
    ) -> None:
        operator = self.make_operator()

        assert operator.is_job_active(status) is is_active
        assert operator.is_job_succeeded(status) is is_succeeded

    def test_default_args_durable_reaches_operator(self) -> None:
        operator = self.make_operator(default_args={"durable": False})

        assert operator.durable is False

    def test_durable_false_submits_without_task_state(self) -> None:
        operator = self.make_operator(durable=False)
        hook = self.make_hook()
        operator.hook = hook
        task_state_store = FakeTaskStateStore({"azure_data_factory_run_id": "prior-run-id"})
        context, task_instance = self.make_context(task_state_store)

        operator.execute(context=context)

        hook.run_pipeline.assert_called_once()
        hook.get_pipeline_run_status.assert_not_called()
        assert task_state_store.get_calls == []
        assert task_state_store.set_calls == []
        assert task_instance.xcom_values == [("run_id", "new-run-id")]

    def test_wait_for_termination_false_preserves_submission_path(self) -> None:
        operator = self.make_operator(wait_for_termination=False)
        hook = self.make_hook()
        operator.hook = hook
        task_state_store = FakeTaskStateStore({"azure_data_factory_run_id": "prior-run-id"})
        context, task_instance = self.make_context(task_state_store)

        operator.execute(context=context)

        hook.run_pipeline.assert_called_once()
        hook.get_pipeline_run_status.assert_not_called()
        hook.wait_for_pipeline_run_status.assert_not_called()
        assert task_state_store.get_calls == []
        assert task_state_store.set_calls == []
        assert task_instance.xcom_values == [("run_id", "new-run-id")]

    def test_deferrable_preserves_submission_path(self) -> None:
        operator = self.make_operator(deferrable=True)
        hook = self.make_hook()
        hook.get_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.SUCCEEDED
        operator.hook = hook
        task_state_store = FakeTaskStateStore({"azure_data_factory_run_id": "prior-run-id"})
        context, task_instance = self.make_context(task_state_store)

        operator.execute(context=context)

        hook.run_pipeline.assert_called_once()
        assert task_state_store.get_calls == []
        assert task_state_store.set_calls == []
        assert task_instance.xcom_values == [("run_id", "new-run-id")]

    def test_on_kill_during_reconnect_status_lookup_cancels_prior_run(self) -> None:
        operator = self.make_operator()
        hook = self.make_hook()
        operator.hook = hook
        task_state_store = FakeTaskStateStore({"azure_data_factory_run_id": "prior-run-id"})
        context, task_instance = self.make_context(task_state_store)

        def cancel_during_status_lookup(**kwargs: Any) -> str:
            operator.on_kill()
            return AzureDataFactoryPipelineRunStatus.IN_PROGRESS

        hook.get_pipeline_run_status.side_effect = cancel_during_status_lookup
        operator.execute(context=context)

        hook.cancel_pipeline_run.assert_called_once_with(
            run_id="prior-run-id",
            resource_group_name="resource-group-name",
            factory_name="factory-name",
        )
        assert operator.run_id == "prior-run-id"
        assert task_instance.xcom_values == [("run_id", "prior-run-id")]


class TestWarnAndDisableDurableAirflowPre3_3:
    def test_no_warning_when_unset(self) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result: bool = data_factory_module._warn_and_disable_durable_pre_3_3(
                data_factory_module._DURABLE_UNSET
            )
        assert result is False
        assert caught == []

    @pytest.mark.parametrize("value", [True, False])
    def test_warns_and_disables_when_explicitly_set(self, value: bool) -> None:
        with pytest.warns(UserWarning, match="durable.*no effect"):
            result: bool = data_factory_module._warn_and_disable_durable_pre_3_3(value)
        assert result is False


@pytest.fixture
def create_task_instance(create_task_instance_of_operator, session):
    def _create_task_instance(operator_class, **kwargs):
        return functools.partial(
            create_task_instance_of_operator,
            session=session,
            operator_class=operator_class,
            dag_id="adhoc_airflow",
        )(**kwargs)

    return _create_task_instance


class TestAzureDataFactoryRunPipelineOperatorWithDeferrable:
    @pytest.fixture(autouse=True)
    def setup_operator(self, dag_maker, create_task_instance):
        """Fixture to set up the operator using create_task_instance."""
        self.ti = create_task_instance(
            operator_class=AzureDataFactoryRunPipelineOperator,
            task_id="run_pipeline",
            pipeline_name="pipeline",
            resource_group_name="resource-group-name",
            factory_name="factory-name",
            parameters={"myParam": "value"},
            deferrable=True,
        )
        self.task = dag_maker.dag.get_task(self.ti.task_id)

    def get_dag_run(self, dag_id: str = "test_dag_id", run_id: str = "test_dag_id") -> DagRun:
        if AIRFLOW_V_3_0_PLUS:
            dag_run = DagRun(
                dag_id=dag_id, run_type="manual", logical_date=timezone.datetime(2022, 1, 1), run_id=run_id
            )
        else:
            dag_run = DagRun(  # type: ignore[call-arg]
                dag_id=dag_id, run_type="manual", execution_date=timezone.datetime(2022, 1, 1), run_id=run_id
            )
        return dag_run

    def get_task_instance(self, task: BaseOperator) -> TaskInstance:
        if AIRFLOW_V_3_0_PLUS:
            return _create_task_instance(
                task,
                run_id=timezone.datetime(2022, 1, 1).isoformat(),
                dag_version_id=mock.MagicMock(),
            )
        return TaskInstance(task, timezone.datetime(2022, 1, 1))

    def get_conn(
        self,
    ) -> Connection:
        return Connection(
            conn_id="test_conn",
            extra={},
        )

    def create_context(self, task, dag=None):
        if dag is None:
            dag = DAG(dag_id="dag", schedule=None)
        tzinfo = pendulum.timezone("UTC")
        logical_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        if AIRFLOW_V_3_0_PLUS:
            dag_run = DagRun(
                dag_id=dag.dag_id,
                logical_date=logical_date,
                run_id=DagRun.generate_run_id(
                    run_type=DagRunType.MANUAL, logical_date=logical_date, run_after=logical_date
                ),
            )
        else:
            dag_run = DagRun(
                dag_id=dag.dag_id,
                execution_date=logical_date,
                run_id=DagRun.generate_run_id(DagRunType.MANUAL, logical_date),
            )
        if AIRFLOW_V_3_0_PLUS:
            task_instance = _create_task_instance(task=task, dag_version_id=mock.MagicMock())
        else:
            task_instance = TaskInstance(task=task)
        task_instance.dag_run = dag_run
        task_instance.xcom_push = mock.Mock()
        date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        return {
            "dag": dag,
            "ts": logical_date.isoformat(),
            "task": task,
            "ti": task_instance,
            "task_instance": task_instance,
            "run_id": dag_run.run_id,
            "dag_run": dag_run,
            "data_interval_end": logical_date,
            date_key: logical_date,
        }

    @pytest.mark.db_test
    @mock.patch(
        "airflow.providers.microsoft.azure.operators.data_factory.AzureDataFactoryRunPipelineOperator.defer"
    )
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.get_pipeline_run_status",
        return_value=AzureDataFactoryPipelineRunStatus.SUCCEEDED,
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.run_pipeline")
    def test_azure_data_factory_run_pipeline_operator_async_succeeded_before_deferred(
        self, mock_run_pipeline, mock_get_status, mock_defer
    ):
        class CreateRunResponse:
            pass

        CreateRunResponse.run_id = AZ_PIPELINE_RUN_ID
        mock_run_pipeline.return_value = CreateRunResponse

        self.task.execute(context=self.create_context(self.task))
        assert not mock_defer.called

    @pytest.mark.db_test
    @pytest.mark.parametrize("status", sorted(AzureDataFactoryPipelineRunStatus.FAILURE_STATES))
    @mock.patch(
        "airflow.providers.microsoft.azure.operators.data_factory.AzureDataFactoryRunPipelineOperator.defer"
    )
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.get_pipeline_run_status",
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.run_pipeline")
    def test_azure_data_factory_run_pipeline_operator_async_error_before_deferred(
        self, mock_run_pipeline, mock_get_status, mock_defer, status
    ):
        mock_get_status.return_value = status

        class CreateRunResponse:
            pass

        CreateRunResponse.run_id = AZ_PIPELINE_RUN_ID
        mock_run_pipeline.return_value = CreateRunResponse

        with pytest.raises(AzureDataFactoryPipelineRunException):
            self.task.execute(context=self.create_context(self.task))
        assert not mock_defer.called

    @pytest.mark.db_test
    @pytest.mark.parametrize("status", sorted(AzureDataFactoryPipelineRunStatus.INTERMEDIATE_STATES))
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.get_pipeline_run_status",
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.run_pipeline")
    def test_azure_data_factory_run_pipeline_operator_async(self, mock_run_pipeline, mock_get_status, status):
        """Assert that AzureDataFactoryRunPipelineOperator(..., deferrable=True) deferred"""

        class CreateRunResponse:
            pass

        CreateRunResponse.run_id = AZ_PIPELINE_RUN_ID
        mock_run_pipeline.return_value = CreateRunResponse

        with pytest.raises(TaskDeferred) as exc:
            self.task.execute(context=self.create_context(self.task))

        assert isinstance(exc.value.trigger, AzureDataFactoryTrigger), (
            "Trigger is not a AzureDataFactoryTrigger"
        )

    @pytest.mark.db_test
    def test_azure_data_factory_run_pipeline_operator_async_execute_complete_success(self):
        """Assert that execute_complete log success message"""
        with mock.patch.object(self.task.log, "info") as mock_log_info:
            self.task.execute_complete(
                context={},
                event={"status": "success", "message": "success", "run_id": AZ_PIPELINE_RUN_ID},
            )
        mock_log_info.assert_called_with("success")

    @pytest.mark.db_test
    def test_azure_data_factory_run_pipeline_operator_async_execute_complete_fail(self):
        """Assert that execute_complete raise exception on error"""
        with pytest.raises(AirflowException):
            self.task.execute_complete(
                context={},
                event={"status": "error", "message": "error", "run_id": AZ_PIPELINE_RUN_ID},
            )
