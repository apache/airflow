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
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock, patch

import pendulum
import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG, Connection
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.triggers.data_factory import AzureDataFactoryTrigger
from airflow.utils import timezone
from airflow.utils.types import DagRunType

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

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
                with pytest.raises(
                    AzureDataFactoryPipelineRunException,
                    match=(
                        f"Pipeline run {PIPELINE_RUN_RESPONSE['run_id']} has not reached a terminal status "
                        f"after {self.config['timeout']} seconds."
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
                self.config["pipeline_name"],
                self.config["resource_group_name"],
                self.config["factory_name"],
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
                self.config["pipeline_name"],
                self.config["resource_group_name"],
                self.config["factory_name"],
                reference_pipeline_run_id=None,
                is_recovery=None,
                start_activity_name=None,
                start_from_failure=None,
                parameters=None,
            )

            # Checking the pipeline run status should _not_ be called when ``wait_for_termination`` is False.
            mock_get_pipeline_run.assert_not_called()

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
        self, resource_group, factory, create_task_instance_of_operator, mock_supervisor_comms
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

        url = ti.task.operator_extra_links[0].get_link(operator=ti.task, ti_key=ti.key)
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
    def setup_operator(self, create_task_instance):
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
            return TaskInstance(task, run_id=timezone.datetime(2022, 1, 1), dag_version_id=mock.MagicMock())
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
            task_instance = TaskInstance(task=task, dag_version_id=mock.MagicMock())
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

        self.ti.task.execute(context=self.create_context(self.ti.task))
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
            self.ti.task.execute(context=self.create_context(self.ti.task))
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
            self.ti.task.execute(context=self.create_context(self.ti.task))

        assert isinstance(exc.value.trigger, AzureDataFactoryTrigger), (
            "Trigger is not a AzureDataFactoryTrigger"
        )

    @pytest.mark.db_test
    def test_azure_data_factory_run_pipeline_operator_async_execute_complete_success(self):
        """Assert that execute_complete log success message"""

        with mock.patch.object(self.ti.task.log, "info") as mock_log_info:
            self.ti.task.execute_complete(
                context={},
                event={"status": "success", "message": "success", "run_id": AZ_PIPELINE_RUN_ID},
            )
        mock_log_info.assert_called_with("success")

    @pytest.mark.db_test
    def test_azure_data_factory_run_pipeline_operator_async_execute_complete_fail(self):
        """Assert that execute_complete raise exception on error"""

        with pytest.raises(AirflowException):
            self.ti.task.execute_complete(
                context={},
                event={"status": "error", "message": "error", "run_id": AZ_PIPELINE_RUN_ID},
            )
