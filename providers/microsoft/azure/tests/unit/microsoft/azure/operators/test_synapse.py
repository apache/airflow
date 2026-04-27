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
import time
from unittest import mock
from unittest.mock import MagicMock, patch

import pendulum
import pytest

from airflow.models import DAG, Connection
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.microsoft.azure.hooks.synapse import (
    AzureSynapsePipelineHook,
    AzureSynapsePipelineRunException,
    AzureSynapsePipelineRunStatus,
)
from airflow.providers.microsoft.azure.operators.synapse import (
    AzureSynapsePipelineRunLink,
    AzureSynapseRunPipelineOperator,
    AzureSynapseRunSparkBatchOperator,
)
from airflow.providers.microsoft.azure.triggers.synapse import AzureSynapsePipelineTrigger
from airflow.utils import timezone
from airflow.utils.types import DagRunType

from tests_common.test_utils.taskinstance import create_task_instance as _create_task_instance
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
SUBSCRIPTION_ID = "subscription_id"
TENANT_ID = "tenant_id"
TASK_ID = "run_spark_op"
AZURE_SYNAPSE_PIPELINE_TASK_ID = "run_pipeline_op"
AZURE_SYNAPSE_CONN_ID = "azure_synapse_test"
CONN_EXTRAS = {
    "synapse__subscriptionId": SUBSCRIPTION_ID,
    "synapse__tenantId": "my-tenant-id",
    "synapse__spark_pool": "my-spark-pool",
}
SYNAPSE_PIPELINE_CONN_EXTRAS = {"tenantId": TENANT_ID}
JOB_RUN_RESPONSE = {"id": 123}
PIPELINE_NAME = "Pipeline 1"
AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT = "azure_synapse_workspace_dev_endpoint"
RESOURCE_GROUP = "op-resource-group"
WORKSPACE_NAME = "workspace-test"
AZURE_SYNAPSE_WORKSPACE_URL = f"https://web.azuresynapse.net?workspace=%2fsubscriptions%{SUBSCRIPTION_ID}%2fresourceGroups%2f{RESOURCE_GROUP}%2fproviders%2fMicrosoft.Synapse%2fworkspaces%2f{WORKSPACE_NAME}"
PIPELINE_RUN_RESPONSE = {"run_id": "run_id"}


class TestAzureSynapseRunSparkBatchOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connection):
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}
        self.config = {
            "task_id": TASK_ID,
            "azure_synapse_conn_id": AZURE_SYNAPSE_CONN_ID,
            "payload": {},
            "check_interval": 1,
            "timeout": 3,
        }

        create_mock_connection(
            Connection(
                conn_id=AZURE_SYNAPSE_CONN_ID,
                conn_type="azure_synapse",
                host="https://synapsetest.net",
                login="client-id",
                password="client-secret",
                extra=CONN_EXTRAS,
            )
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.get_job_run_status")
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.run_spark_job")
    def test_azure_synapse_run_spark_batch_operator_success(
        self, mock_run_spark_job, mock_get_job_run_status
    ):
        mock_get_job_run_status.return_value = "success"
        mock_run_spark_job.return_value = MagicMock(**JOB_RUN_RESPONSE)
        op = AzureSynapseRunSparkBatchOperator(
            task_id="test", azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID, spark_pool="test_pool", payload={}
        )
        op.execute(context=self.mock_context)
        assert op.job_id == JOB_RUN_RESPONSE["id"]

    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.get_job_run_status")
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.run_spark_job")
    def test_azure_synapse_run_spark_batch_operator_error(self, mock_run_spark_job, mock_get_job_run_status):
        mock_get_job_run_status.return_value = "error"
        mock_run_spark_job.return_value = MagicMock(**JOB_RUN_RESPONSE)
        op = AzureSynapseRunSparkBatchOperator(
            task_id="test", azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID, spark_pool="test_pool", payload={}
        )
        with pytest.raises(
            AirflowException,
            match=f"Job run {JOB_RUN_RESPONSE['id']} has failed or has been cancelled.",
        ):
            op.execute(context=self.mock_context)

    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.get_job_run_status")
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.run_spark_job")
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapseHook.cancel_job_run")
    def test_azure_synapse_run_spark_batch_operator_on_kill(
        self, mock_cancel_job_run, mock_run_spark_job, mock_get_job_run_status
    ):
        mock_get_job_run_status.return_value = "success"
        mock_run_spark_job.return_value = MagicMock(**JOB_RUN_RESPONSE)
        op = AzureSynapseRunSparkBatchOperator(
            task_id="test", azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID, spark_pool="test_pool", payload={}
        )
        op.execute(context=self.mock_context)
        op.on_kill()
        mock_cancel_job_run.assert_called_once_with(job_id=JOB_RUN_RESPONSE["id"])


class TestAzureSynapseRunPipelineOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connection):
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}
        self.config = {
            "task_id": AZURE_SYNAPSE_PIPELINE_TASK_ID,
            "azure_synapse_conn_id": AZURE_SYNAPSE_CONN_ID,
            "pipeline_name": PIPELINE_NAME,
            "azure_synapse_workspace_dev_endpoint": AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT,
            "check_interval": 1,
            "timeout": 3,
        }

        create_mock_connection(
            Connection(
                conn_id=AZURE_SYNAPSE_CONN_ID,
                conn_type="azure_synapse_pipeline",
                host=AZURE_SYNAPSE_WORKSPACE_URL,
                login="client_id",
                password="client_secret",
                extra=SYNAPSE_PIPELINE_CONN_EXTRAS,
            )
        )

    @staticmethod
    def create_pipeline_run(status: str):
        """Helper function to create a mock pipeline run with a given execution status."""

        run = MagicMock()
        run.status = status

        return run

    @patch.object(AzureSynapsePipelineHook, "run_pipeline", return_value=MagicMock(**PIPELINE_RUN_RESPONSE))
    @pytest.mark.parametrize(
        ("pipeline_run_status", "expected_output"),
        [
            (AzureSynapsePipelineRunStatus.SUCCEEDED, None),
            (AzureSynapsePipelineRunStatus.FAILED, "exception"),
            (AzureSynapsePipelineRunStatus.CANCELLED, "exception"),
            (AzureSynapsePipelineRunStatus.IN_PROGRESS, "timeout"),
            (AzureSynapsePipelineRunStatus.QUEUED, "timeout"),
            (AzureSynapsePipelineRunStatus.CANCELING, "timeout"),
        ],
    )
    def test_execute_wait_for_termination(self, mock_run_pipeline, pipeline_run_status, expected_output):
        # Initialize the operator with mock config, (**) unpacks the config dict.
        operator = AzureSynapseRunPipelineOperator(**self.config)

        assert operator.azure_synapse_conn_id == self.config["azure_synapse_conn_id"]
        assert operator.pipeline_name == self.config["pipeline_name"]
        assert (
            operator.azure_synapse_workspace_dev_endpoint
            == self.config["azure_synapse_workspace_dev_endpoint"]
        )
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert operator.wait_for_termination

        with patch.object(AzureSynapsePipelineHook, "get_pipeline_run") as mock_get_pipeline_run:
            mock_get_pipeline_run.return_value = TestAzureSynapseRunPipelineOperator.create_pipeline_run(
                pipeline_run_status
            )

            if not expected_output:
                # A successful operator execution should not return any values.
                assert not operator.execute(context=self.mock_context)
            elif expected_output == "exception":
                # The operator should fail if the pipeline run fails or is canceled.
                with pytest.raises(
                    AzureSynapsePipelineRunException,
                    match=f"Pipeline run {PIPELINE_RUN_RESPONSE['run_id']} has failed or has been cancelled.",
                ):
                    operator.execute(context=self.mock_context)
            else:
                # Demonstrating the operator timing out after surpassing the configured timeout value.
                with pytest.raises(
                    AzureSynapsePipelineRunException,
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

            # Check if mock_run_pipeline called with particular set of arguments.
            mock_run_pipeline.assert_called_once_with(
                pipeline_name=self.config["pipeline_name"],
                reference_pipeline_run_id=None,
                is_recovery=None,
                start_activity_name=None,
                parameters=None,
            )

            if pipeline_run_status in AzureSynapsePipelineRunStatus.TERMINAL_STATUSES:
                mock_get_pipeline_run.assert_called_once_with(run_id=mock_run_pipeline.return_value.run_id)
            else:
                # When the pipeline run status is not in a terminal status or "Succeeded", the operator will
                # continue to call ``get_pipeline_run()`` until a ``timeout`` number of seconds has passed
                # (3 seconds for this test).  Therefore, there should be 4 calls of this function: one
                # initially and 3 for each check done at a 1 second interval.
                assert mock_get_pipeline_run.call_count == 4

                mock_get_pipeline_run.assert_called_with(run_id=mock_run_pipeline.return_value.run_id)

    @patch.object(AzureSynapsePipelineHook, "run_pipeline", return_value=MagicMock(**PIPELINE_RUN_RESPONSE))
    def test_execute_no_wait_for_termination(self, mock_run_pipeline):
        operator = AzureSynapseRunPipelineOperator(wait_for_termination=False, **self.config)

        assert operator.azure_synapse_conn_id == self.config["azure_synapse_conn_id"]
        assert operator.pipeline_name == self.config["pipeline_name"]
        assert (
            operator.azure_synapse_workspace_dev_endpoint
            == self.config["azure_synapse_workspace_dev_endpoint"]
        )
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert not operator.wait_for_termination

        with patch.object(
            AzureSynapsePipelineHook, "get_pipeline_run", autospec=True
        ) as mock_get_pipeline_run:
            operator.execute(context=self.mock_context)

            # Check the ``run_id`` attr is assigned after executing the pipeline.
            assert operator.run_id == PIPELINE_RUN_RESPONSE["run_id"]

            # Check to ensure an `XCom` is pushed regardless of pipeline run result.
            self.mock_ti.xcom_push.assert_called_once_with(
                key="run_id", value=PIPELINE_RUN_RESPONSE["run_id"]
            )

            mock_run_pipeline.assert_called_once_with(
                pipeline_name=self.config["pipeline_name"],
                reference_pipeline_run_id=None,
                is_recovery=None,
                start_activity_name=None,
                parameters=None,
            )

            # Checking the pipeline run status should _not_ be called when ``wait_for_termination`` is False.
            mock_get_pipeline_run.assert_not_called()

    @pytest.mark.db_test
    def test_run_pipeline_operator_link(
        self,
        dag_maker,
        create_task_instance_of_operator,
        mock_supervisor_comms,
    ):
        ti = create_task_instance_of_operator(
            AzureSynapseRunPipelineOperator,
            dag_id="test_synapse_run_pipeline_op_link",
            task_id=AZURE_SYNAPSE_PIPELINE_TASK_ID,
            azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID,
            pipeline_name=PIPELINE_NAME,
            azure_synapse_workspace_dev_endpoint=AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT,
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
            "https://ms.web.azuresynapse.net/en/monitoring/pipelineruns/{run_id}"
            "?workspace=%2Fsubscriptions%2F{subscription_id}%2F"
            "resourceGroups%2F{resource_group}%2Fproviders%2FMicrosoft.Synapse"
            "%2Fworkspaces%2F{workspace_name}"
        )

        conn = AzureSynapsePipelineHook.get_connection(AZURE_SYNAPSE_CONN_ID)
        conn_synapse_workspace_url = conn.host

        # Extract the workspace_name, subscription_id and resource_group from the Synapse workspace url.
        pipeline_run_object = AzureSynapsePipelineRunLink()
        fields = pipeline_run_object.get_fields_from_url(workspace_url=conn_synapse_workspace_url)

        assert url == (
            EXPECTED_PIPELINE_RUN_OP_EXTRA_LINK.format(
                run_id=PIPELINE_RUN_RESPONSE["run_id"],
                subscription_id=fields["subscription_id"],
                resource_group=fields["resource_group"],
                workspace_name=fields["workspace_name"],
            )
        )

    def test_pipeline_operator_link_invalid_uri_pattern(self):
        with pytest.raises(ValueError, match="Invalid workspace URL format"):
            AzureSynapsePipelineRunLink().get_fields_from_url(workspace_url="https://example.org/")

    def test_pipeline_operator_link_invalid_uri_workspace_segments(self):
        workspace_url = "https://web.azuresynapse.net?workspace=%2Fsubscriptions%2Fspam-egg"
        with pytest.raises(ValueError, match="Workspace expected at least 5 segments"):
            AzureSynapsePipelineRunLink().get_fields_from_url(workspace_url=workspace_url)


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


class TestAzureSynapseRunPipelineOperatorWithDeferrable:
    @pytest.fixture(autouse=True)
    def setup_operator(self, dag_maker, create_task_instance):
        """Fixture to set up the operator using create_task_instance."""
        self.ti = create_task_instance(
            operator_class=AzureSynapseRunPipelineOperator,
            task_id="run_pipeline",
            pipeline_name=PIPELINE_NAME,
            azure_synapse_workspace_dev_endpoint=AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT,
            azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID,
            deferrable=True,
        )
        self.task = dag_maker.dag.get_task(self.ti.task_id)

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
                    run_type=DagRunType.MANUAL,
                    logical_date=logical_date,
                    run_after=logical_date,
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
    @mock.patch("airflow.providers.microsoft.azure.operators.synapse.AzureSynapseRunPipelineOperator.defer")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.synapse.AzureSynapsePipelineHook.get_pipeline_run_status",
        return_value=AzureSynapsePipelineRunStatus.SUCCEEDED,
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapsePipelineHook.run_pipeline")
    def test_synapse_run_pipeline_operator_async_succeeded_before_deferred(
        self, mock_run_pipeline, mock_get_status, mock_defer
    ):
        class CreateRunResponse:
            pass

        CreateRunResponse.run_id = PIPELINE_RUN_RESPONSE["run_id"]
        mock_run_pipeline.return_value = CreateRunResponse

        self.task.execute(context=self.create_context(self.task))
        assert not mock_defer.called

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "status",
        sorted(AzureSynapsePipelineRunStatus.FAILURE_STATES),
    )
    @mock.patch("airflow.providers.microsoft.azure.operators.synapse.AzureSynapseRunPipelineOperator.defer")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.synapse.AzureSynapsePipelineHook.get_pipeline_run_status"
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapsePipelineHook.run_pipeline")
    def test_synapse_run_pipeline_operator_async_error_before_deferred(
        self, mock_run_pipeline, mock_get_status, mock_defer, status
    ):
        mock_get_status.return_value = status

        class CreateRunResponse:
            pass

        CreateRunResponse.run_id = PIPELINE_RUN_RESPONSE["run_id"]
        mock_run_pipeline.return_value = CreateRunResponse

        with pytest.raises(AzureSynapsePipelineRunException):
            self.task.execute(context=self.create_context(self.task))

        assert not mock_defer.called

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "status",
        sorted(AzureSynapsePipelineRunStatus.INTERMEDIATE_STATES),
    )
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.synapse.AzureSynapsePipelineHook.get_pipeline_run_status"
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.synapse.AzureSynapsePipelineHook.run_pipeline")
    def test_synapse_run_pipeline_operator_async(self, mock_run_pipeline, mock_get_status, status):
        """Assert that AzureSynapseRunPipelineOperator(..., deferrable=True) deferred."""

        mock_get_status.return_value = status

        class CreateRunResponse:
            pass

        CreateRunResponse.run_id = PIPELINE_RUN_RESPONSE["run_id"]
        mock_run_pipeline.return_value = CreateRunResponse

        with pytest.raises(TaskDeferred) as exc:
            self.task.execute(context=self.create_context(self.task))

        trigger = exc.value.trigger

        assert isinstance(trigger, AzureSynapsePipelineTrigger)

        assert trigger.run_id == PIPELINE_RUN_RESPONSE["run_id"]
        assert trigger.check_interval == self.task.check_interval

    @pytest.mark.db_test
    def test_synapse_run_pipeline_operator_async_execute_complete_success(self):
        """Assert that execute_complete logs success message."""
        with mock.patch.object(self.task.log, "info") as mock_log_info:
            self.task.execute_complete(
                context={},
                event={
                    "status": "success",
                    "message": "success",
                    "run_id": PIPELINE_RUN_RESPONSE["run_id"],
                },
            )

        mock_log_info.assert_called_with("success")

    @pytest.mark.db_test
    def test_synapse_run_pipeline_operator_async_execute_complete_fail(self):
        """Assert that execute_complete raises exception on error."""
        with pytest.raises(AzureSynapsePipelineRunException):
            self.task.execute_complete(
                context={},
                event={
                    "status": "error",
                    "message": "error",
                    "run_id": PIPELINE_RUN_RESPONSE["run_id"],
                },
            )

    @pytest.mark.db_test
    def test_execute_complete_no_event(self):
        with pytest.raises(AzureSynapsePipelineRunException, match="no event"):
            self.task.execute_complete(context={}, event=None)

    @pytest.mark.db_test
    def test_execute_complete_unexpected_event(self):
        with pytest.raises(AzureSynapsePipelineRunException, match="Unexpected"):
            self.task.execute_complete(
                context={},
                event={"status": "unknown", "message": "??"},
            )

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "pipeline_state",
        [
            AzureSynapsePipelineRunStatus.FAILED,
            AzureSynapsePipelineRunStatus.CANCELLED,
        ],
    )
    def test_failure_states_roundtrip(self, pipeline_state):
        trigger = AzureSynapsePipelineTrigger(
            run_id=PIPELINE_RUN_RESPONSE["run_id"],
            azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID,
            azure_synapse_workspace_dev_endpoint=AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT,
            end_time=time.time() + 100,
            check_interval=1,
        )

        event = trigger._build_trigger_event(pipeline_state)

        assert event is not None

        payload = event.payload

        assert payload["status"] == "error"

        with pytest.raises(AzureSynapsePipelineRunException) as exc:
            self.task.execute_complete(context={}, event=payload)

        assert f"state {pipeline_state}" in str(exc.value)
