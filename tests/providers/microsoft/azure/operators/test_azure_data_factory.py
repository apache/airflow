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

import json
from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.utils import db, timezone

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


class TestAzureDataFactoryRunPipelineOperator:
    def setup_method(self):
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

        db.merge_conn(
            Connection(
                conn_id="azure_data_factory_test",
                conn_type="azure_data_factory",
                login="client-id",
                password="client-secret",
                extra=json.dumps(CONN_EXTRAS),
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
        "pipeline_run_status,expected_output",
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
                    run_id=mock_run_pipeline.return_value.run_id,
                    factory_name=self.config["factory_name"],
                    resource_group_name=self.config["resource_group_name"],
                )
            else:
                # When the pipeline run status is not in a terminal status or "Succeeded", the operator will
                # continue to call ``get_pipeline_run()`` until a ``timeout`` number of seconds has passed
                # (3 seconds for this test).  Therefore, there should be 4 calls of this function: one
                # initially and 3 for each check done at a 1 second interval.
                assert mock_get_pipeline_run.call_count == 4

                mock_get_pipeline_run.assert_called_with(
                    run_id=mock_run_pipeline.return_value.run_id,
                    factory_name=self.config["factory_name"],
                    resource_group_name=self.config["resource_group_name"],
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

    @pytest.mark.parametrize(
        "resource_group,factory",
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
    def test_run_pipeline_operator_link(self, resource_group, factory, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AzureDataFactoryRunPipelineOperator,
            dag_id="test_adf_run_pipeline_op_link",
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            azure_data_factory_conn_id=AZURE_DATA_FACTORY_CONN_ID,
            pipeline_name=PIPELINE_NAME,
            resource_group_name=resource_group,
            factory_name=factory,
        )
        ti.xcom_push(key="run_id", value=PIPELINE_RUN_RESPONSE["run_id"])

        url = ti.task.get_extra_links(ti, "Monitor Pipeline Run")
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
                resource_group_name=resource_group if resource_group else conn_resource_group_name,
                factory_name=factory if factory else conn_factory_name,
            )
        )
