#
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

from datetime import timedelta
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from botocore.waiter import Waiter
from jinja2 import StrictUndefined

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.triggers.emr import EmrCreateJobFlowTrigger
from airflow.providers.amazon.aws.utils.waiter import WAITER_POLICY_NAME_MAPPING, WaitPolicy
from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.taskinstance import create_task_instance, render_template_fields
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS
from unit.amazon.aws.utils.test_template_fields import validate_template_fields
from unit.amazon.aws.utils.test_waiter import assert_expected_waiter_type

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

TASK_ID = "test_task"

TEST_DAG_ID = "test_dag_id"

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

JOB_FLOW_ID = "j-8989898989"
RUN_JOB_FLOW_SUCCESS_RETURN = {"ResponseMetadata": {"HTTPStatusCode": 200}, "JobFlowId": JOB_FLOW_ID}

TEMPLATE_SEARCHPATH = Path(__file__).parents[1].joinpath("config_templates").as_posix()


@pytest.fixture
def mocked_hook_client():
    with patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn") as m:
        yield m


class TestEmrCreateJobFlowOperator:
    # When
    _config = {
        "Name": "test_job_flow",
        "ReleaseLabel": "5.11.0",
        "Steps": [
            {
                "Name": "test_step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": ["/usr/lib/spark/bin/run-example", "{{ macros.ds_add(ds, -1) }}", "{{ ds }}"],
                },
            }
        ],
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.operator = EmrCreateJobFlowOperator(
            task_id=TASK_ID,
            aws_conn_id="aws_default",
            emr_conn_id="emr_default",
            region_name="ap-southeast-2",
            dag=DAG(
                TEST_DAG_ID,
                schedule=None,
                default_args=args,
                template_searchpath=TEMPLATE_SEARCHPATH,
                template_undefined=StrictUndefined,
            ),
        )
        self.mock_context = MagicMock()

    def test_init(self):
        assert self.operator.aws_conn_id == "aws_default"
        assert self.operator.emr_conn_id == "emr_default"
        assert self.operator.region_name == "ap-southeast-2"

    @pytest.mark.db_test
    def test_render_template(self, session, clean_dags_dagruns_and_dagbundles, testing_dag_bundle):
        self.operator.job_flow_overrides = self._config
        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion

            sync_dag_to_db(self.operator.dag)
            dag_version = DagVersion.get_latest_version(self.operator.dag.dag_id)
            ti = create_task_instance(task=self.operator, dag_version_id=dag_version.id)
            dag_run = DagRun(
                dag_id=self.operator.dag_id,
                logical_date=DEFAULT_DATE,
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
                run_after=timezone.utcnow(),
            )
        else:
            dag_run = DagRun(
                dag_id=self.operator.dag_id,
                execution_date=DEFAULT_DATE,
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
            )
            ti = TaskInstance(task=self.operator)
        ti.dag_run = dag_run
        render_template_fields(ti, self.operator)

        expected_args = {
            "Name": "test_job_flow",
            "ReleaseLabel": "5.11.0",
            "Steps": [
                {
                    "Name": "test_step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "/usr/lib/spark/bin/run-example",
                            (DEFAULT_DATE - timedelta(days=1)).strftime("%Y-%m-%d"),
                            DEFAULT_DATE.strftime("%Y-%m-%d"),
                        ],
                    },
                }
            ],
        }

        assert self.operator.job_flow_overrides == expected_args

    @pytest.mark.db_test
    def test_render_template_from_file(
        self, mocked_hook_client, session, clean_dags_dagruns_and_dagbundles, testing_dag_bundle
    ):
        self.operator.job_flow_overrides = "job.j2.json"
        self.operator.params = {"releaseLabel": "5.11.0"}

        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion

            sync_dag_to_db(self.operator.dag)
            dag_version = DagVersion.get_latest_version(self.operator.dag.dag_id)
            ti = create_task_instance(task=self.operator, dag_version_id=dag_version.id)
            dag_run = DagRun(
                dag_id=self.operator.dag_id,
                logical_date=DEFAULT_DATE,
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
                run_after=timezone.utcnow(),
            )
        else:
            dag_run = DagRun(
                dag_id=self.operator.dag_id,
                execution_date=DEFAULT_DATE,
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
            )
            ti = TaskInstance(task=self.operator)
        ti.dag_run = dag_run
        render_template_fields(ti, self.operator)

        mocked_hook_client.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN

        # String in job_flow_overrides (i.e. from loaded as a file) is not "parsed" until inside execute()
        self.operator.execute(self.mock_context)

        expected_args = {
            "Name": "test_job_flow",
            "ReleaseLabel": "5.11.0",
            "Steps": [
                {
                    "Name": "test_step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "/usr/lib/spark/bin/run-example",
                            "2016-12-31",
                            "2017-01-01",
                        ],
                    },
                }
            ],
        }

        assert self.operator.job_flow_overrides == expected_args

    def test_execute_returns_job_id(self, mocked_hook_client):
        mocked_hook_client.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN
        assert self.operator.execute(self.mock_context) == JOB_FLOW_ID

    @mock.patch("botocore.waiter.get_service_module_name", return_value="emr")
    @mock.patch.object(Waiter, "wait")
    def test_execute_with_wait_for_completion(self, mock_waiter, _, mocked_hook_client):
        mocked_hook_client.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN

        self.operator.wait_for_completion = True

        assert self.operator.execute(self.mock_context) == JOB_FLOW_ID
        mock_waiter.assert_called_once_with(mock.ANY, ClusterId=JOB_FLOW_ID, WaiterConfig=mock.ANY)
        assert_expected_waiter_type(mock_waiter, WAITER_POLICY_NAME_MAPPING[WaitPolicy.WAIT_FOR_COMPLETION])

    def test_create_job_flow_deferrable(self, mocked_hook_client):
        """
        Test to make sure that the operator raises a TaskDeferred exception
        if run in deferrable mode and wait_for_completion is set.
        """
        mocked_hook_client.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN

        self.operator.deferrable = True
        self.operator.wait_for_completion = True
        with pytest.raises(TaskDeferred) as exc:
            self.operator.execute(self.mock_context)

        assert isinstance(exc.value.trigger, EmrCreateJobFlowTrigger), (
            "Trigger is not a EmrCreateJobFlowTrigger"
        )

    def test_create_job_flow_deferrable_no_wait(self, mocked_hook_client):
        """
        Test to make sure that the operator does NOT raise a TaskDeferred exception
        if run in deferrable mode but wait_for_completion is not set.
        """
        mocked_hook_client.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN

        self.operator.deferrable = True
        # wait_for_completion is None by default
        result = self.operator.execute(self.mock_context)
        assert result == JOB_FLOW_ID

    def test_template_fields(self):
        validate_template_fields(self.operator)

    def test_wait_policy_deprecation_warning(self):
        """Test that using wait_policy raises a deprecation warning."""
        with pytest.warns(AirflowProviderDeprecationWarning, match="`wait_policy` parameter is deprecated"):
            EmrCreateJobFlowOperator(
                task_id=TASK_ID,
                wait_policy=WaitPolicy.WAIT_FOR_COMPLETION,
            )
