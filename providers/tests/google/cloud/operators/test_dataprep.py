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

from unittest import mock

import pytest

from airflow.providers.google.cloud.operators.dataprep import (
    DataprepCopyFlowOperator,
    DataprepDeleteFlowOperator,
    DataprepGetJobGroupOperator,
    DataprepGetJobsForJobGroupOperator,
    DataprepRunFlowOperator,
    DataprepRunJobGroupOperator,
)

GCP_PROJECT_ID = "test-project-id"
DATAPREP_CONN_ID = "dataprep_default"
JOB_ID = 143
FLOW_ID = 128754
NEW_FLOW_ID = 1312
TASK_ID = "dataprep_job"
INCLUDE_DELETED = False
EMBED = ""
DATAPREP_JOB_RECIPE_ID = 1234567
PATH_TO_OUTOUT_FILE = "path_to_output_file"
DATA = {
    "wrangledDataset": {"id": DATAPREP_JOB_RECIPE_ID},
    "overrides": {
        "execution": "dataflow",
        "profiler": False,
        "writesettings": [
            {
                "path": PATH_TO_OUTOUT_FILE,
                "action": "create",
                "format": "csv",
                "compression": "none",
                "header": False,
                "asSingleFile": False,
            }
        ],
    },
}


class TestDataprepGetJobsForJobGroupOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute(self, hook_mock):
        op = DataprepGetJobsForJobGroupOperator(
            dataprep_conn_id=DATAPREP_CONN_ID, job_group_id=JOB_ID, task_id=TASK_ID
        )
        op.execute(context={})
        hook_mock.assert_called_once_with(dataprep_conn_id=DATAPREP_CONN_ID)
        hook_mock.return_value.get_jobs_for_job_group.assert_called_once_with(
            job_id=JOB_ID
        )


class TestDataprepGetJobGroupOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute(self, hook_mock):
        op = DataprepGetJobGroupOperator(
            dataprep_conn_id=DATAPREP_CONN_ID,
            project_id=None,
            job_group_id=JOB_ID,
            embed=EMBED,
            include_deleted=INCLUDE_DELETED,
            task_id=TASK_ID,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(dataprep_conn_id="dataprep_default")
        hook_mock.return_value.get_job_group.assert_called_once_with(
            job_group_id=JOB_ID, embed=EMBED, include_deleted=INCLUDE_DELETED
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.DataprepJobGroupLink")
    @pytest.mark.parametrize(
        "provide_project_id, expected_call_count",
        [
            (True, 1),
            (False, 0),
        ],
    )
    def test_execute_with_project_id_will_persist_link_to_job_group(
        self,
        link_mock,
        _,
        provide_project_id,
        expected_call_count,
    ):
        context = mock.MagicMock()
        project_id = GCP_PROJECT_ID if provide_project_id else None

        op = DataprepGetJobGroupOperator(
            task_id=TASK_ID,
            project_id=project_id,
            dataprep_conn_id=DATAPREP_CONN_ID,
            job_group_id=JOB_ID,
            embed=EMBED,
            include_deleted=INCLUDE_DELETED,
        )
        op.execute(context=context)

        assert link_mock.persist.call_count == expected_call_count
        if provide_project_id:
            link_mock.persist.assert_called_with(
                context=context,
                task_instance=op,
                project_id=project_id,
                job_group_id=JOB_ID,
            )


class TestDataprepRunJobGroupOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute(self, hook_mock):
        op = DataprepRunJobGroupOperator(
            dataprep_conn_id=DATAPREP_CONN_ID,
            body_request=DATA,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(dataprep_conn_id="dataprep_default")
        hook_mock.return_value.run_job_group.assert_called_once_with(body_request=DATA)


class TestDataprepCopyFlowOperatorTest:
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute_with_default_params(self, hook_mock):
        op = DataprepCopyFlowOperator(
            task_id=TASK_ID,
            dataprep_conn_id=DATAPREP_CONN_ID,
            flow_id=FLOW_ID,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(dataprep_conn_id="dataprep_default")
        hook_mock.return_value.copy_flow.assert_called_once_with(
            flow_id=FLOW_ID,
            name="",
            description="",
            copy_datasources=False,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute_with_specified_params(self, hook_mock):
        op = DataprepCopyFlowOperator(
            task_id=TASK_ID,
            dataprep_conn_id=DATAPREP_CONN_ID,
            flow_id=FLOW_ID,
            name="specified name",
            description="specified description",
            copy_datasources=True,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(dataprep_conn_id="dataprep_default")
        hook_mock.return_value.copy_flow.assert_called_once_with(
            flow_id=FLOW_ID,
            name="specified name",
            description="specified description",
            copy_datasources=True,
        )

    @pytest.mark.db_test
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute_with_templated_params(
        self, _, create_task_instance_of_operator, session
    ):
        dag_id = "test_execute_with_templated_params"
        ti = create_task_instance_of_operator(
            DataprepCopyFlowOperator,
            dag_id=dag_id,
            project_id="{{ dag.dag_id }}",
            task_id=TASK_ID,
            flow_id="{{ dag.dag_id }}",
            name="{{ dag.dag_id }}",
            description="{{ dag.dag_id }}",
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        assert dag_id == ti.task.project_id
        assert dag_id == ti.task.flow_id
        assert dag_id == ti.task.name
        assert dag_id == ti.task.description

    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.DataprepFlowLink")
    @pytest.mark.parametrize(
        "provide_project_id, expected_call_count",
        [
            (True, 1),
            (False, 0),
        ],
    )
    def test_execute_with_project_id_will_persist_link_to_flow(
        self,
        link_mock,
        hook_mock,
        provide_project_id,
        expected_call_count,
    ):
        hook_mock.return_value.copy_flow.return_value = {"id": NEW_FLOW_ID}
        context = mock.MagicMock()
        project_id = GCP_PROJECT_ID if provide_project_id else None

        op = DataprepCopyFlowOperator(
            task_id=TASK_ID,
            project_id=project_id,
            dataprep_conn_id=DATAPREP_CONN_ID,
            flow_id=FLOW_ID,
            name="specified name",
            description="specified description",
            copy_datasources=True,
        )
        op.execute(context=context)

        assert link_mock.persist.call_count == expected_call_count
        if provide_project_id:
            link_mock.persist.assert_called_with(
                context=context,
                task_instance=op,
                project_id=project_id,
                flow_id=NEW_FLOW_ID,
            )


class TestDataprepDeleteFlowOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute(self, hook_mock):
        op = DataprepDeleteFlowOperator(
            task_id=TASK_ID,
            dataprep_conn_id=DATAPREP_CONN_ID,
            flow_id=FLOW_ID,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(dataprep_conn_id="dataprep_default")
        hook_mock.return_value.delete_flow.assert_called_once_with(
            flow_id=FLOW_ID,
        )

    @pytest.mark.db_test
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute_with_template_params(
        self, _, create_task_instance_of_operator, session
    ):
        dag_id = "test_execute_delete_flow_with_template"
        ti = create_task_instance_of_operator(
            DataprepDeleteFlowOperator,
            dag_id=dag_id,
            task_id=TASK_ID,
            flow_id="{{ dag.dag_id }}",
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        assert dag_id == ti.task.flow_id


class TestDataprepRunFlowOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute(self, hook_mock):
        op = DataprepRunFlowOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT_ID,
            dataprep_conn_id=DATAPREP_CONN_ID,
            flow_id=FLOW_ID,
            body_request={},
        )
        op.execute(context=mock.MagicMock())
        hook_mock.assert_called_once_with(dataprep_conn_id="dataprep_default")
        hook_mock.return_value.run_flow.assert_called_once_with(
            flow_id=FLOW_ID,
            body_request={},
        )

    @pytest.mark.db_test
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute_with_template_params(
        self, _, create_task_instance_of_operator, session
    ):
        dag_id = "test_execute_run_flow_with_template"
        ti = create_task_instance_of_operator(
            DataprepRunFlowOperator,
            dag_id=dag_id,
            task_id=TASK_ID,
            project_id="{{ dag.dag_id }}",
            flow_id="{{ dag.dag_id }}",
            body_request={},
        )
        session.add(ti)
        session.commit()
        ti.render_templates()

        assert dag_id == ti.task.project_id
        assert dag_id == ti.task.flow_id
