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
from unittest.mock import ANY

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.dataproc_v1 import (
    BatchControllerAsyncClient,
    ClusterControllerAsyncClient,
    JobControllerAsyncClient,
    JobStatus,
    WorkflowTemplateServiceAsyncClient,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataproc import DataprocAsyncHook, DataprocHook, DataProcJobBuilder
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.version import version
from tests.providers.google.cloud.utils.compat import AsyncMock, async_mock

AIRFLOW_VERSION = "v" + version.replace(".", "-").replace("+", "-")

JOB = {"job": "test-job"}
JOB_ID = "test-id"
TASK_ID = "test-task-id"
GCP_LOCATION = "global"
GCP_PROJECT = "test-project"
CLUSTER_CONFIG = {"test": "test"}
LABELS = {"test": "test"}
CLUSTER_NAME = "cluster-name"
CLUSTER = {
    "cluster_name": CLUSTER_NAME,
    "config": CLUSTER_CONFIG,
    "labels": LABELS,
    "project_id": GCP_PROJECT,
}
BATCH = {"batch": "test-batch"}
BATCH_ID = "batch-id"
BATCH_NAME = "projects/{}/regions/{}/batches/{}"
PARENT = "projects/{}/regions/{}"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
DATAPROC_STRING = "airflow.providers.google.cloud.hooks.dataproc.{}"


def mock_init(*args, **kwargs):
    pass


async def mock_awaitable(*args, **kwargs):
    pass


class TestDataprocHook:
    def setup_method(self):
        with mock.patch(BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_init):
            self.hook = DataprocHook(gcp_conn_id="test")

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("ClusterControllerClient"))
    def test_get_cluster_client(self, mock_client, mock_get_credentials):
        self.hook.get_cluster_client(region=GCP_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("ClusterControllerClient"))
    def test_get_cluster_client_region(self, mock_client, mock_get_credentials):
        self.hook.get_cluster_client(region="region1")
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=ANY,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("WorkflowTemplateServiceClient"))
    def test_get_template_client_global(self, mock_client, mock_get_credentials):
        _ = self.hook.get_template_client()
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("WorkflowTemplateServiceClient"))
    def test_get_template_client_region(self, mock_client, mock_get_credentials):
        _ = self.hook.get_template_client(region="region1")
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=ANY,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("JobControllerClient"))
    def test_get_job_client(self, mock_client, mock_get_credentials):
        self.hook.get_job_client(region=GCP_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("JobControllerClient"))
    def test_get_job_client_region(self, mock_client, mock_get_credentials):
        self.hook.get_job_client(region="region1")
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=ANY,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("BatchControllerClient"))
    def test_get_batch_client(self, mock_client, mock_get_credentials):
        self.hook.get_batch_client(region=GCP_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("BatchControllerClient"))
    def test_get_batch_client_region(self, mock_client, mock_get_credentials):
        self.hook.get_batch_client(region="region1")
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value, client_info=CLIENT_INFO, client_options=ANY
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_create_cluster(self, mock_client):
        self.hook.create_cluster(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            cluster_name=CLUSTER_NAME,
            cluster_config=CLUSTER_CONFIG,
            labels=LABELS,
        )
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.create_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster=CLUSTER,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_delete_cluster(self, mock_client):
        self.hook.delete_cluster(project_id=GCP_PROJECT, region=GCP_LOCATION, cluster_name=CLUSTER_NAME)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.delete_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster_name=CLUSTER_NAME,
                cluster_uuid=None,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_diagnose_cluster(self, mock_client):
        self.hook.diagnose_cluster(project_id=GCP_PROJECT, region=GCP_LOCATION, cluster_name=CLUSTER_NAME)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.diagnose_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster_name=CLUSTER_NAME,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.diagnose_cluster.return_value.result.assert_called_once_with()

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_get_cluster(self, mock_client):
        self.hook.get_cluster(project_id=GCP_PROJECT, region=GCP_LOCATION, cluster_name=CLUSTER_NAME)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.get_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster_name=CLUSTER_NAME,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_list_clusters(self, mock_client):
        filter_ = "filter"

        self.hook.list_clusters(project_id=GCP_PROJECT, region=GCP_LOCATION, filter_=filter_)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.list_clusters.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                filter=filter_,
                page_size=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_update_cluster(self, mock_client):
        update_mask = "update-mask"
        self.hook.update_cluster(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            cluster=CLUSTER,
            cluster_name=CLUSTER_NAME,
            update_mask=update_mask,
        )
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.update_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster=CLUSTER,
                cluster_name=CLUSTER_NAME,
                update_mask=update_mask,
                graceful_decommission_timeout=None,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_update_cluster_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.update_cluster(
                project_id=GCP_PROJECT,
                cluster=CLUSTER,
                cluster_name=CLUSTER_NAME,
                update_mask="update-mask",
            )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_template_client"))
    def test_create_workflow_template(self, mock_client):
        template = {"test": "test"}
        parent = f"projects/{GCP_PROJECT}/regions/{GCP_LOCATION}"
        self.hook.create_workflow_template(region=GCP_LOCATION, template=template, project_id=GCP_PROJECT)
        mock_client.return_value.create_workflow_template.assert_called_once_with(
            request=dict(parent=parent, template=template), retry=DEFAULT, timeout=None, metadata=()
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_template_client"))
    def test_instantiate_workflow_template(self, mock_client):
        template_name = "template_name"
        name = f"projects/{GCP_PROJECT}/regions/{GCP_LOCATION}/workflowTemplates/{template_name}"
        self.hook.instantiate_workflow_template(
            region=GCP_LOCATION, template_name=template_name, project_id=GCP_PROJECT
        )
        mock_client.return_value.instantiate_workflow_template.assert_called_once_with(
            request=dict(name=name, version=None, parameters=None, request_id=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_template_client"))
    def test_instantiate_workflow_template_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.instantiate_workflow_template(template_name="template_name", project_id=GCP_PROJECT)

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_template_client"))
    def test_instantiate_inline_workflow_template(self, mock_client):
        template = {"test": "test"}
        parent = f"projects/{GCP_PROJECT}/regions/{GCP_LOCATION}"
        self.hook.instantiate_inline_workflow_template(
            region=GCP_LOCATION, template=template, project_id=GCP_PROJECT
        )
        mock_client.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            request=dict(parent=parent, template=template, request_id=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_template_client"))
    def test_instantiate_inline_workflow_template_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.instantiate_inline_workflow_template(template={"test": "test"}, project_id=GCP_PROJECT)

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job"))
    def test_wait_for_job(self, mock_get_job):
        mock_get_job.side_effect = [
            mock.MagicMock(status=mock.MagicMock(state=JobStatus.State.RUNNING)),
            mock.MagicMock(status=mock.MagicMock(state=JobStatus.State.ERROR)),
        ]
        with pytest.raises(AirflowException):
            self.hook.wait_for_job(job_id=JOB_ID, region=GCP_LOCATION, project_id=GCP_PROJECT, wait_time=0)
        calls = [
            mock.call(region=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT),
            mock.call(region=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT),
        ]
        mock_get_job.assert_has_calls(calls)

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job"))
    def test_wait_for_job_missing_region(self, mock_get_job):
        with pytest.raises(TypeError):
            self.hook.wait_for_job(job_id=JOB_ID, project_id=GCP_PROJECT, wait_time=0)

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job_client"))
    def test_get_job(self, mock_client):
        self.hook.get_job(region=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.get_job.assert_called_once_with(
            request=dict(
                region=GCP_LOCATION,
                job_id=JOB_ID,
                project_id=GCP_PROJECT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job_client"))
    def test_get_job_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.get_job(job_id=JOB_ID, project_id=GCP_PROJECT)

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job_client"))
    def test_submit_job(self, mock_client):
        self.hook.submit_job(region=GCP_LOCATION, job=JOB, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.submit_job.assert_called_once_with(
            request=dict(
                region=GCP_LOCATION,
                job=JOB,
                project_id=GCP_PROJECT,
                request_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job_client"))
    def test_submit_job_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.submit_job(job=JOB, project_id=GCP_PROJECT)

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job_client"))
    def test_cancel_job(self, mock_client):
        self.hook.cancel_job(region=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.cancel_job.assert_called_once_with(
            request=dict(
                region=GCP_LOCATION,
                job_id=JOB_ID,
                project_id=GCP_PROJECT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_batch_client"))
    def test_create_batch(self, mock_client):
        self.hook.create_batch(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            batch=BATCH,
            batch_id=BATCH_ID,
        )
        mock_client.assert_called_once_with(GCP_LOCATION)
        mock_client.return_value.create_batch.assert_called_once_with(
            request=dict(
                parent=PARENT.format(GCP_PROJECT, GCP_LOCATION),
                batch=BATCH,
                batch_id=BATCH_ID,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_batch_client"))
    def test_delete_batch(self, mock_client):
        self.hook.delete_batch(
            batch_id=BATCH_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
        )
        mock_client.assert_called_once_with(GCP_LOCATION)
        mock_client.return_value.delete_batch.assert_called_once_with(
            request=dict(
                name=BATCH_NAME.format(GCP_PROJECT, GCP_LOCATION, BATCH_ID),
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_batch_client"))
    def test_get_batch(self, mock_client):
        self.hook.get_batch(
            batch_id=BATCH_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
        )
        mock_client.assert_called_once_with(GCP_LOCATION)
        mock_client.return_value.get_batch.assert_called_once_with(
            request=dict(
                name=BATCH_NAME.format(GCP_PROJECT, GCP_LOCATION, BATCH_ID),
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_batch_client"))
    def test_list_batches(self, mock_client):
        self.hook.list_batches(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
        )
        mock_client.assert_called_once_with(GCP_LOCATION)
        mock_client.return_value.list_batches.assert_called_once_with(
            request=dict(
                parent=PARENT.format(GCP_PROJECT, GCP_LOCATION),
                page_size=None,
                page_token=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )


class TestDataprocAsyncHook:
    def setup_method(self):
        with mock.patch(BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_init):
            self.hook = DataprocAsyncHook(gcp_conn_id="test")

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("ClusterControllerAsyncClient"))
    def test_get_cluster_client(self, mock_client, mock_get_credentials):
        self.hook.get_cluster_client(region=GCP_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("ClusterControllerAsyncClient"))
    def test_get_cluster_client_region(self, mock_client, mock_get_credentials):
        self.hook.get_cluster_client(region="region1")
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=ANY,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("WorkflowTemplateServiceAsyncClient"))
    def test_get_template_client_global(self, mock_client, mock_get_credentials):
        _ = self.hook.get_template_client()
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("WorkflowTemplateServiceAsyncClient"))
    def test_get_template_client_region(self, mock_client, mock_get_credentials):
        _ = self.hook.get_template_client(region="region1")
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=ANY,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("JobControllerAsyncClient"))
    def test_get_job_client(self, mock_client, mock_get_credentials):
        self.hook.get_job_client(region=GCP_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("JobControllerAsyncClient"))
    def test_get_job_client_region(self, mock_client, mock_get_credentials):
        self.hook.get_job_client(region="region1")
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=ANY,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("BatchControllerAsyncClient"))
    def test_get_batch_client(self, mock_client, mock_get_credentials):
        self.hook.get_batch_client(region=GCP_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_credentials"))
    @mock.patch(DATAPROC_STRING.format("BatchControllerAsyncClient"))
    def test_get_batch_client_region(self, mock_client, mock_get_credentials):
        self.hook.get_batch_client(region="region1")
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value, client_info=CLIENT_INFO, client_options=ANY
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_cluster_client"))
    async def test_create_cluster(self, mock_client):
        mock_cluster_client = AsyncMock(ClusterControllerAsyncClient)
        mock_client.return_value = mock_cluster_client
        await self.hook.create_cluster(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            cluster_name=CLUSTER_NAME,
            cluster_config=CLUSTER_CONFIG,
            labels=LABELS,
        )
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.create_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster=CLUSTER,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_cluster_client"))
    async def test_delete_cluster(self, mock_client):
        mock_cluster_client = AsyncMock(ClusterControllerAsyncClient)
        mock_client.return_value = mock_cluster_client
        await self.hook.delete_cluster(project_id=GCP_PROJECT, region=GCP_LOCATION, cluster_name=CLUSTER_NAME)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.delete_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster_name=CLUSTER_NAME,
                cluster_uuid=None,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_cluster_client"))
    async def test_diagnose_cluster(self, mock_client):
        mock_cluster_client = AsyncMock(ClusterControllerAsyncClient)
        mock_client.return_value = mock_cluster_client
        await self.hook.diagnose_cluster(
            project_id=GCP_PROJECT, region=GCP_LOCATION, cluster_name=CLUSTER_NAME
        )
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.diagnose_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster_name=CLUSTER_NAME,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.diagnose_cluster.return_value.result.assert_called_once_with()

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_cluster_client"))
    async def test_get_cluster(self, mock_client):
        mock_cluster_client = AsyncMock(ClusterControllerAsyncClient)
        mock_client.return_value = mock_cluster_client
        await self.hook.get_cluster(project_id=GCP_PROJECT, region=GCP_LOCATION, cluster_name=CLUSTER_NAME)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.get_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster_name=CLUSTER_NAME,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_cluster_client"))
    async def test_list_clusters(self, mock_client):
        filter_ = "filter"
        mock_cluster_client = AsyncMock(ClusterControllerAsyncClient)
        mock_client.return_value = mock_cluster_client
        await self.hook.list_clusters(project_id=GCP_PROJECT, region=GCP_LOCATION, filter_=filter_)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.list_clusters.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                filter=filter_,
                page_size=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_cluster_client"))
    async def test_update_cluster(self, mock_client):
        update_mask = "update-mask"
        mock_cluster_client = AsyncMock(ClusterControllerAsyncClient)
        mock_client.return_value = mock_cluster_client
        await self.hook.update_cluster(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            cluster=CLUSTER,
            cluster_name=CLUSTER_NAME,
            update_mask=update_mask,
        )
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.update_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster=CLUSTER,
                cluster_name=CLUSTER_NAME,
                update_mask=update_mask,
                graceful_decommission_timeout=None,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_cluster_client"))
    def test_update_cluster_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.update_cluster(
                project_id=GCP_PROJECT,
                cluster=CLUSTER,
                cluster_name=CLUSTER_NAME,
                update_mask="update-mask",
            )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_template_client"))
    async def test_create_workflow_template(self, mock_client):
        template = {"test": "test"}
        parent = f"projects/{GCP_PROJECT}/regions/{GCP_LOCATION}"
        mock_template_client = AsyncMock(WorkflowTemplateServiceAsyncClient)
        mock_client.return_value = mock_template_client
        await self.hook.create_workflow_template(
            region=GCP_LOCATION, template=template, project_id=GCP_PROJECT
        )
        mock_client.return_value.create_workflow_template.assert_called_once_with(
            request=dict(parent=parent, template=template), retry=DEFAULT, timeout=None, metadata=()
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_template_client"))
    async def test_instantiate_workflow_template(self, mock_client):
        template_name = "template_name"
        name = f"projects/{GCP_PROJECT}/regions/{GCP_LOCATION}/workflowTemplates/{template_name}"
        mock_template_client = AsyncMock(WorkflowTemplateServiceAsyncClient)
        mock_client.return_value = mock_template_client
        await self.hook.instantiate_workflow_template(
            region=GCP_LOCATION, template_name=template_name, project_id=GCP_PROJECT
        )
        mock_client.return_value.instantiate_workflow_template.assert_called_once_with(
            request=dict(name=name, version=None, parameters=None, request_id=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_operation"))
    async def test_get_operation(self, mock_client):
        mock_client.return_value = None
        hook = DataprocAsyncHook(
            gcp_conn_id="google_cloud_default", delegate_to=None, impersonation_chain=None
        )
        await hook.get_operation(region=GCP_LOCATION, operation_name="operation_name")
        mock_client.assert_called_once()
        mock_client.assert_called_with(region=GCP_LOCATION, operation_name="operation_name")

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_template_client"))
    def test_instantiate_workflow_template_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.instantiate_workflow_template(template_name="template_name", project_id=GCP_PROJECT)

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_template_client"))
    async def test_instantiate_inline_workflow_template(self, mock_client):
        template = {"test": "test"}
        parent = f"projects/{GCP_PROJECT}/regions/{GCP_LOCATION}"
        mock_template_client = AsyncMock(WorkflowTemplateServiceAsyncClient)
        mock_client.return_value = mock_template_client
        await self.hook.instantiate_inline_workflow_template(
            region=GCP_LOCATION, template=template, project_id=GCP_PROJECT
        )
        mock_client.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            request=dict(parent=parent, template=template, request_id=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_template_client"))
    def test_instantiate_inline_workflow_template_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.instantiate_inline_workflow_template(template={"test": "test"}, project_id=GCP_PROJECT)

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_job_client"))
    async def test_get_job(self, mock_client):
        mock_job_client = AsyncMock(JobControllerAsyncClient)
        mock_client.return_value = mock_job_client
        await self.hook.get_job(region=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.get_job.assert_called_once_with(
            request=dict(
                region=GCP_LOCATION,
                job_id=JOB_ID,
                project_id=GCP_PROJECT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_job_client"))
    def test_get_job_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.get_job(job_id=JOB_ID, project_id=GCP_PROJECT)

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_job_client"))
    async def test_submit_job(self, mock_client):
        mock_job_client = AsyncMock(JobControllerAsyncClient)
        mock_client.return_value = mock_job_client
        await self.hook.submit_job(region=GCP_LOCATION, job=JOB, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.submit_job.assert_called_once_with(
            request=dict(
                region=GCP_LOCATION,
                job=JOB,
                project_id=GCP_PROJECT,
                request_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_job_client"))
    def test_submit_job_missing_region(self, mock_client):
        with pytest.raises(TypeError):
            self.hook.submit_job(job=JOB, project_id=GCP_PROJECT)

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_job_client"))
    async def test_cancel_job(self, mock_client):
        mock_job_client = AsyncMock(JobControllerAsyncClient)
        mock_client.return_value = mock_job_client
        await self.hook.cancel_job(region=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.cancel_job.assert_called_once_with(
            request=dict(
                region=GCP_LOCATION,
                job_id=JOB_ID,
                project_id=GCP_PROJECT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_batch_client"))
    async def test_create_batch(self, mock_client):
        mock_batch_client = AsyncMock(BatchControllerAsyncClient)
        mock_client.return_value = mock_batch_client
        await self.hook.create_batch(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            batch=BATCH,
            batch_id=BATCH_ID,
        )
        mock_client.assert_called_once_with(GCP_LOCATION)
        mock_client.return_value.create_batch.assert_called_once_with(
            request=dict(
                parent=PARENT.format(GCP_PROJECT, GCP_LOCATION),
                batch=BATCH,
                batch_id=BATCH_ID,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_batch_client"))
    async def test_delete_batch(self, mock_client):
        mock_batch_client = AsyncMock(BatchControllerAsyncClient)
        mock_client.return_value = mock_batch_client
        await self.hook.delete_batch(
            batch_id=BATCH_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
        )
        mock_client.assert_called_once_with(GCP_LOCATION)
        mock_client.return_value.delete_batch.assert_called_once_with(
            request=dict(
                name=BATCH_NAME.format(GCP_PROJECT, GCP_LOCATION, BATCH_ID),
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_batch_client"))
    async def test_get_batch(self, mock_client):
        mock_batch_client = AsyncMock(BatchControllerAsyncClient)
        mock_client.return_value = mock_batch_client
        await self.hook.get_batch(
            batch_id=BATCH_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
        )
        mock_client.assert_called_once_with(GCP_LOCATION)
        mock_client.return_value.get_batch.assert_called_once_with(
            request=dict(
                name=BATCH_NAME.format(GCP_PROJECT, GCP_LOCATION, BATCH_ID),
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @pytest.mark.asyncio
    @async_mock.patch(DATAPROC_STRING.format("DataprocAsyncHook.get_batch_client"))
    async def test_list_batches(self, mock_client):
        mock_batch_client = AsyncMock(BatchControllerAsyncClient)
        mock_client.return_value = mock_batch_client
        await self.hook.list_batches(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
        )
        mock_client.assert_called_once_with(GCP_LOCATION)
        mock_client.return_value.list_batches.assert_called_once_with(
            request=dict(
                parent=PARENT.format(GCP_PROJECT, GCP_LOCATION),
                page_size=None,
                page_token=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )


class TestDataProcJobBuilder:
    def setup_method(self) -> None:
        self.job_type = "test"
        self.builder = DataProcJobBuilder(
            project_id=GCP_PROJECT,
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            job_type=self.job_type,
            properties={"test": "test"},
        )

    @pytest.mark.parametrize("job_name", [TASK_ID, f"group.{TASK_ID}"])
    @mock.patch(DATAPROC_STRING.format("uuid.uuid4"))
    def test_init(self, mock_uuid, job_name):
        mock_uuid.return_value = "uuid"
        properties = {"test": "test"}
        expected_job_id = f"{job_name}_{mock_uuid.return_value}".replace(".", "_")
        expected_job = {
            "job": {
                "labels": {"airflow-version": AIRFLOW_VERSION},
                "placement": {"cluster_name": CLUSTER_NAME},
                "reference": {"job_id": expected_job_id, "project_id": GCP_PROJECT},
                "test": {"properties": properties},
            }
        }
        builder = DataProcJobBuilder(
            project_id=GCP_PROJECT,
            task_id=job_name,
            cluster_name=CLUSTER_NAME,
            job_type="test",
            properties=properties,
        )
        assert expected_job == builder.job

    def test_add_labels(self):
        labels = {"key": "value"}
        self.builder.add_labels(labels)
        assert "key" in self.builder.job["job"]["labels"]
        assert "value" == self.builder.job["job"]["labels"]["key"]

    def test_add_variables(self):
        variables = ["variable"]
        self.builder.add_variables(variables)
        assert variables == self.builder.job["job"][self.job_type]["script_variables"]

    def test_add_args(self):
        args = ["args"]
        self.builder.add_args(args)
        assert args == self.builder.job["job"][self.job_type]["args"]

    def test_add_query(self):
        query = ["query"]
        self.builder.add_query(query)
        assert {"queries": [query]} == self.builder.job["job"][self.job_type]["query_list"]

    def test_add_query_uri(self):
        query_uri = "query_uri"
        self.builder.add_query_uri(query_uri)
        assert query_uri == self.builder.job["job"][self.job_type]["query_file_uri"]

    def test_add_jar_file_uris(self):
        jar_file_uris = ["jar_file_uris"]
        self.builder.add_jar_file_uris(jar_file_uris)
        assert jar_file_uris == self.builder.job["job"][self.job_type]["jar_file_uris"]

    def test_add_archive_uris(self):
        archive_uris = ["archive_uris"]
        self.builder.add_archive_uris(archive_uris)
        assert archive_uris == self.builder.job["job"][self.job_type]["archive_uris"]

    def test_add_file_uris(self):
        file_uris = ["file_uris"]
        self.builder.add_file_uris(file_uris)
        assert file_uris == self.builder.job["job"][self.job_type]["file_uris"]

    def test_add_python_file_uris(self):
        python_file_uris = ["python_file_uris"]
        self.builder.add_python_file_uris(python_file_uris)
        assert python_file_uris == self.builder.job["job"][self.job_type]["python_file_uris"]

    def test_set_main_error(self):
        with pytest.raises(Exception):
            self.builder.set_main("test", "test")

    def test_set_main_class(self):
        main = "main"
        self.builder.set_main(main_class=main, main_jar=None)
        assert main == self.builder.job["job"][self.job_type]["main_class"]

    def test_set_main_jar(self):
        main = "main"
        self.builder.set_main(main_class=None, main_jar=main)
        assert main == self.builder.job["job"][self.job_type]["main_jar_file_uri"]

    def test_set_python_main(self):
        main = "main"
        self.builder.set_python_main(main)
        assert main == self.builder.job["job"][self.job_type]["main_python_file_uri"]

    @pytest.mark.parametrize(
        "job_name",
        [
            pytest.param("name", id="simple"),
            pytest.param("name_with_dash", id="name with underscores"),
            pytest.param("group.name", id="name with dot"),
            pytest.param("group.name_with_dash", id="name with dot and underscores"),
        ],
    )
    @mock.patch(DATAPROC_STRING.format("uuid.uuid4"))
    def test_set_job_name(self, mock_uuid, job_name):
        uuid = "test_uuid"
        expected_job_name = f"{job_name}_{uuid[:8]}".replace(".", "_")
        mock_uuid.return_value = uuid
        self.builder.set_job_name(job_name)
        assert expected_job_name == self.builder.job["job"]["reference"]["job_id"]
        assert len(self.builder.job["job"]["reference"]["job_id"]) == len(job_name) + 9

    def test_build(self):
        assert self.builder.job == self.builder.build()
