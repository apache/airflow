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
"""
This module contains various unit tests for Google Cloud DLP Operators
"""
from __future__ import annotations

from unittest import mock

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.dlp_v2.types import (
    DeidentifyContentResponse,
    DeidentifyTemplate,
    DlpJob,
    InspectContentResponse,
    InspectTemplate,
    JobTrigger,
    ListInfoTypesResponse,
    RedactImageResponse,
    ReidentifyContentResponse,
    StoredInfoType,
)

from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCancelDLPJobOperator,
    CloudDLPCreateDeidentifyTemplateOperator,
    CloudDLPCreateDLPJobOperator,
    CloudDLPCreateInspectTemplateOperator,
    CloudDLPCreateJobTriggerOperator,
    CloudDLPCreateStoredInfoTypeOperator,
    CloudDLPDeidentifyContentOperator,
    CloudDLPDeleteDeidentifyTemplateOperator,
    CloudDLPDeleteDLPJobOperator,
    CloudDLPDeleteInspectTemplateOperator,
    CloudDLPDeleteJobTriggerOperator,
    CloudDLPDeleteStoredInfoTypeOperator,
    CloudDLPGetDeidentifyTemplateOperator,
    CloudDLPGetDLPJobOperator,
    CloudDLPGetDLPJobTriggerOperator,
    CloudDLPGetInspectTemplateOperator,
    CloudDLPGetStoredInfoTypeOperator,
    CloudDLPInspectContentOperator,
    CloudDLPListDeidentifyTemplatesOperator,
    CloudDLPListDLPJobsOperator,
    CloudDLPListInfoTypesOperator,
    CloudDLPListInspectTemplatesOperator,
    CloudDLPListJobTriggersOperator,
    CloudDLPListStoredInfoTypesOperator,
    CloudDLPRedactImageOperator,
    CloudDLPReidentifyContentOperator,
    CloudDLPUpdateDeidentifyTemplateOperator,
    CloudDLPUpdateInspectTemplateOperator,
    CloudDLPUpdateJobTriggerOperator,
    CloudDLPUpdateStoredInfoTypeOperator,
)

GCP_CONN_ID = "google_cloud_default"
ORGANIZATION_ID = "test-org"
PROJECT_ID = "test-project"
DLP_JOB_ID = "job123"
TEMPLATE_ID = "template123"
STORED_INFO_TYPE_ID = "type123"
TRIGGER_ID = "trigger123"
DLP_JOB_PATH = f"projects/{PROJECT_ID}/dlpJobs/{DLP_JOB_ID}"
DLP_JOB_TRIGGER_PATH = f"projects/{PROJECT_ID}/jobTriggers/{TRIGGER_ID}"


class TestCloudDLPCancelDLPJobOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_cancel_dlp_job(self, mock_hook):
        mock_hook.return_value.cancel_dlp_job.return_value = mock.MagicMock()
        operator = CloudDLPCancelDLPJobOperator(dlp_job_id=DLP_JOB_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.cancel_dlp_job.assert_called_once_with(
            dlp_job_id=DLP_JOB_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPCreateDeidentifyTemplateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_deidentify_template(self, mock_hook):
        mock_hook.return_value.create_deidentify_template.return_value = mock.MagicMock()
        mock_hook.return_value.create_deidentify_template.return_value = DeidentifyTemplate(name=DLP_JOB_PATH)
        operator = CloudDLPCreateDeidentifyTemplateOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_deidentify_template.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            deidentify_template=None,
            template_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPCreateDLPJobOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_dlp_job(self, mock_hook):
        mock_hook.return_value.create_dlp_job.return_value = DlpJob(
            name=DLP_JOB_PATH, state=DlpJob.JobState.PENDING
        )
        operator = CloudDLPCreateDLPJobOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_dlp_job.assert_called_once_with(
            project_id=PROJECT_ID,
            inspect_job=None,
            risk_job=None,
            job_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
            wait_until_finished=True,
        )


class TestCloudDLPCreateInspectTemplateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_inspect_template(self, mock_hook):
        mock_hook.return_value.create_inspect_template.return_value = InspectTemplate(name=DLP_JOB_PATH)
        operator = CloudDLPCreateInspectTemplateOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_inspect_template.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            inspect_template=None,
            template_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPCreateJobTriggerOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_job_trigger(self, mock_hook):
        mock_hook.return_value.create_job_trigger.return_value = JobTrigger(name=DLP_JOB_TRIGGER_PATH)
        operator = CloudDLPCreateJobTriggerOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_job_trigger.assert_called_once_with(
            project_id=PROJECT_ID,
            job_trigger=None,
            trigger_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPCreateStoredInfoTypeOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_stored_info_type(self, mock_hook):
        mock_hook.return_value.create_stored_info_type.return_value = StoredInfoType(name=DLP_JOB_PATH)
        operator = CloudDLPCreateStoredInfoTypeOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_stored_info_type.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            config=None,
            stored_info_type_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPDeidentifyContentOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_deidentify_content(self, mock_hook):
        mock_hook.return_value.deidentify_content.return_value = DeidentifyContentResponse()
        operator = CloudDLPDeidentifyContentOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.deidentify_content.assert_called_once_with(
            project_id=PROJECT_ID,
            deidentify_config=None,
            inspect_config=None,
            item=None,
            inspect_template_name=None,
            deidentify_template_name=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPDeleteDeidentifyTemplateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_deidentify_template(self, mock_hook):
        mock_hook.return_value.delete_deidentify_template.return_value = mock.MagicMock()
        operator = CloudDLPDeleteDeidentifyTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_deidentify_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPDeleteDlpJobOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_dlp_job(self, mock_hook):
        mock_hook.return_value.delete_dlp_job.return_value = mock.MagicMock()
        operator = CloudDLPDeleteDLPJobOperator(dlp_job_id=DLP_JOB_ID, project_id=PROJECT_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_dlp_job.assert_called_once_with(
            dlp_job_id=DLP_JOB_ID,
            project_id=PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPDeleteInspectTemplateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_inspect_template(self, mock_hook):
        mock_hook.return_value.delete_inspect_template.return_value = mock.MagicMock()
        operator = CloudDLPDeleteInspectTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_inspect_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPDeleteJobTriggerOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_job_trigger(self, mock_hook):
        mock_hook.return_value.delete_job_trigger.return_value = mock.MagicMock()
        operator = CloudDLPDeleteJobTriggerOperator(
            job_trigger_id=TRIGGER_ID, project_id=PROJECT_ID, task_id="id"
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_job_trigger.assert_called_once_with(
            job_trigger_id=TRIGGER_ID,
            project_id=PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPDeleteStoredInfoTypeOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_stored_info_type(self, mock_hook):
        mock_hook.return_value.delete_stored_info_type.return_value = mock.MagicMock()
        operator = CloudDLPDeleteStoredInfoTypeOperator(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            task_id="id",
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_stored_info_type.assert_called_once_with(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPGetDeidentifyTemplateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_deidentify_template(self, mock_hook):
        mock_hook.return_value.get_deidentify_template.return_value = DeidentifyTemplate()
        operator = CloudDLPGetDeidentifyTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_deidentify_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPGetDlpJobOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_dlp_job(self, mock_hook):
        mock_hook.return_value.get_dlp_job.return_value = DlpJob()
        operator = CloudDLPGetDLPJobOperator(dlp_job_id=DLP_JOB_ID, project_id=PROJECT_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_dlp_job.assert_called_once_with(
            dlp_job_id=DLP_JOB_ID,
            project_id=PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPGetInspectTemplateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_inspect_template(self, mock_hook):
        mock_hook.return_value.get_inspect_template.return_value = InspectTemplate()
        operator = CloudDLPGetInspectTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_inspect_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPGetJobTripperOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_job_trigger(self, mock_hook):
        mock_hook.return_value.get_job_trigger.return_value = JobTrigger()
        operator = CloudDLPGetDLPJobTriggerOperator(
            job_trigger_id=TRIGGER_ID, project_id=PROJECT_ID, task_id="id"
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_job_trigger.assert_called_once_with(
            job_trigger_id=TRIGGER_ID,
            project_id=PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPGetStoredInfoTypeOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_stored_info_type(self, mock_hook):
        mock_hook.return_value.get_stored_info_type.return_value = StoredInfoType()
        operator = CloudDLPGetStoredInfoTypeOperator(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            task_id="id",
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_stored_info_type.assert_called_once_with(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPInspectContentOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_inspect_content(self, mock_hook):
        inspect_template_name = "inspect_template_name/name"
        mock_hook.return_value.inspect_content.return_value = InspectContentResponse()
        operator = CloudDLPInspectContentOperator(
            project_id=PROJECT_ID, task_id="id", inspect_template_name=inspect_template_name
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.inspect_content.assert_called_once_with(
            project_id=PROJECT_ID,
            inspect_config=None,
            item=None,
            inspect_template_name=inspect_template_name,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPListDeidentifyTemplatesOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_deidentify_templates(self, mock_hook):
        mock_hook.return_value.list_deidentify_templates.return_value = mock.MagicMock()
        operator = CloudDLPListDeidentifyTemplatesOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_deidentify_templates.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            page_size=None,
            order_by=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPListDlpJobsOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_dlp_jobs(self, mock_hook):
        mock_hook.return_value.list_dlp_jobs.return_value = mock.MagicMock()
        operator = CloudDLPListDLPJobsOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_dlp_jobs.assert_called_once_with(
            project_id=PROJECT_ID,
            results_filter=None,
            page_size=None,
            job_type=None,
            order_by=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPListInfoTypesOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_info_types(self, mock_hook):
        mock_hook.return_value.list_info_types.return_value = ListInfoTypesResponse()
        operator = CloudDLPListInfoTypesOperator(task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_info_types.assert_called_once_with(
            language_code=None,
            results_filter=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPListInspectTemplatesOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_inspect_templates(self, mock_hook):
        mock_hook.return_value.list_inspect_templates.return_value = mock.MagicMock()
        operator = CloudDLPListInspectTemplatesOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_inspect_templates.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            page_size=None,
            order_by=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPListJobTriggersOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_job_triggers(self, mock_hook):
        mock_hook.return_value.list_job_triggers.return_value = mock.MagicMock()
        operator = CloudDLPListJobTriggersOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_job_triggers.assert_called_once_with(
            project_id=PROJECT_ID,
            page_size=None,
            order_by=None,
            results_filter=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPListStoredInfoTypesOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_stored_info_types(self, mock_hook):
        mock_hook.return_value.list_stored_info_types.return_value = mock.MagicMock()
        operator = CloudDLPListStoredInfoTypesOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_stored_info_types.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            page_size=None,
            order_by=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPRedactImageOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_redact_image(self, mock_hook):
        mock_hook.return_value.redact_image.return_value = RedactImageResponse()
        operator = CloudDLPRedactImageOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.redact_image.assert_called_once_with(
            project_id=PROJECT_ID,
            inspect_config=None,
            image_redaction_configs=None,
            include_findings=None,
            byte_item=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPReidentifyContentOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_reidentify_content(self, mock_hook):
        mock_hook.return_value.reidentify_content.return_value = ReidentifyContentResponse()
        operator = CloudDLPReidentifyContentOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.reidentify_content.assert_called_once_with(
            project_id=PROJECT_ID,
            reidentify_config=None,
            inspect_config=None,
            item=None,
            inspect_template_name=None,
            reidentify_template_name=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPUpdateDeidentifyTemplateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_update_deidentify_template(self, mock_hook):
        mock_hook.return_value.update_deidentify_template.return_value = DeidentifyTemplate()
        operator = CloudDLPUpdateDeidentifyTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.update_deidentify_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            deidentify_template=None,
            update_mask=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPUpdateInspectTemplateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_update_inspect_template(self, mock_hook):
        mock_hook.return_value.update_inspect_template.return_value = InspectTemplate()
        operator = CloudDLPUpdateInspectTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.update_inspect_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            inspect_template=None,
            update_mask=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPUpdateJobTriggerOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_update_job_trigger(self, mock_hook):
        mock_hook.return_value.update_job_trigger.return_value = JobTrigger()
        operator = CloudDLPUpdateJobTriggerOperator(job_trigger_id=TRIGGER_ID, task_id="id")
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.update_job_trigger.assert_called_once_with(
            job_trigger_id=TRIGGER_ID,
            project_id=None,
            job_trigger=None,
            update_mask=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudDLPUpdateStoredInfoTypeOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_update_stored_info_type(self, mock_hook):
        mock_hook.return_value.update_stored_info_type.return_value = StoredInfoType()
        operator = CloudDLPUpdateStoredInfoTypeOperator(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            task_id="id",
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.update_stored_info_type.assert_called_once_with(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            config=None,
            update_mask=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
