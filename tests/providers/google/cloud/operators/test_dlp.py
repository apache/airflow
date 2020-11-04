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

# pylint: disable=R0904, C0111
"""
This module contains various unit tests for Google Cloud DLP Operators
"""

import unittest
from unittest import mock

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


class TestCloudDLPCancelDLPJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_cancel_dlp_job(self, mock_hook):
        mock_hook.return_value.cancel_dlp_job.return_value = mock.MagicMock()
        operator = CloudDLPCancelDLPJobOperator(dlp_job_id=DLP_JOB_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.cancel_dlp_job.assert_called_once_with(
            dlp_job_id=DLP_JOB_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPCreateDeidentifyTemplateOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_deidentify_template(self, mock_hook):
        mock_hook.return_value.create_deidentify_template.return_value = mock.MagicMock()
        operator = CloudDLPCreateDeidentifyTemplateOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_deidentify_template.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            deidentify_template=None,
            template_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPCreateDLPJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_dlp_job(self, mock_hook):
        mock_hook.return_value.create_dlp_job.return_value = mock.MagicMock()
        operator = CloudDLPCreateDLPJobOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_dlp_job.assert_called_once_with(
            project_id=PROJECT_ID,
            inspect_job=None,
            risk_job=None,
            job_id=None,
            retry=None,
            timeout=None,
            metadata=None,
            wait_until_finished=True,
        )


class TestCloudDLPCreateInspectTemplateOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_inspect_template(self, mock_hook):
        mock_hook.return_value.create_inspect_template.return_value = mock.MagicMock()
        operator = CloudDLPCreateInspectTemplateOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_inspect_template.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            inspect_template=None,
            template_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPCreateJobTriggerOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_job_trigger(self, mock_hook):
        mock_hook.return_value.create_job_trigger.return_value = mock.MagicMock()
        operator = CloudDLPCreateJobTriggerOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_job_trigger.assert_called_once_with(
            project_id=PROJECT_ID,
            job_trigger=None,
            trigger_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPCreateStoredInfoTypeOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_create_stored_info_type(self, mock_hook):
        mock_hook.return_value.create_stored_info_type.return_value = mock.MagicMock()
        operator = CloudDLPCreateStoredInfoTypeOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_stored_info_type.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            config=None,
            stored_info_type_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPDeidentifyContentOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_deidentify_content(self, mock_hook):
        mock_hook.return_value.deidentify_content.return_value = mock.MagicMock()
        operator = CloudDLPDeidentifyContentOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
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
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPDeleteDeidentifyTemplateOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_deidentify_template(self, mock_hook):
        mock_hook.return_value.delete_deidentify_template.return_value = mock.MagicMock()
        operator = CloudDLPDeleteDeidentifyTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_deidentify_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPDeleteDlpJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_dlp_job(self, mock_hook):
        mock_hook.return_value.delete_dlp_job.return_value = mock.MagicMock()
        operator = CloudDLPDeleteDLPJobOperator(dlp_job_id=DLP_JOB_ID, project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_dlp_job.assert_called_once_with(
            dlp_job_id=DLP_JOB_ID,
            project_id=PROJECT_ID,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPDeleteInspectTemplateOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_inspect_template(self, mock_hook):
        mock_hook.return_value.delete_inspect_template.return_value = mock.MagicMock()
        operator = CloudDLPDeleteInspectTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_inspect_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPDeleteJobTriggerOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_job_trigger(self, mock_hook):
        mock_hook.return_value.delete_job_trigger.return_value = mock.MagicMock()
        operator = CloudDLPDeleteJobTriggerOperator(
            job_trigger_id=TRIGGER_ID, project_id=PROJECT_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_job_trigger.assert_called_once_with(
            job_trigger_id=TRIGGER_ID,
            project_id=PROJECT_ID,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPDeleteStoredInfoTypeOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_delete_stored_info_type(self, mock_hook):
        mock_hook.return_value.delete_stored_info_type.return_value = mock.MagicMock()
        operator = CloudDLPDeleteStoredInfoTypeOperator(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            task_id="id",
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_stored_info_type.assert_called_once_with(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPGetDeidentifyTemplateOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_deidentify_template(self, mock_hook):
        mock_hook.return_value.get_deidentify_template.return_value = mock.MagicMock()
        operator = CloudDLPGetDeidentifyTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_deidentify_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPGetDlpJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_dlp_job(self, mock_hook):
        mock_hook.return_value.get_dlp_job.return_value = mock.MagicMock()
        operator = CloudDLPGetDLPJobOperator(dlp_job_id=DLP_JOB_ID, project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_dlp_job.assert_called_once_with(
            dlp_job_id=DLP_JOB_ID,
            project_id=PROJECT_ID,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPGetInspectTemplateOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_inspect_template(self, mock_hook):
        mock_hook.return_value.get_inspect_template.return_value = mock.MagicMock()
        operator = CloudDLPGetInspectTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_inspect_template.assert_called_once_with(
            template_id=TEMPLATE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPGetJobTripperOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_job_trigger(self, mock_hook):
        mock_hook.return_value.get_job_trigger.return_value = mock.MagicMock()
        operator = CloudDLPGetDLPJobTriggerOperator(
            job_trigger_id=TRIGGER_ID, project_id=PROJECT_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_job_trigger.assert_called_once_with(
            job_trigger_id=TRIGGER_ID,
            project_id=PROJECT_ID,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPGetStoredInfoTypeOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_get_stored_info_type(self, mock_hook):
        mock_hook.return_value.get_stored_info_type.return_value = mock.MagicMock()
        operator = CloudDLPGetStoredInfoTypeOperator(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            task_id="id",
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_stored_info_type.assert_called_once_with(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPInspectContentOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_inspect_content(self, mock_hook):
        mock_hook.return_value.inspect_content.return_value = mock.MagicMock()
        operator = CloudDLPInspectContentOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.inspect_content.assert_called_once_with(
            project_id=PROJECT_ID,
            inspect_config=None,
            item=None,
            inspect_template_name=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPListDeidentifyTemplatesOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_deidentify_templates(self, mock_hook):
        mock_hook.return_value.list_deidentify_templates.return_value = mock.MagicMock()
        operator = CloudDLPListDeidentifyTemplatesOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_deidentify_templates.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            page_size=None,
            order_by=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPListDlpJobsOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_dlp_jobs(self, mock_hook):
        mock_hook.return_value.list_dlp_jobs.return_value = mock.MagicMock()
        operator = CloudDLPListDLPJobsOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
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
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPListInfoTypesOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_info_types(self, mock_hook):
        mock_hook.return_value.list_info_types.return_value = mock.MagicMock()
        operator = CloudDLPListInfoTypesOperator(task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_info_types.assert_called_once_with(
            language_code=None,
            results_filter=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPListInspectTemplatesOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_inspect_templates(self, mock_hook):
        mock_hook.return_value.list_inspect_templates.return_value = mock.MagicMock()
        operator = CloudDLPListInspectTemplatesOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_inspect_templates.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            page_size=None,
            order_by=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPListJobTriggersOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_job_triggers(self, mock_hook):
        mock_hook.return_value.list_job_triggers.return_value = mock.MagicMock()
        operator = CloudDLPListJobTriggersOperator(project_id=PROJECT_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_job_triggers.assert_called_once_with(
            project_id=PROJECT_ID,
            page_size=None,
            order_by=None,
            results_filter=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPListStoredInfoTypesOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_list_stored_info_types(self, mock_hook):
        mock_hook.return_value.list_stored_info_types.return_value = mock.MagicMock()
        operator = CloudDLPListStoredInfoTypesOperator(organization_id=ORGANIZATION_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_stored_info_types.assert_called_once_with(
            organization_id=ORGANIZATION_ID,
            project_id=None,
            page_size=None,
            order_by=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPRedactImageOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_redact_image(self, mock_hook):
        mock_hook.return_value.redact_image.return_value = mock.MagicMock()
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
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPReidentifyContentOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_reidentify_content(self, mock_hook):
        mock_hook.return_value.reidentify_content.return_value = mock.MagicMock()
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
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPUpdateDeidentifyTemplateOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_update_deidentify_template(self, mock_hook):
        mock_hook.return_value.update_deidentify_template.return_value = mock.MagicMock()
        operator = CloudDLPUpdateDeidentifyTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=None)
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
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPUpdateInspectTemplateOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_update_inspect_template(self, mock_hook):
        mock_hook.return_value.update_inspect_template.return_value = mock.MagicMock()
        operator = CloudDLPUpdateInspectTemplateOperator(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID, task_id="id"
        )
        operator.execute(context=None)
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
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPUpdateJobTriggerOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_update_job_trigger(self, mock_hook):
        mock_hook.return_value.update_job_trigger.return_value = mock.MagicMock()
        operator = CloudDLPUpdateJobTriggerOperator(job_trigger_id=TRIGGER_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.update_job_trigger.assert_called_once_with(
            job_trigger_id=TRIGGER_ID,
            project_id=None,
            job_trigger=None,
            update_mask=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudDLPUpdateStoredInfoTypeOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_update_stored_info_type(self, mock_hook):
        mock_hook.return_value.update_stored_info_type.return_value = mock.MagicMock()
        operator = CloudDLPUpdateStoredInfoTypeOperator(
            stored_info_type_id=STORED_INFO_TYPE_ID,
            organization_id=ORGANIZATION_ID,
            task_id="id",
        )
        operator.execute(context=None)
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
            retry=None,
            timeout=None,
            metadata=None,
        )
