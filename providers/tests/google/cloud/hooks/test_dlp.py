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
This module contains various unit tests for
functions in CloudDLPHook
"""

from __future__ import annotations

from typing import Any
from unittest import mock
from unittest.mock import PropertyMock

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.dlp_v2.types import DlpJob

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dlp import CloudDLPHook
from airflow.providers.google.common.consts import CLIENT_INFO

from providers.tests.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id

API_RESPONSE: dict[Any, Any] = {}
ORGANIZATION_ID = "test-org"
ORGANIZATION_PATH = f"organizations/{ORGANIZATION_ID}"
PROJECT_ID = "test-project"
PROJECT_PATH = f"projects/{PROJECT_ID}"
DLP_JOB_ID = "job123"
DLP_JOB_PATH = f"projects/{PROJECT_ID}/dlpJobs/{DLP_JOB_ID}"
TEMPLATE_ID = "template123"
STORED_INFO_TYPE_ID = "type123"
TRIGGER_ID = "trigger123"
DEIDENTIFY_TEMPLATE_ORGANIZATION_PATH = f"organizations/{ORGANIZATION_ID}/deidentifyTemplates/{TEMPLATE_ID}"
INSPECT_TEMPLATE_ORGANIZATION_PATH = f"organizations/{ORGANIZATION_ID}/inspectTemplates/{TEMPLATE_ID}"
STORED_INFO_TYPE_ORGANIZATION_PATH = f"organizations/{ORGANIZATION_ID}/storedInfoTypes/{STORED_INFO_TYPE_ID}"
DEIDENTIFY_TEMPLATE_PROJECT_PATH = f"projects/{PROJECT_ID}/deidentifyTemplates/{TEMPLATE_ID}"
INSPECT_TEMPLATE_PROJECT_PATH = f"projects/{PROJECT_ID}/inspectTemplates/{TEMPLATE_ID}"
STORED_INFO_TYPE_PROJECT_PATH = f"projects/{PROJECT_ID}/storedInfoTypes/{STORED_INFO_TYPE_ID}"
JOB_TRIGGER_PATH = f"projects/{PROJECT_ID}/jobTriggers/{TRIGGER_ID}"


class TestCloudDLPHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            CloudDLPHook(gcp_conn_id="GCP_CONN_ID", delegate_to="delegate_to")

    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudDLPHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.DlpServiceClient")
    def test_dlp_service_client_creation(self, mock_client, mock_get_creds):
        result = self.hook.get_conn()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value, client_info=CLIENT_INFO)
        assert mock_client.return_value == result
        assert self.hook._client == result

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_cancel_dlp_job(self, get_conn):
        self.hook.cancel_dlp_job(dlp_job_id=DLP_JOB_ID, project_id=PROJECT_ID)

        get_conn.return_value.cancel_dlp_job.assert_called_once_with(
            request=dict(name=DLP_JOB_PATH), retry=DEFAULT, timeout=None, metadata=()
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_cancel_dlp_job_without_dlp_job_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.cancel_dlp_job(dlp_job_id=None, project_id=PROJECT_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_cancel_dlp_job_without_parent(self, _, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.cancel_dlp_job(dlp_job_id=DLP_JOB_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_deidentify_template_with_org_id(self, get_conn, mock_project_id):
        get_conn.return_value.create_deidentify_template.return_value = API_RESPONSE
        result = self.hook.create_deidentify_template(organization_id=ORGANIZATION_ID)

        assert result is API_RESPONSE
        get_conn.return_value.create_deidentify_template.assert_called_once_with(
            request=dict(
                parent=ORGANIZATION_PATH,
                deidentify_template=None,
                template_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_deidentify_template_with_project_id(self, get_conn):
        get_conn.return_value.create_deidentify_template.return_value = API_RESPONSE
        result = self.hook.create_deidentify_template(project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.create_deidentify_template.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                deidentify_template=None,
                template_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_deidentify_template_without_parent(self, _, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.create_deidentify_template()

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_dlp_job(self, get_conn):
        get_conn.return_value.create_dlp_job.return_value = API_RESPONSE
        result = self.hook.create_dlp_job(project_id=PROJECT_ID, wait_until_finished=False)

        assert result is API_RESPONSE
        get_conn.return_value.create_dlp_job.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                inspect_job=None,
                risk_job=None,
                job_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_dlp_job_without_project_id(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.create_dlp_job()

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_dlp_job_with_wait_until_finished(self, get_conn):
        job_for_create = DlpJob(name=DLP_JOB_PATH, state=DlpJob.JobState.PENDING)
        get_conn.return_value.create_dlp_job.return_value = job_for_create
        job_for_get = DlpJob(name=DLP_JOB_PATH, state=DlpJob.JobState.DONE)
        get_conn.return_value.get_dlp_job.return_value = job_for_get

        self.hook.create_dlp_job(project_id=PROJECT_ID)

        get_conn.return_value.get_dlp_job.assert_called_once_with(
            request=dict(
                name=DLP_JOB_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_inspect_template_with_org_id(self, get_conn, mock_project_id):
        get_conn.return_value.create_inspect_template.return_value = API_RESPONSE
        result = self.hook.create_inspect_template(organization_id=ORGANIZATION_ID)

        assert result is API_RESPONSE
        get_conn.return_value.create_inspect_template.assert_called_once_with(
            request=dict(
                parent=ORGANIZATION_PATH,
                inspect_template=None,
                template_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_inspect_template_with_project_id(self, get_conn):
        get_conn.return_value.create_inspect_template.return_value = API_RESPONSE
        result = self.hook.create_inspect_template(project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.create_inspect_template.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                inspect_template=None,
                template_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_inspect_template_without_parent(self, _, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.create_inspect_template()

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_job_trigger(self, get_conn):
        get_conn.return_value.create_job_trigger.return_value = API_RESPONSE
        result = self.hook.create_job_trigger(project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.create_job_trigger.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                job_trigger=None,
                trigger_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_job_trigger_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.create_job_trigger()

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_stored_info_type_with_org_id(self, get_conn, mock_project_id):
        get_conn.return_value.create_stored_info_type.return_value = API_RESPONSE
        result = self.hook.create_stored_info_type(organization_id=ORGANIZATION_ID)

        assert result is API_RESPONSE
        get_conn.return_value.create_stored_info_type.assert_called_once_with(
            request=dict(
                parent=ORGANIZATION_PATH,
                config=None,
                stored_info_type_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_stored_info_type_with_project_id(self, get_conn):
        get_conn.return_value.create_stored_info_type.return_value = API_RESPONSE
        result = self.hook.create_stored_info_type(project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.create_stored_info_type.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                config=None,
                stored_info_type_id=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_create_stored_info_type_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.create_stored_info_type()

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_deidentify_content(self, get_conn):
        get_conn.return_value.deidentify_content.return_value = API_RESPONSE
        result = self.hook.deidentify_content(project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.deidentify_content.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                deidentify_config=None,
                inspect_config=None,
                item=None,
                inspect_template_name=None,
                deidentify_template_name=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_deidentify_content_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.deidentify_content()

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_deidentify_template_with_org_id(self, get_conn, mock_project_id):
        self.hook.delete_deidentify_template(template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID)

        get_conn.return_value.delete_deidentify_template.assert_called_once_with(
            request=dict(
                name=DEIDENTIFY_TEMPLATE_ORGANIZATION_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_deidentify_template_with_project_id(self, get_conn):
        self.hook.delete_deidentify_template(template_id=TEMPLATE_ID, project_id=PROJECT_ID)

        get_conn.return_value.delete_deidentify_template.assert_called_once_with(
            request=dict(
                name=DEIDENTIFY_TEMPLATE_PROJECT_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_deidentify_template_without_template_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.delete_deidentify_template(template_id=None)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_deidentify_template_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.delete_deidentify_template(template_id=TEMPLATE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_dlp_job(self, get_conn):
        self.hook.delete_dlp_job(dlp_job_id=DLP_JOB_ID, project_id=PROJECT_ID)

        get_conn.return_value.delete_dlp_job.assert_called_once_with(
            request=dict(
                name=DLP_JOB_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_dlp_job_without_dlp_job_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.delete_dlp_job(dlp_job_id=None, project_id=PROJECT_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_dlp_job_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.delete_dlp_job(dlp_job_id=DLP_JOB_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_inspect_template_with_org_id(self, get_conn, mock_project_id):
        self.hook.delete_inspect_template(template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID)

        get_conn.return_value.delete_inspect_template.assert_called_once_with(
            request=dict(
                name=INSPECT_TEMPLATE_ORGANIZATION_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_inspect_template_with_project_id(self, get_conn):
        self.hook.delete_inspect_template(template_id=TEMPLATE_ID, project_id=PROJECT_ID)

        get_conn.return_value.delete_inspect_template.assert_called_once_with(
            request=dict(
                name=INSPECT_TEMPLATE_PROJECT_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_inspect_template_without_template_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.delete_inspect_template(template_id=None)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_inspect_template_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.delete_inspect_template(template_id=TEMPLATE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_job_trigger(self, get_conn):
        self.hook.delete_job_trigger(job_trigger_id=TRIGGER_ID, project_id=PROJECT_ID)

        get_conn.return_value.delete_job_trigger.assert_called_once_with(
            request=dict(
                name=JOB_TRIGGER_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_job_trigger_without_trigger_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.delete_job_trigger(job_trigger_id=None, project_id=PROJECT_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_job_trigger_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.delete_job_trigger(job_trigger_id=TRIGGER_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_stored_info_type_with_org_id(self, get_conn, mock_project_id):
        self.hook.delete_stored_info_type(
            stored_info_type_id=STORED_INFO_TYPE_ID, organization_id=ORGANIZATION_ID
        )

        get_conn.return_value.delete_stored_info_type.assert_called_once_with(
            request=dict(
                name=STORED_INFO_TYPE_ORGANIZATION_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_stored_info_type_with_project_id(self, get_conn):
        self.hook.delete_stored_info_type(stored_info_type_id=STORED_INFO_TYPE_ID, project_id=PROJECT_ID)

        get_conn.return_value.delete_stored_info_type.assert_called_once_with(
            request=dict(
                name=STORED_INFO_TYPE_PROJECT_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_stored_info_type_without_stored_info_type_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.delete_stored_info_type(stored_info_type_id=None)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_delete_stored_info_type_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.delete_stored_info_type(stored_info_type_id=STORED_INFO_TYPE_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_deidentify_template_with_org_id(self, get_conn, mock_project_id):
        get_conn.return_value.get_deidentify_template.return_value = API_RESPONSE
        result = self.hook.get_deidentify_template(template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID)

        assert result is API_RESPONSE
        get_conn.return_value.get_deidentify_template.assert_called_once_with(
            request=dict(
                name=DEIDENTIFY_TEMPLATE_ORGANIZATION_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_deidentify_template_with_project_id(self, get_conn):
        get_conn.return_value.get_deidentify_template.return_value = API_RESPONSE
        result = self.hook.get_deidentify_template(template_id=TEMPLATE_ID, project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.get_deidentify_template.assert_called_once_with(
            request=dict(
                name=DEIDENTIFY_TEMPLATE_PROJECT_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_deidentify_template_without_template_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.get_deidentify_template(template_id=None)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_deidentify_template_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.get_deidentify_template(template_id=TEMPLATE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_dlp_job(self, get_conn):
        get_conn.return_value.get_dlp_job.return_value = API_RESPONSE
        result = self.hook.get_dlp_job(dlp_job_id=DLP_JOB_ID, project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.get_dlp_job.assert_called_once_with(
            request=dict(
                name=DLP_JOB_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_dlp_job_without_dlp_job_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.get_dlp_job(dlp_job_id=None, project_id=PROJECT_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_dlp_job_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.get_dlp_job(dlp_job_id=DLP_JOB_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_inspect_template_with_org_id(self, get_conn, mock_project_id):
        get_conn.return_value.get_inspect_template.return_value = API_RESPONSE
        result = self.hook.get_inspect_template(template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID)

        assert result is API_RESPONSE
        get_conn.return_value.get_inspect_template.assert_called_once_with(
            request=dict(
                name=INSPECT_TEMPLATE_ORGANIZATION_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_inspect_template_with_project_id(self, get_conn):
        get_conn.return_value.get_inspect_template.return_value = API_RESPONSE
        result = self.hook.get_inspect_template(template_id=TEMPLATE_ID, project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.get_inspect_template.assert_called_once_with(
            request=dict(
                name=INSPECT_TEMPLATE_PROJECT_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_inspect_template_without_template_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.get_inspect_template(template_id=None)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_inspect_template_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.get_inspect_template(template_id=TEMPLATE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_job_trigger(self, get_conn):
        get_conn.return_value.get_job_trigger.return_value = API_RESPONSE
        result = self.hook.get_job_trigger(job_trigger_id=TRIGGER_ID, project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.get_job_trigger.assert_called_once_with(
            request=dict(
                name=JOB_TRIGGER_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_job_trigger_without_trigger_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.get_job_trigger(job_trigger_id=None, project_id=PROJECT_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_job_trigger_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.get_job_trigger(job_trigger_id=TRIGGER_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_stored_info_type_with_org_id(self, get_conn, mock_project_id):
        get_conn.return_value.get_stored_info_type.return_value = API_RESPONSE
        result = self.hook.get_stored_info_type(
            stored_info_type_id=STORED_INFO_TYPE_ID, organization_id=ORGANIZATION_ID
        )

        assert result is API_RESPONSE
        get_conn.return_value.get_stored_info_type.assert_called_once_with(
            request=dict(
                name=STORED_INFO_TYPE_ORGANIZATION_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_stored_info_type_with_project_id(self, get_conn):
        get_conn.return_value.get_stored_info_type.return_value = API_RESPONSE
        result = self.hook.get_stored_info_type(
            stored_info_type_id=STORED_INFO_TYPE_ID, project_id=PROJECT_ID
        )

        assert result is API_RESPONSE
        get_conn.return_value.get_stored_info_type.assert_called_once_with(
            request=dict(
                name=STORED_INFO_TYPE_PROJECT_PATH,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_stored_info_type_without_stored_info_type_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.get_stored_info_type(stored_info_type_id=None)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_get_stored_info_type_without_parent(self, mock_get_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.get_stored_info_type(stored_info_type_id=STORED_INFO_TYPE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_inspect_content(self, get_conn):
        get_conn.return_value.inspect_content.return_value = API_RESPONSE
        result = self.hook.inspect_content(project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.inspect_content.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                inspect_config=None,
                item=None,
                inspect_template_name=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_inspect_content_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.inspect_content()

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_deidentify_templates_with_org_id(self, get_conn, mock_project_id):
        result = self.hook.list_deidentify_templates(organization_id=ORGANIZATION_ID)

        assert isinstance(result, list)
        get_conn.return_value.list_deidentify_templates.assert_called_once_with(
            request=dict(
                parent=ORGANIZATION_PATH,
                page_size=None,
                order_by=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_deidentify_templates_with_project_id(self, get_conn):
        result = self.hook.list_deidentify_templates(project_id=PROJECT_ID)

        assert isinstance(result, list)
        get_conn.return_value.list_deidentify_templates.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                page_size=None,
                order_by=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_deidentify_templates_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.list_deidentify_templates()

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_dlp_jobs(self, get_conn):
        result = self.hook.list_dlp_jobs(project_id=PROJECT_ID)

        assert isinstance(result, list)
        get_conn.return_value.list_dlp_jobs.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                filter=None,
                page_size=None,
                type_=None,
                order_by=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_dlp_jobs_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.list_dlp_jobs()

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_info_types(self, get_conn):
        get_conn.return_value.list_info_types.return_value = API_RESPONSE
        result = self.hook.list_info_types()

        assert result is API_RESPONSE
        get_conn.return_value.list_info_types.assert_called_once_with(
            request=dict(
                language_code=None,
                filter=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_inspect_templates_with_org_id(self, get_conn, mock_project_id):
        result = self.hook.list_inspect_templates(organization_id=ORGANIZATION_ID)

        assert isinstance(result, list)
        get_conn.return_value.list_inspect_templates.assert_called_once_with(
            request=dict(
                parent=ORGANIZATION_PATH,
                page_size=None,
                order_by=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_inspect_templates_with_project_id(self, get_conn):
        result = self.hook.list_inspect_templates(project_id=PROJECT_ID)

        assert isinstance(result, list)
        get_conn.return_value.list_inspect_templates.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                page_size=None,
                order_by=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_inspect_templates_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.list_inspect_templates()

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_job_triggers(self, get_conn):
        result = self.hook.list_job_triggers(project_id=PROJECT_ID)

        assert isinstance(result, list)
        get_conn.return_value.list_job_triggers.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                page_size=None,
                order_by=None,
                filter=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_job_triggers_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.list_job_triggers()

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_stored_info_types_with_org_id(self, get_conn, mock_project_id):
        result = self.hook.list_stored_info_types(organization_id=ORGANIZATION_ID)

        assert isinstance(result, list)
        get_conn.return_value.list_stored_info_types.assert_called_once_with(
            request=dict(
                parent=ORGANIZATION_PATH,
                page_size=None,
                order_by=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_stored_info_types_with_project_id(self, get_conn):
        result = self.hook.list_stored_info_types(project_id=PROJECT_ID)

        assert isinstance(result, list)
        get_conn.return_value.list_stored_info_types.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                page_size=None,
                order_by=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_list_stored_info_types_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.list_stored_info_types()

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_redact_image(self, get_conn):
        get_conn.return_value.redact_image.return_value = API_RESPONSE
        result = self.hook.redact_image(project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.redact_image.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                inspect_config=None,
                image_redaction_configs=None,
                include_findings=None,
                byte_item=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_redact_image_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.redact_image()

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_reidentify_content(self, get_conn):
        get_conn.return_value.reidentify_content.return_value = API_RESPONSE
        result = self.hook.reidentify_content(project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.reidentify_content.assert_called_once_with(
            request=dict(
                parent=PROJECT_PATH,
                reidentify_config=None,
                inspect_config=None,
                item=None,
                inspect_template_name=None,
                reidentify_template_name=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_reidentify_content_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.reidentify_content()

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_deidentify_template_with_org_id(self, get_conn, mock_project_id):
        get_conn.return_value.update_deidentify_template.return_value = API_RESPONSE
        result = self.hook.update_deidentify_template(
            template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID
        )

        assert result is API_RESPONSE
        get_conn.return_value.update_deidentify_template.assert_called_once_with(
            request=dict(
                name=DEIDENTIFY_TEMPLATE_ORGANIZATION_PATH,
                deidentify_template=None,
                update_mask=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_deidentify_template_with_project_id(self, get_conn):
        get_conn.return_value.update_deidentify_template.return_value = API_RESPONSE
        result = self.hook.update_deidentify_template(template_id=TEMPLATE_ID, project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.update_deidentify_template.assert_called_once_with(
            request=dict(
                name=DEIDENTIFY_TEMPLATE_PROJECT_PATH,
                deidentify_template=None,
                update_mask=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_deidentify_template_without_template_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.update_deidentify_template(template_id=None, organization_id=ORGANIZATION_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_deidentify_template_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.update_deidentify_template(template_id=TEMPLATE_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_inspect_template_with_org_id(self, get_conn, mock_project_id):
        get_conn.return_value.update_inspect_template.return_value = API_RESPONSE
        result = self.hook.update_inspect_template(template_id=TEMPLATE_ID, organization_id=ORGANIZATION_ID)

        assert result is API_RESPONSE
        get_conn.return_value.update_inspect_template.assert_called_once_with(
            request=dict(
                name=INSPECT_TEMPLATE_ORGANIZATION_PATH,
                inspect_template=None,
                update_mask=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_inspect_template_with_project_id(self, get_conn):
        get_conn.return_value.update_inspect_template.return_value = API_RESPONSE
        result = self.hook.update_inspect_template(template_id=TEMPLATE_ID, project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.update_inspect_template.assert_called_once_with(
            request=dict(
                name=INSPECT_TEMPLATE_PROJECT_PATH,
                inspect_template=None,
                update_mask=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_inspect_template_without_template_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.update_inspect_template(template_id=None, organization_id=ORGANIZATION_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_inspect_template_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.update_inspect_template(template_id=TEMPLATE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_job_trigger(self, get_conn):
        get_conn.return_value.update_job_trigger.return_value = API_RESPONSE
        result = self.hook.update_job_trigger(job_trigger_id=TRIGGER_ID, project_id=PROJECT_ID)

        assert result is API_RESPONSE
        get_conn.return_value.update_job_trigger.assert_called_once_with(
            name=JOB_TRIGGER_PATH,
            job_trigger=None,
            update_mask=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_job_trigger_without_job_trigger_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.update_job_trigger(job_trigger_id=None, project_id=PROJECT_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_job_trigger_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.update_job_trigger(job_trigger_id=TRIGGER_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_stored_info_type_with_org_id(self, get_conn, mock_project_id):
        get_conn.return_value.update_stored_info_type.return_value = API_RESPONSE
        result = self.hook.update_stored_info_type(
            stored_info_type_id=STORED_INFO_TYPE_ID, organization_id=ORGANIZATION_ID
        )

        assert result is API_RESPONSE
        get_conn.return_value.update_stored_info_type.assert_called_once_with(
            request=dict(
                name=STORED_INFO_TYPE_ORGANIZATION_PATH,
                config=None,
                update_mask=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_stored_info_type_with_project_id(self, get_conn):
        get_conn.return_value.update_stored_info_type.return_value = API_RESPONSE
        result = self.hook.update_stored_info_type(
            stored_info_type_id=STORED_INFO_TYPE_ID, project_id=PROJECT_ID
        )

        assert result is API_RESPONSE
        get_conn.return_value.update_stored_info_type.assert_called_once_with(
            request=dict(
                name=STORED_INFO_TYPE_PROJECT_PATH,
                config=None,
                update_mask=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_stored_info_type_without_stored_info_type_id(self, _):
        with pytest.raises(AirflowException):
            self.hook.update_stored_info_type(stored_info_type_id=None, organization_id=ORGANIZATION_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dlp.CloudDLPHook.get_conn")
    def test_update_stored_info_type_without_parent(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException):
            self.hook.update_stored_info_type(stored_info_type_id=STORED_INFO_TYPE_ID)
