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

"""Tests for Cloud Data Loss Prevention links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.data_loss_prevention import (
    DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK,
    DLP_DEIDENTIFY_TEMPLATES_LIST_LINK,
    DLP_INFO_TYPE_DETAILS_LINK,
    DLP_INFO_TYPES_LIST_LINK,
    DLP_INSPECT_TEMPLATE_DETAILS_LINK,
    DLP_INSPECT_TEMPLATES_LIST_LINK,
    DLP_JOB_DETAILS_LINK,
    DLP_JOB_TRIGGER_DETAILS_LINK,
    DLP_JOB_TRIGGER_LIST_LINK,
    DLP_JOBS_LIST_LINK,
    DLP_POSSIBLE_INFO_TYPES_LIST_LINK,
    CloudDLPDeidentifyTemplateDetailsLink,
    CloudDLPDeidentifyTemplatesListLink,
    CloudDLPInfoTypeDetailsLink,
    CloudDLPInfoTypesListLink,
    CloudDLPInspectTemplateDetailsLink,
    CloudDLPInspectTemplatesListLink,
    CloudDLPJobDetailsLink,
    CloudDLPJobsListLink,
    CloudDLPJobTriggerDetailsLink,
    CloudDLPJobTriggersListLink,
    CloudDLPPossibleInfoTypesListLink,
)

TEST_INFO_TYPE_NAME = "test-info-type-name"
TEST_JOB_NAME = "test-job-name"
TEST_PROJECT_ID = "test-project-id"
TEST_TEMPLATE_NAME = "test-template-name"
TEST_TRIGGER_NAME = "test-trigger-name"


class TestCloudDLPDeidentifyTemplatesListLink:
    def test_class_attributes(self):
        assert CloudDLPDeidentifyTemplatesListLink.key == "cloud_dlp_deidentify_templates_list_key"
        assert CloudDLPDeidentifyTemplatesListLink.name == "Cloud DLP Deidentify Templates List"
        assert CloudDLPDeidentifyTemplatesListLink.format_str == DLP_DEIDENTIFY_TEMPLATES_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPDeidentifyTemplatesListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_deidentify_templates_list_key",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDLPDeidentifyTemplatesListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``DLP_DEIDENTIFY_TEMPLATES_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_DEIDENTIFY_TEMPLATES_LIST_LINK.format(project_id=TEST_PROJECT_ID)


class TestCloudDLPDeidentifyTemplateDetailsLink:
    def test_class_attributes(self):
        assert CloudDLPDeidentifyTemplateDetailsLink.key == "cloud_dlp_deidentify_template_details_key"
        assert CloudDLPDeidentifyTemplateDetailsLink.name == "Cloud DLP Deidentify Template Details"
        assert CloudDLPDeidentifyTemplateDetailsLink.format_str == DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPDeidentifyTemplateDetailsLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            template_name=TEST_TEMPLATE_NAME,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_deidentify_template_details_key",
            value={"project_id": TEST_PROJECT_ID, "template_name": TEST_TEMPLATE_NAME},
        )

    def test_format_link(self):
        link = CloudDLPDeidentifyTemplateDetailsLink()

        result = link._format_link(project_id=TEST_PROJECT_ID, template_name=TEST_TEMPLATE_NAME)

        # ``DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK.format(
            project_id=TEST_PROJECT_ID, template_name=TEST_TEMPLATE_NAME
        )


class TestCloudDLPJobTriggersListLink:
    def test_class_attributes(self):
        assert CloudDLPJobTriggersListLink.key == "cloud_dlp_job_triggers_list_key"
        assert CloudDLPJobTriggersListLink.name == "Cloud DLP Job Triggers List"
        assert CloudDLPJobTriggersListLink.format_str == DLP_JOB_TRIGGER_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPJobTriggersListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_job_triggers_list_key",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDLPJobTriggersListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``DLP_JOB_TRIGGER_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_JOB_TRIGGER_LIST_LINK.format(project_id=TEST_PROJECT_ID)


class TestCloudDLPJobTriggerDetailsLink:
    def test_class_attributes(self):
        assert CloudDLPJobTriggerDetailsLink.key == "cloud_dlp_job_trigger_details_key"
        assert CloudDLPJobTriggerDetailsLink.name == "Cloud DLP Job Triggers Details"
        assert CloudDLPJobTriggerDetailsLink.format_str == DLP_JOB_TRIGGER_DETAILS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPJobTriggerDetailsLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            trigger_name=TEST_TRIGGER_NAME,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_job_trigger_details_key",
            value={"project_id": TEST_PROJECT_ID, "trigger_name": TEST_TRIGGER_NAME},
        )

    def test_format_link(self):
        link = CloudDLPJobTriggerDetailsLink()

        result = link._format_link(project_id=TEST_PROJECT_ID, trigger_name=TEST_TRIGGER_NAME)

        # ``DLP_JOB_TRIGGER_DETAILS_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_JOB_TRIGGER_DETAILS_LINK.format(
            project_id=TEST_PROJECT_ID, trigger_name=TEST_TRIGGER_NAME
        )


class TestCloudDLPJobsListLink:
    def test_class_attributes(self):
        assert CloudDLPJobsListLink.key == "cloud_dlp_jobs_list_key"
        assert CloudDLPJobsListLink.name == "Cloud DLP Jobs List"
        assert CloudDLPJobsListLink.format_str == DLP_JOBS_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPJobsListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_jobs_list_key",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDLPJobsListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``DLP_JOBS_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_JOBS_LIST_LINK.format(project_id=TEST_PROJECT_ID)


class TestCloudDLPJobDetailsLink:
    def test_class_attributes(self):
        assert CloudDLPJobDetailsLink.key == "cloud_dlp_job_details_key"
        assert CloudDLPJobDetailsLink.name == "Cloud DLP Job Details"
        assert CloudDLPJobDetailsLink.format_str == DLP_JOB_DETAILS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPJobDetailsLink.persist(
            context=mock_context,
            job_name=TEST_JOB_NAME,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_job_details_key",
            value={"job_name": TEST_JOB_NAME, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDLPJobDetailsLink()

        result = link._format_link(job_name=TEST_JOB_NAME, project_id=TEST_PROJECT_ID)

        # ``DLP_JOB_DETAILS_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_JOB_DETAILS_LINK.format(job_name=TEST_JOB_NAME, project_id=TEST_PROJECT_ID)


class TestCloudDLPInspectTemplatesListLink:
    def test_class_attributes(self):
        assert CloudDLPInspectTemplatesListLink.key == "cloud_dlp_inspect_templates_list_key"
        assert CloudDLPInspectTemplatesListLink.name == "Cloud DLP Inspect Templates List"
        assert CloudDLPInspectTemplatesListLink.format_str == DLP_INSPECT_TEMPLATES_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPInspectTemplatesListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_inspect_templates_list_key",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDLPInspectTemplatesListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``DLP_INSPECT_TEMPLATES_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_INSPECT_TEMPLATES_LIST_LINK.format(project_id=TEST_PROJECT_ID)


class TestCloudDLPInspectTemplateDetailsLink:
    def test_class_attributes(self):
        assert CloudDLPInspectTemplateDetailsLink.key == "cloud_dlp_inspect_template_details_key"
        assert CloudDLPInspectTemplateDetailsLink.name == "Cloud DLP Inspect Template Details"
        assert CloudDLPInspectTemplateDetailsLink.format_str == DLP_INSPECT_TEMPLATE_DETAILS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPInspectTemplateDetailsLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            template_name=TEST_TEMPLATE_NAME,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_inspect_template_details_key",
            value={"project_id": TEST_PROJECT_ID, "template_name": TEST_TEMPLATE_NAME},
        )

    def test_format_link(self):
        link = CloudDLPInspectTemplateDetailsLink()

        result = link._format_link(project_id=TEST_PROJECT_ID, template_name=TEST_TEMPLATE_NAME)

        # ``DLP_INSPECT_TEMPLATE_DETAILS_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_INSPECT_TEMPLATE_DETAILS_LINK.format(
            project_id=TEST_PROJECT_ID, template_name=TEST_TEMPLATE_NAME
        )


class TestCloudDLPInfoTypesListLink:
    def test_class_attributes(self):
        assert CloudDLPInfoTypesListLink.key == "cloud_dlp_info_types_list_key"
        assert CloudDLPInfoTypesListLink.name == "Cloud DLP Info Types List"
        assert CloudDLPInfoTypesListLink.format_str == DLP_INFO_TYPES_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPInfoTypesListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_info_types_list_key",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDLPInfoTypesListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``DLP_INFO_TYPES_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_INFO_TYPES_LIST_LINK.format(project_id=TEST_PROJECT_ID)


class TestCloudDLPInfoTypeDetailsLink:
    def test_class_attributes(self):
        assert CloudDLPInfoTypeDetailsLink.key == "cloud_dlp_info_type_details_key"
        assert CloudDLPInfoTypeDetailsLink.name == "Cloud DLP Info Type Details"
        assert CloudDLPInfoTypeDetailsLink.format_str == DLP_INFO_TYPE_DETAILS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPInfoTypeDetailsLink.persist(
            context=mock_context,
            info_type_name=TEST_INFO_TYPE_NAME,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_info_type_details_key",
            value={"info_type_name": TEST_INFO_TYPE_NAME, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDLPInfoTypeDetailsLink()

        result = link._format_link(info_type_name=TEST_INFO_TYPE_NAME, project_id=TEST_PROJECT_ID)

        # ``DLP_INFO_TYPE_DETAILS_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_INFO_TYPE_DETAILS_LINK.format(
            info_type_name=TEST_INFO_TYPE_NAME, project_id=TEST_PROJECT_ID
        )


class TestCloudDLPPossibleInfoTypesListLink:
    def test_class_attributes(self):
        assert CloudDLPPossibleInfoTypesListLink.key == "cloud_dlp_possible_info_types_list_key"
        assert CloudDLPPossibleInfoTypesListLink.name == "Cloud DLP Possible Info Types List"
        assert CloudDLPPossibleInfoTypesListLink.format_str == DLP_POSSIBLE_INFO_TYPES_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudDLPPossibleInfoTypesListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_dlp_possible_info_types_list_key",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudDLPPossibleInfoTypesListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``DLP_POSSIBLE_INFO_TYPES_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DLP_POSSIBLE_INFO_TYPES_LIST_LINK.format(project_id=TEST_PROJECT_ID)
