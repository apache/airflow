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

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.data_loss_prevention import (
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

EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATES_LIST_LINK_NAME = "Cloud DLP Deidentify Templates List"
EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATES_LIST_LINK_KEY = "cloud_dlp_deidentify_templates_list_key"
EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATES_LIST_LINK_FORMAT_STR = (
    BASE_LINK + "/security/dlp/landing/configuration/templates/deidentify?project={project_id}"
)
EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK_NAME = "Cloud DLP Deidentify Template Details"
EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK_KEY = "cloud_dlp_deidentify_template_details_key"
EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK_FORMAT_STR = (
    BASE_LINK
    + "/security/dlp/projects/{project_id}/locations/global/deidentifyTemplates/{template_name}?project={project_id}"
)
EXPECTED_CLOUD_DLP_JOB_TRIGGERS_LIST_LINK_NAME = "Cloud DLP Job Triggers List"
EXPECTED_CLOUD_DLP_JOB_TRIGGERS_LIST_LINK_KEY = "cloud_dlp_job_triggers_list_key"
EXPECTED_CLOUD_DLP_JOB_TRIGGERS_LIST_LINK_FORMAT_STR = (
    BASE_LINK + "/security/dlp/landing/inspection/triggers?project={project_id}"
)
EXPECTED_CLOUD_DLP_JOB_TRIGGER_DETAILS_LINK_NAME = "Cloud DLP Job Triggers Details"
EXPECTED_CLOUD_DLP_JOB_TRIGGER_DETAILS_LINK_KEY = "cloud_dlp_job_trigger_details_key"
EXPECTED_CLOUD_DLP_JOB_TRIGGER_DETAILS_LINK_FORMAT_STR = (
    BASE_LINK
    + "/security/dlp/projects/{project_id}/locations/global/jobTriggers/{trigger_name}?project={project_id}"
)
EXPECTED_CLOUD_DLP_JOBS_LIST_LINK_NAME = "Cloud DLP Jobs List"
EXPECTED_CLOUD_DLP_JOBS_LIST_LINK_KEY = "cloud_dlp_jobs_list_key"
EXPECTED_CLOUD_DLP_JOBS_LIST_LINK_FORMAT_STR = (
    BASE_LINK + "/security/dlp/landing/inspection/jobs?project={project_id}"
)
EXPECTED_CLOUD_DLP_JOB_DETAILS_LINK_NAME = "Cloud DLP Job Details"
EXPECTED_CLOUD_DLP_JOB_DETAILS_LINK_KEY = "cloud_dlp_job_details_key"
EXPECTED_CLOUD_DLP_JOB_DETAILS_LINK_FORMAT_STR = (
    BASE_LINK + "/security/dlp/projects/{project_id}/locations/global/dlpJobs/{job_name}?project={project_id}"
)
EXPECTED_CLOUD_DLP_INSPECT_TEMPLATES_LIST_LINK_NAME = "Cloud DLP Inspect Templates List"
EXPECTED_CLOUD_DLP_INSPECT_TEMPLATES_LIST_LINK_KEY = "cloud_dlp_inspect_templates_list_key"
EXPECTED_CLOUD_DLP_INSPECT_TEMPLATES_LIST_LINK_FORMAT_STR = (
    BASE_LINK + "/security/dlp/landing/configuration/templates/inspect?project={project_id}"
)
EXPECTED_CLOUD_DLP_INSPECT_TEMPLATE_DETAILS_LINK_NAME = "Cloud DLP Inspect Template Details"
EXPECTED_CLOUD_DLP_INSPECT_TEMPLATE_DETAILS_LINK_KEY = "cloud_dlp_inspect_template_details_key"
EXPECTED_CLOUD_DLP_INSPECT_TEMPLATE_DETAILS_LINK_FORMAT_STR = (
    BASE_LINK
    + "/security/dlp/projects/{project_id}/locations/global/inspectTemplates/{template_name}?project={project_id}"
)
EXPECTED_CLOUD_DLP_INFO_TYPES_LIST_LINK_NAME = "Cloud DLP Info Types List"
EXPECTED_CLOUD_DLP_INFO_TYPES_LIST_LINK_KEY = "cloud_dlp_info_types_list_key"
EXPECTED_CLOUD_DLP_INFO_TYPES_LIST_LINK_FORMAT_STR = (
    BASE_LINK + "/security/dlp/landing/configuration/infoTypes/stored?cloudshell=false&project={project_id}"
)
EXPECTED_CLOUD_DLP_INFO_TYPE_DETAILS_LINK_NAME = "Cloud DLP Info Type Details"
EXPECTED_CLOUD_DLP_INFO_TYPE_DETAILS_LINK_KEY = "cloud_dlp_info_type_details_key"
EXPECTED_CLOUD_DLP_INFO_TYPE_DETAILS_LINK_FORMAT_STR = (
    BASE_LINK
    + "/security/dlp/projects/{project_id}/locations/global/storedInfoTypes/{info_type_name}?project={project_id}"
)
EXPECTED_CLOUD_DLP_POSSIBLE_INFO_TYPES_LIST_LINK_NAME = "Cloud DLP Possible Info Types List"
EXPECTED_CLOUD_DLP_POSSIBLE_INFO_TYPES_LIST_LINK_KEY = "cloud_dlp_possible_info_types_list_key"
EXPECTED_CLOUD_DLP_POSSIBLE_INFO_TYPES_LIST_LINK_FORMAT_STR = (
    BASE_LINK + "/security/dlp/landing/configuration/infoTypes/built-in?project={project_id}"
)


class TestCloudDLPDeidentifyTemplatesListLink:
    def test_class_attributes(self):
        assert (
            CloudDLPDeidentifyTemplatesListLink.name == EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATES_LIST_LINK_NAME
        )
        assert (
            CloudDLPDeidentifyTemplatesListLink.key == EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATES_LIST_LINK_KEY
        )
        assert (
            CloudDLPDeidentifyTemplatesListLink.format_str
            == EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATES_LIST_LINK_FORMAT_STR
        )


class TestCloudDLPDeidentifyTemplateDetailsLink:
    def test_class_attributes(self):
        assert (
            CloudDLPDeidentifyTemplateDetailsLink.name
            == EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK_NAME
        )
        assert (
            CloudDLPDeidentifyTemplateDetailsLink.key
            == EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK_KEY
        )
        assert (
            CloudDLPDeidentifyTemplateDetailsLink.format_str
            == EXPECTED_CLOUD_DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK_FORMAT_STR
        )


class TestCloudDLPJobTriggersListLink:
    def test_class_attributes(self):
        assert CloudDLPJobTriggersListLink.name == EXPECTED_CLOUD_DLP_JOB_TRIGGERS_LIST_LINK_NAME
        assert CloudDLPJobTriggersListLink.key == EXPECTED_CLOUD_DLP_JOB_TRIGGERS_LIST_LINK_KEY
        assert CloudDLPJobTriggersListLink.format_str == EXPECTED_CLOUD_DLP_JOB_TRIGGERS_LIST_LINK_FORMAT_STR


class TestCloudDLPJobTriggerDetailsLink:
    def test_class_attributes(self):
        assert CloudDLPJobTriggerDetailsLink.name == EXPECTED_CLOUD_DLP_JOB_TRIGGER_DETAILS_LINK_NAME
        assert CloudDLPJobTriggerDetailsLink.key == EXPECTED_CLOUD_DLP_JOB_TRIGGER_DETAILS_LINK_KEY
        assert (
            CloudDLPJobTriggerDetailsLink.format_str == EXPECTED_CLOUD_DLP_JOB_TRIGGER_DETAILS_LINK_FORMAT_STR
        )


class TestCloudDLPJobsListLink:
    def test_class_attributes(self):
        assert CloudDLPJobsListLink.name == EXPECTED_CLOUD_DLP_JOBS_LIST_LINK_NAME
        assert CloudDLPJobsListLink.key == EXPECTED_CLOUD_DLP_JOBS_LIST_LINK_KEY
        assert CloudDLPJobsListLink.format_str == EXPECTED_CLOUD_DLP_JOBS_LIST_LINK_FORMAT_STR


class TestCloudDLPJobDetailsLink:
    def test_class_attributes(self):
        assert CloudDLPJobDetailsLink.name == EXPECTED_CLOUD_DLP_JOB_DETAILS_LINK_NAME
        assert CloudDLPJobDetailsLink.key == EXPECTED_CLOUD_DLP_JOB_DETAILS_LINK_KEY
        assert CloudDLPJobDetailsLink.format_str == EXPECTED_CLOUD_DLP_JOB_DETAILS_LINK_FORMAT_STR


class TestCloudDLPInspectTemplatesListLink:
    def test_class_attributes(self):
        assert CloudDLPInspectTemplatesListLink.name == EXPECTED_CLOUD_DLP_INSPECT_TEMPLATES_LIST_LINK_NAME
        assert CloudDLPInspectTemplatesListLink.key == EXPECTED_CLOUD_DLP_INSPECT_TEMPLATES_LIST_LINK_KEY
        assert (
            CloudDLPInspectTemplatesListLink.format_str
            == EXPECTED_CLOUD_DLP_INSPECT_TEMPLATES_LIST_LINK_FORMAT_STR
        )


class TestCloudDLPInspectTemplateDetailsLink:
    def test_class_attributes(self):
        assert (
            CloudDLPInspectTemplateDetailsLink.name == EXPECTED_CLOUD_DLP_INSPECT_TEMPLATE_DETAILS_LINK_NAME
        )
        assert CloudDLPInspectTemplateDetailsLink.key == EXPECTED_CLOUD_DLP_INSPECT_TEMPLATE_DETAILS_LINK_KEY
        assert (
            CloudDLPInspectTemplateDetailsLink.format_str
            == EXPECTED_CLOUD_DLP_INSPECT_TEMPLATE_DETAILS_LINK_FORMAT_STR
        )


class TestCloudDLPInfoTypesListLink:
    def test_class_attributes(self):
        assert CloudDLPInfoTypesListLink.name == EXPECTED_CLOUD_DLP_INFO_TYPES_LIST_LINK_NAME
        assert CloudDLPInfoTypesListLink.key == EXPECTED_CLOUD_DLP_INFO_TYPES_LIST_LINK_KEY
        assert CloudDLPInfoTypesListLink.format_str == EXPECTED_CLOUD_DLP_INFO_TYPES_LIST_LINK_FORMAT_STR


class TestCloudDLPInfoTypeDetailsLink:
    def test_class_attributes(self):
        assert CloudDLPInfoTypeDetailsLink.name == EXPECTED_CLOUD_DLP_INFO_TYPE_DETAILS_LINK_NAME
        assert CloudDLPInfoTypeDetailsLink.key == EXPECTED_CLOUD_DLP_INFO_TYPE_DETAILS_LINK_KEY
        assert CloudDLPInfoTypeDetailsLink.format_str == EXPECTED_CLOUD_DLP_INFO_TYPE_DETAILS_LINK_FORMAT_STR


class TestCloudDLPPossibleInfoTypesListLink:
    def test_class_attributes(self):
        assert CloudDLPPossibleInfoTypesListLink.name == EXPECTED_CLOUD_DLP_POSSIBLE_INFO_TYPES_LIST_LINK_NAME
        assert CloudDLPPossibleInfoTypesListLink.key == EXPECTED_CLOUD_DLP_POSSIBLE_INFO_TYPES_LIST_LINK_KEY
        assert (
            CloudDLPPossibleInfoTypesListLink.format_str
            == EXPECTED_CLOUD_DLP_POSSIBLE_INFO_TYPES_LIST_LINK_FORMAT_STR
        )
