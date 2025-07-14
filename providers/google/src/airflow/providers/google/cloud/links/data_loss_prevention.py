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

from airflow.providers.google.cloud.links.base import BaseGoogleLink

BASE_LINK = "https://console.cloud.google.com"

DLP_BASE_LINK = BASE_LINK + "/security/dlp"

DLP_DEIDENTIFY_TEMPLATES_LIST_LINK = (
    DLP_BASE_LINK + "/landing/configuration/templates/deidentify?project={project_id}"
)
DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK = (
    DLP_BASE_LINK
    + "/projects/{project_id}/locations/global/deidentifyTemplates/{template_name}?project={project_id}"
)

DLP_JOB_TRIGGER_LIST_LINK = DLP_BASE_LINK + "/landing/inspection/triggers?project={project_id}"
DLP_JOB_TRIGGER_DETAILS_LINK = (
    DLP_BASE_LINK + "/projects/{project_id}/locations/global/jobTriggers/{trigger_name}?project={project_id}"
)

DLP_JOBS_LIST_LINK = DLP_BASE_LINK + "/landing/inspection/jobs?project={project_id}"
DLP_JOB_DETAILS_LINK = (
    DLP_BASE_LINK + "/projects/{project_id}/locations/global/dlpJobs/{job_name}?project={project_id}"
)

DLP_INSPECT_TEMPLATES_LIST_LINK = (
    DLP_BASE_LINK + "/landing/configuration/templates/inspect?project={project_id}"
)
DLP_INSPECT_TEMPLATE_DETAILS_LINK = (
    DLP_BASE_LINK
    + "/projects/{project_id}/locations/global/inspectTemplates/{template_name}?project={project_id}"
)

DLP_INFO_TYPES_LIST_LINK = (
    DLP_BASE_LINK + "/landing/configuration/infoTypes/stored?cloudshell=false&project={project_id}"
)
DLP_INFO_TYPE_DETAILS_LINK = (
    DLP_BASE_LINK
    + "/projects/{project_id}/locations/global/storedInfoTypes/{info_type_name}?project={project_id}"
)
DLP_POSSIBLE_INFO_TYPES_LIST_LINK = (
    DLP_BASE_LINK + "/landing/configuration/infoTypes/built-in?project={project_id}"
)


class CloudDLPDeidentifyTemplatesListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Deidentify Templates List"
    key = "cloud_dlp_deidentify_templates_list_key"
    format_str = DLP_DEIDENTIFY_TEMPLATES_LIST_LINK


class CloudDLPDeidentifyTemplateDetailsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Deidentify Template Details"
    key = "cloud_dlp_deidentify_template_details_key"
    format_str = DLP_DEIDENTIFY_TEMPLATE_DETAILS_LINK


class CloudDLPJobTriggersListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Job Triggers List"
    key = "cloud_dlp_job_triggers_list_key"
    format_str = DLP_JOB_TRIGGER_LIST_LINK


class CloudDLPJobTriggerDetailsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Job Triggers Details"
    key = "cloud_dlp_job_trigger_details_key"
    format_str = DLP_JOB_TRIGGER_DETAILS_LINK


class CloudDLPJobsListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Jobs List"
    key = "cloud_dlp_jobs_list_key"
    format_str = DLP_JOBS_LIST_LINK


class CloudDLPJobDetailsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Job Details"
    key = "cloud_dlp_job_details_key"
    format_str = DLP_JOB_DETAILS_LINK


class CloudDLPInspectTemplatesListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Inspect Templates List"
    key = "cloud_dlp_inspect_templates_list_key"
    format_str = DLP_INSPECT_TEMPLATES_LIST_LINK


class CloudDLPInspectTemplateDetailsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Inspect Template Details"
    key = "cloud_dlp_inspect_template_details_key"
    format_str = DLP_INSPECT_TEMPLATE_DETAILS_LINK


class CloudDLPInfoTypesListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Info Types List"
    key = "cloud_dlp_info_types_list_key"
    format_str = DLP_INFO_TYPES_LIST_LINK


class CloudDLPInfoTypeDetailsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Info Type Details"
    key = "cloud_dlp_info_type_details_key"
    format_str = DLP_INFO_TYPE_DETAILS_LINK


class CloudDLPPossibleInfoTypesListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Data Loss Prevention link."""

    name = "Cloud DLP Possible Info Types List"
    key = "cloud_dlp_possible_info_types_list_key"
    format_str = DLP_POSSIBLE_INFO_TYPES_LIST_LINK
