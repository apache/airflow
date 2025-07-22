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

BUILD_BASE_LINK = "/cloud-build"

BUILD_LINK = BUILD_BASE_LINK + "/builds;region={region}/{build_id}?project={project_id}"

BUILD_LIST_LINK = BUILD_BASE_LINK + "/builds;region={region}?project={project_id}"

BUILD_TRIGGERS_LIST_LINK = BUILD_BASE_LINK + "/triggers;region={region}?project={project_id}"

BUILD_TRIGGER_DETAILS_LINK = (
    BUILD_BASE_LINK + "/triggers;region={region}/edit/{trigger_id}?project={project_id}"
)


class CloudBuildLink(BaseGoogleLink):
    """Helper class for constructing Cloud Build link."""

    name = "Cloud Build Details"
    key = "cloud_build_key"
    format_str = BUILD_LINK


class CloudBuildListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Build List link."""

    name = "Cloud Builds List"
    key = "cloud_build_list_key"
    format_str = BUILD_LIST_LINK


class CloudBuildTriggersListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Build Triggers List link."""

    name = "Cloud Build Triggers List"
    key = "cloud_build_triggers_list_key"
    format_str = BUILD_TRIGGERS_LIST_LINK


class CloudBuildTriggerDetailsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Build Trigger Details link."""

    name = "Cloud Build Triggers Details"
    key = "cloud_build_triggers_details_key"
    format_str = BUILD_TRIGGER_DETAILS_LINK
