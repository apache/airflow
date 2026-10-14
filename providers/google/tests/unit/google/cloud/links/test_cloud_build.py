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

from airflow.providers.google.cloud.links.cloud_build import (
    CloudBuildLink,
    CloudBuildListLink,
    CloudBuildTriggerDetailsLink,
    CloudBuildTriggersListLink,
)

EXPECTED_CLOUD_BUILD_LINK_NAME = "Cloud Build Details"
EXPECTED_CLOUD_BUILD_LINK_KEY = "cloud_build_key"
EXPECTED_CLOUD_BUILD_LINK_FORMAT_STR = "/cloud-build/builds;region={region}/{build_id}?project={project_id}"
EXPECTED_CLOUD_BUILD_LIST_LINK_NAME = "Cloud Builds List"
EXPECTED_CLOUD_BUILD_LIST_LINK_KEY = "cloud_build_list_key"
EXPECTED_CLOUD_BUILD_LIST_LINK_FORMAT_STR = "/cloud-build/builds;region={region}?project={project_id}"
EXPECTED_CLOUD_BUILD_TRIGGERS_LIST_LINK_NAME = "Cloud Build Triggers List"
EXPECTED_CLOUD_BUILD_TRIGGERS_LIST_LINK_KEY = "cloud_build_triggers_list_key"
EXPECTED_CLOUD_BUILD_TRIGGERS_LIST_LINK_FORMAT_STR = (
    "/cloud-build/triggers;region={region}?project={project_id}"
)
EXPECTED_CLOUD_BUILD_TRIGGER_DETAILS_LINK_NAME = "Cloud Build Triggers Details"
EXPECTED_CLOUD_BUILD_TRIGGER_DETAILS_LINK_KEY = "cloud_build_triggers_details_key"
EXPECTED_CLOUD_BUILD_TRIGGER_DETAILS_LINK_FORMAT_STR = (
    "/cloud-build/triggers;region={region}/edit/{trigger_id}?project={project_id}"
)


class TestCloudBuildLink:
    def test_class_attributes(self):
        assert CloudBuildLink.name == EXPECTED_CLOUD_BUILD_LINK_NAME
        assert CloudBuildLink.key == EXPECTED_CLOUD_BUILD_LINK_KEY
        assert CloudBuildLink.format_str == EXPECTED_CLOUD_BUILD_LINK_FORMAT_STR


class TestCloudBuildListLink:
    def test_class_attributes(self):
        assert CloudBuildListLink.name == EXPECTED_CLOUD_BUILD_LIST_LINK_NAME
        assert CloudBuildListLink.key == EXPECTED_CLOUD_BUILD_LIST_LINK_KEY
        assert CloudBuildListLink.format_str == EXPECTED_CLOUD_BUILD_LIST_LINK_FORMAT_STR


class TestCloudBuildTriggersListLink:
    def test_class_attributes(self):
        assert CloudBuildTriggersListLink.name == EXPECTED_CLOUD_BUILD_TRIGGERS_LIST_LINK_NAME
        assert CloudBuildTriggersListLink.key == EXPECTED_CLOUD_BUILD_TRIGGERS_LIST_LINK_KEY
        assert CloudBuildTriggersListLink.format_str == EXPECTED_CLOUD_BUILD_TRIGGERS_LIST_LINK_FORMAT_STR


class TestCloudBuildTriggerDetailsLink:
    def test_class_attributes(self):
        assert CloudBuildTriggerDetailsLink.name == EXPECTED_CLOUD_BUILD_TRIGGER_DETAILS_LINK_NAME
        assert CloudBuildTriggerDetailsLink.key == EXPECTED_CLOUD_BUILD_TRIGGER_DETAILS_LINK_KEY
        assert CloudBuildTriggerDetailsLink.format_str == EXPECTED_CLOUD_BUILD_TRIGGER_DETAILS_LINK_FORMAT_STR
