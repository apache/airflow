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

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

BUILD_BASE_LINK = "/cloud-build"

BUILD_LINK = BUILD_BASE_LINK + "/builds;region={region}/{build_id}?project={project_id}"

BUILD_LIST_LINK = BUILD_BASE_LINK + "/builds;region={region}?project={project_id}"

BUILD_TRIGGERS_LIST_LINK = (
    BUILD_BASE_LINK + "/triggers;region={region}?project={project_id}"
)

BUILD_TRIGGER_DETAILS_LINK = (
    BUILD_BASE_LINK + "/triggers;region={region}/edit/{trigger_id}?project={project_id}"
)


class CloudBuildLink(BaseGoogleLink):
    """Helper class for constructing Cloud Build link."""

    name = "Cloud Build Details"
    key = "cloud_build_key"
    format_str = BUILD_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        build_id: str,
        project_id: str,
        region: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=CloudBuildLink.key,
            value={
                "project_id": project_id,
                "region": region,
                "build_id": build_id,
            },
        )


class CloudBuildListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Build List link."""

    name = "Cloud Builds List"
    key = "cloud_build_list_key"
    format_str = BUILD_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
        region: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=CloudBuildListLink.key,
            value={
                "project_id": project_id,
                "region": region,
            },
        )


class CloudBuildTriggersListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Build Triggers List link."""

    name = "Cloud Build Triggers List"
    key = "cloud_build_triggers_list_key"
    format_str = BUILD_TRIGGERS_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
        region: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=CloudBuildTriggersListLink.key,
            value={
                "project_id": project_id,
                "region": region,
            },
        )


class CloudBuildTriggerDetailsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Build Trigger Details link."""

    name = "Cloud Build Triggers Details"
    key = "cloud_build_triggers_details_key"
    format_str = BUILD_TRIGGER_DETAILS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
        region: str,
        trigger_id: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=CloudBuildTriggerDetailsLink.key,
            value={
                "project_id": project_id,
                "region": region,
                "trigger_id": trigger_id,
            },
        )
