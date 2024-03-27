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
"""This module contains Google Compute Engine links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context

COMPUTE_BASE_LINK = "https://console.cloud.google.com/compute"
COMPUTE_LINK = (
    COMPUTE_BASE_LINK + "/instancesDetail/zones/{location_id}/instances/{resource_id}?project={project_id}"
)
COMPUTE_TEMPLATE_LINK = COMPUTE_BASE_LINK + "/instanceTemplates/details/{resource_id}?project={project_id}"
COMPUTE_GROUP_MANAGER_LINK = (
    COMPUTE_BASE_LINK + "/instanceGroups/details/{location_id}/{resource_id}?project={project_id}"
)


class ComputeInstanceDetailsLink(BaseGoogleLink):
    """Helper class for constructing Compute Instance details Link."""

    name = "Compute Instance details"
    key = "compute_instance_details"
    format_str = COMPUTE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        location_id: str,
        resource_id: str | None,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=ComputeInstanceDetailsLink.key,
            value={
                "location_id": location_id,
                "resource_id": resource_id,
                "project_id": project_id,
            },
        )


class ComputeInstanceTemplateDetailsLink(BaseGoogleLink):
    """Helper class for constructing Compute Instance Template details Link."""

    name = "Compute Instance Template details"
    key = "compute_instance_template_details"
    format_str = COMPUTE_TEMPLATE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        resource_id: str | None,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=ComputeInstanceTemplateDetailsLink.key,
            value={
                "resource_id": resource_id,
                "project_id": project_id,
            },
        )


class ComputeInstanceGroupManagerDetailsLink(BaseGoogleLink):
    """Helper class for constructing Compute Instance Group Manager details Link."""

    name = "Compute Instance Group Manager"
    key = "compute_instance_group_manager_details"
    format_str = COMPUTE_GROUP_MANAGER_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        location_id: str,
        resource_id: str | None,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=ComputeInstanceGroupManagerDetailsLink.key,
            value={
                "location_id": location_id,
                "resource_id": resource_id,
                "project_id": project_id,
            },
        )
