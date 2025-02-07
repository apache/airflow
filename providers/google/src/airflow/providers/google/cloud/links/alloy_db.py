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
"""This module contains Google Cloud AlloyDB links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context

ALLOY_DB_BASE_LINK = "/alloydb"
ALLOY_DB_CLUSTER_LINK = (
    ALLOY_DB_BASE_LINK + "/locations/{location_id}/clusters/{cluster_id}?project={project_id}"
)
ALLOY_DB_USERS_LINK = (
    ALLOY_DB_BASE_LINK + "/locations/{location_id}/clusters/{cluster_id}/users?project={project_id}"
)
ALLOY_DB_BACKUPS_LINK = ALLOY_DB_BASE_LINK + "/backups?project={project_id}"


class AlloyDBClusterLink(BaseGoogleLink):
    """Helper class for constructing AlloyDB cluster Link."""

    name = "AlloyDB Cluster"
    key = "alloy_db_cluster"
    format_str = ALLOY_DB_CLUSTER_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        location_id: str,
        cluster_id: str,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=AlloyDBClusterLink.key,
            value={"location_id": location_id, "cluster_id": cluster_id, "project_id": project_id},
        )


class AlloyDBUsersLink(BaseGoogleLink):
    """Helper class for constructing AlloyDB users Link."""

    name = "AlloyDB Users"
    key = "alloy_db_users"
    format_str = ALLOY_DB_USERS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        location_id: str,
        cluster_id: str,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=AlloyDBUsersLink.key,
            value={"location_id": location_id, "cluster_id": cluster_id, "project_id": project_id},
        )


class AlloyDBBackupsLink(BaseGoogleLink):
    """Helper class for constructing AlloyDB backups Link."""

    name = "AlloyDB Backups"
    key = "alloy_db_backups"
    format_str = ALLOY_DB_BACKUPS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=AlloyDBBackupsLink.key,
            value={"project_id": project_id},
        )
