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
"""This module contains Google Spanner links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context

SPANNER_BASE_LINK = "/spanner/instances"
SPANNER_INSTANCE_LINK = (
    SPANNER_BASE_LINK + "/{instance_id}/details/databases?project={project_id}"
)
SPANNER_DATABASE_LINK = (
    SPANNER_BASE_LINK
    + "/{instance_id}/databases/{database_id}/details/tables?project={project_id}"
)


class SpannerInstanceLink(BaseGoogleLink):
    """Helper class for constructing Spanner Instance Link."""

    name = "Spanner Instance"
    key = "spanner_instance"
    format_str = SPANNER_INSTANCE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        instance_id: str,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=SpannerInstanceLink.key,
            value={"instance_id": instance_id, "project_id": project_id},
        )


class SpannerDatabaseLink(BaseGoogleLink):
    """Helper class for constructing Spanner Database Link."""

    name = "Spanner Database"
    key = "spanner_database"
    format_str = SPANNER_DATABASE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        instance_id: str,
        database_id: str,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=SpannerDatabaseLink.key,
            value={
                "instance_id": instance_id,
                "database_id": database_id,
                "project_id": project_id,
            },
        )
