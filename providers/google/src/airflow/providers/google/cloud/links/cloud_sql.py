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
"""This module contains Google Cloud SQL links."""
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context


CLOUD_SQL_BASE_LINK = "/sql"
CLOUD_SQL_INSTANCE_LINK = CLOUD_SQL_BASE_LINK + "/instances/{instance}/overview?project={project_id}"
CLOUD_SQL_INSTANCE_DATABASE_LINK = (
    CLOUD_SQL_BASE_LINK + "/instances/{instance}/databases?project={project_id}"
)


class CloudSQLInstanceLink(BaseGoogleLink):
    """Helper class for constructing Cloud SQL Instance Link"""

    name = "Cloud SQL Instance"
    key = "cloud_sql_instance"
    format_str = CLOUD_SQL_INSTANCE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        cloud_sql_instance: str,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=CloudSQLInstanceLink.key,
            value={"instance": cloud_sql_instance, "project_id": project_id},
        )


class CloudSQLInstanceDatabaseLink(BaseGoogleLink):
    """Helper class for constructing Cloud SQL Instance Database Link"""

    name = "Cloud SQL Instance Database"
    key = "cloud_sql_instance_database"
    format_str = CLOUD_SQL_INSTANCE_DATABASE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        cloud_sql_instance: str,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=CloudSQLInstanceDatabaseLink.key,
            value={"instance": cloud_sql_instance, "project_id": project_id},
        )
