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
"""This module contains Google Dataplex links."""

from datetime import datetime
from typing import TYPE_CHECKING

from airflow.models import BaseOperator, BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.utils.context import Context

DATAPLEX_BASE_LINK = "https://console.cloud.google.com/dataplex/process/tasks"
DATAPLEX_TASK_LINK = DATAPLEX_BASE_LINK + "/{lake_id}.{task_id};location={region}/jobs?project={project_id}"
DATAPLEX_TASKS_LINK = DATAPLEX_BASE_LINK + "?project={project_id}&qLake={lake_id}.{region}"


class DataplexTaskLink(BaseOperatorLink):
    """Helper class for constructing Dataplex Task link"""

    name = "Dataplex Task"
    key = "task_conf"

    @staticmethod
    def persist(
        context: "Context",
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataplexTaskLink.key,
            value={
                "lake_id": task_instance.lake_id,
                "task_id": task_instance.dataplex_task_id,
                "region": task_instance.region,
                "project_id": task_instance.project_id,
            },
        )

    def get_link(self, operator: BaseOperator, dttm: datetime):
        task_conf = XCom.get_one(
            key=DataplexTaskLink.key,
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
        )
        return (
            DATAPLEX_TASK_LINK.format(
                lake_id=task_conf["lake_id"],
                task_id=task_conf["task_id"],
                region=task_conf["region"],
                project_id=task_conf["project_id"],
            )
            if task_conf
            else ""
        )


class DataplexTasksLink(BaseOperatorLink):
    """Helper class for constructing Dataplex Tasks link"""

    name = "Dataplex Tasks"
    key = "tasks_conf"

    @staticmethod
    def persist(
        context: "Context",
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataplexTasksLink.key,
            value={
                "project_id": task_instance.project_id,
                "lake_id": task_instance.lake_id,
                "region": task_instance.region,
            },
        )

    def get_link(self, operator: BaseOperator, dttm: datetime):
        tasks_conf = XCom.get_one(
            key=DataplexTasksLink.key,
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
        )
        return (
            DATAPLEX_TASKS_LINK.format(
                project_id=tasks_conf["project_id"],
                lake_id=tasks_conf["lake_id"],
                region=tasks_conf["region"],
            )
            if tasks_conf
            else ""
        )
