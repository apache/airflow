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
"""This module contains Google Dataproc links."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from airflow.models import BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.context import Context

DATAPROC_BASE_LINK = "https://console.cloud.google.com/dataproc"
DATAPROC_JOB_LOG_LINK = DATAPROC_BASE_LINK + "/jobs/{resource}?region={region}&project={project_id}"
DATAPROC_CLUSTER_LINK = (
    DATAPROC_BASE_LINK + "/clusters/{resource}/monitoring?region={region}&project={project_id}"
)
DATAPROC_WORKFLOW_TEMPLATE_LINK = (
    DATAPROC_BASE_LINK + "/workflows/templates/{region}/{resource}?project={project_id}"
)
DATAPROC_WORKFLOW_LINK = DATAPROC_BASE_LINK + "/workflows/instances/{region}/{resource}?project={project_id}"
DATAPROC_BATCH_LINK = DATAPROC_BASE_LINK + "/batches/{region}/{resource}/monitoring?project={project_id}"
DATAPROC_BATCHES_LINK = DATAPROC_BASE_LINK + "/batches?project={project_id}"


class DataprocLink(BaseOperatorLink):
    """Helper class for constructing Dataproc resource link"""

    name = "Dataproc resource"
    key = "conf"

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        url: str,
        resource: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataprocLink.key,
            value={
                "region": task_instance.region,
                "project_id": task_instance.project_id,
                "url": url,
                "resource": resource,
            },
        )

    def get_link(
        self,
        operator,
        dttm: datetime | None = None,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        if ti_key is not None:
            conf = XCom.get_value(key=self.key, ti_key=ti_key)
        else:
            assert dttm
            conf = XCom.get_one(
                key=self.key, dag_id=operator.dag.dag_id, task_id=operator.task_id, execution_date=dttm
            )
        return (
            conf["url"].format(
                region=conf["region"], project_id=conf["project_id"], resource=conf["resource"]
            )
            if conf
            else ""
        )


class DataprocListLink(BaseOperatorLink):
    """Helper class for constructing list of Dataproc resources link"""

    name = "Dataproc resources"
    key = "list_conf"

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        url: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataprocListLink.key,
            value={
                "project_id": task_instance.project_id,
                "url": url,
            },
        )

    def get_link(
        self,
        operator,
        dttm: datetime | None = None,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        if ti_key is not None:
            list_conf = XCom.get_value(key=self.key, ti_key=ti_key)
        else:
            assert dttm
            list_conf = XCom.get_one(
                key=self.key,
                dag_id=operator.dag.dag_id,
                task_id=operator.task_id,
                execution_date=dttm,
            )
        return (
            list_conf["url"].format(
                project_id=list_conf["project_id"],
            )
            if list_conf
            else ""
        )
