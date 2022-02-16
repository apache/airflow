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
"""This module contains a link for GCS Storage assets."""
from datetime import datetime
from typing import TYPE_CHECKING

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.xcom import XCom

BASE_LINK = "https://console.cloud.google.com"
GCS_STORAGE_LINK = BASE_LINK + "/storage/browser/{uri};tab=objects?project={project_id}"

if TYPE_CHECKING:
    from airflow.utils.context import Context


class StorageLink(BaseOperatorLink):
    """Helper class for constructing GCS Storage link"""

    name = "GCS Storage"
    key = "storage_conf"

    @staticmethod
    def persist(context: "Context", task_instance, uri: str):
        task_instance.xcom_push(
            context=context,
            key=StorageLink.key,
            value={"uri": uri, "project_id": task_instance.project_id},
        )

    def get_link(self, operator: BaseOperator, dttm: datetime):
        storage_conf = XCom.get_one(
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
            key=StorageLink.key,
        )
        return (
            GCS_STORAGE_LINK.format(
                uri=storage_conf["uri"],
                project_id=storage_conf["project_id"],
            )
            if storage_conf
            else ""
        )
