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
"""This module contains Google Cloud Tasks links."""
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

CLOUD_TASKS_BASE_LINK = "/cloudtasks"
CLOUD_TASKS_QUEUE_LINK = CLOUD_TASKS_BASE_LINK + "/queue/{location}/{queue_id}/tasks?project={project_id}"
CLOUD_TASKS_LINK = CLOUD_TASKS_BASE_LINK + "?project={project_id}"


class CloudTasksQueueLink(BaseGoogleLink):
    """Helper class for constructing Cloud Task Queue Link"""

    name = "Cloud Tasks Queue"
    key = "cloud_task_queue"
    format_str = CLOUD_TASKS_QUEUE_LINK

    @staticmethod
    def extract_parts(queue_name: str | None):
        """
        Extract project_id, location and queue id from queue name:
        projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID
        """
        if not queue_name:
            return "", "", ""
        parts = queue_name.split("/")
        return parts[1], parts[3], parts[5]

    @staticmethod
    def persist(
        operator_instance: BaseOperator,
        context: Context,
        queue_name: str | None,
    ):
        project_id, location, queue_id = CloudTasksQueueLink.extract_parts(queue_name)
        operator_instance.xcom_push(
            context,
            key=CloudTasksQueueLink.key,
            value={"project_id": project_id, "location": location, "queue_id": queue_id},
        )


class CloudTasksLink(BaseGoogleLink):
    """Helper class for constructing Cloud Task Link"""

    name = "Cloud Tasks"
    key = "cloud_task"
    format_str = CLOUD_TASKS_LINK

    @staticmethod
    def persist(
        operator_instance: BaseOperator,
        context: Context,
        project_id: str | None,
    ):
        operator_instance.xcom_push(
            context,
            key=CloudTasksLink.key,
            value={"project_id": project_id},
        )
