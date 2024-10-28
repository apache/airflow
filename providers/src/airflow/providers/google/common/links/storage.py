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

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

BASE_LINK = "https://console.cloud.google.com"
GCS_STORAGE_LINK = BASE_LINK + "/storage/browser/{uri};tab=objects?project={project_id}"
GCS_FILE_DETAILS_LINK = (
    BASE_LINK + "/storage/browser/_details/{uri};tab=live_object?project={project_id}"
)

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context


class StorageLink(BaseGoogleLink):
    """Helper class for constructing GCS Storage link."""

    name = "GCS Storage"
    key = "storage_conf"
    format_str = GCS_STORAGE_LINK

    @staticmethod
    def persist(context: Context, task_instance, uri: str, project_id: str | None):
        task_instance.xcom_push(
            context=context,
            key=StorageLink.key,
            value={"uri": uri, "project_id": project_id},
        )


class FileDetailsLink(BaseGoogleLink):
    """Helper class for constructing GCS file details link."""

    name = "GCS File Details"
    key = "file_details"
    format_str = GCS_FILE_DETAILS_LINK

    @staticmethod
    def persist(
        context: Context, task_instance: BaseOperator, uri: str, project_id: str | None
    ):
        task_instance.xcom_push(
            context=context,
            key=FileDetailsLink.key,
            value={"uri": uri, "project_id": project_id},
        )
