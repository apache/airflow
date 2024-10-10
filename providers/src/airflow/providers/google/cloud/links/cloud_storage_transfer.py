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
"""This module contains Google Storage Transfer Service links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

CLOUD_STORAGE_TRANSFER_BASE_LINK = "https://console.cloud.google.com/transfer"

CLOUD_STORAGE_TRANSFER_LIST_LINK = CLOUD_STORAGE_TRANSFER_BASE_LINK + "/jobs?project={project_id}"

CLOUD_STORAGE_TRANSFER_JOB_LINK = (
    CLOUD_STORAGE_TRANSFER_BASE_LINK + "/jobs/transferJobs%2F{transfer_job}/runs?project={project_id}"
)

CLOUD_STORAGE_TRANSFER_OPERATION_LINK = (
    CLOUD_STORAGE_TRANSFER_BASE_LINK
    + "/jobs/transferJobs%2F{transfer_job}/runs/transferOperations%2F{transfer_operation}"
    + "?project={project_id}"
)


class CloudStorageTransferLinkHelper:
    """Helper class for Storage Transfer links."""

    @staticmethod
    def extract_parts(operation_name: str | None):
        if not operation_name:
            return "", ""
        transfer_operation = operation_name.split("/")[1]
        transfer_job = operation_name.split("-")[1]
        return transfer_operation, transfer_job


class CloudStorageTransferListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Storage Transfer Link."""

    name = "Cloud Storage Transfer"
    key = "cloud_storage_transfer"
    format_str = CLOUD_STORAGE_TRANSFER_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=CloudStorageTransferListLink.key,
            value={"project_id": project_id},
        )


class CloudStorageTransferJobLink(BaseGoogleLink):
    """Helper class for constructing Storage Transfer Job Link."""

    name = "Cloud Storage Transfer Job"
    key = "cloud_storage_transfer_job"
    format_str = CLOUD_STORAGE_TRANSFER_JOB_LINK

    @staticmethod
    def persist(
        task_instance,
        context: Context,
        project_id: str,
        job_name: str,
    ):
        job_name = job_name.split("/")[1] if job_name else ""

        task_instance.xcom_push(
            context,
            key=CloudStorageTransferJobLink.key,
            value={
                "project_id": project_id,
                "transfer_job": job_name,
            },
        )


class CloudStorageTransferDetailsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Storage Transfer Operation Link."""

    name = "Cloud Storage Transfer Details"
    key = "cloud_storage_transfer_details"
    format_str = CLOUD_STORAGE_TRANSFER_OPERATION_LINK

    @staticmethod
    def persist(
        task_instance,
        context: Context,
        project_id: str,
        operation_name: str,
    ):
        transfer_operation, transfer_job = CloudStorageTransferLinkHelper.extract_parts(operation_name)

        task_instance.xcom_push(
            context,
            key=CloudStorageTransferDetailsLink.key,
            value={
                "project_id": project_id,
                "transfer_job": transfer_job,
                "transfer_operation": transfer_operation,
            },
        )
