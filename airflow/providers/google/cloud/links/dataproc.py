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

import warnings
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import BaseOperatorLink, XCom
from airflow.providers.google.cloud.links.base import BASE_LINK, BaseGoogleLink

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


def __getattr__(name: str) -> Any:
    # PEP-562: deprecate module-level variable
    if name == "DATAPROC_JOB_LOG_LINK":
        # TODO: remove DATAPROC_JOB_LOG_LINK alias in the next major release
        # For backward-compatibility, DATAPROC_JOB_LINK was DATAPROC_JOB_LOG_LINK.
        warnings.warn(
            (
                "DATAPROC_JOB_LOG_LINK has been deprecated and will be removed in the next MAJOR release."
                " Please use DATAPROC_JOB_LINK instead"
            ),
            AirflowProviderDeprecationWarning,
        )
        return DATAPROC_JOB_LINK
    raise AttributeError(f"module {__name__} has no attribute {name}")


DATAPROC_BASE_LINK = BASE_LINK + "/dataproc"
DATAPROC_JOB_LINK = DATAPROC_BASE_LINK + "/jobs/{job_id}?region={region}&project={project_id}"

DATAPROC_CLUSTER_LINK = (
    DATAPROC_BASE_LINK + "/clusters/{cluster_id}/monitoring?region={region}&project={project_id}"
)
DATAPROC_WORKFLOW_TEMPLATE_LINK = (
    DATAPROC_BASE_LINK + "/workflows/templates/{region}/{workflow_template_id}?project={project_id}"
)
DATAPROC_WORKFLOW_LINK = (
    DATAPROC_BASE_LINK + "/workflows/instances/{region}/{workflow_id}?project={project_id}"
)

DATAPROC_BATCH_LINK = DATAPROC_BASE_LINK + "/batches/{region}/{batch_id}/monitoring?project={project_id}"
DATAPROC_BATCHES_LINK = DATAPROC_BASE_LINK + "/batches?project={project_id}"
DATAPROC_JOB_LINK_DEPRECATED = DATAPROC_BASE_LINK + "/jobs/{resource}?region={region}&project={project_id}"
DATAPROC_CLUSTER_LINK_DEPRECATED = (
    DATAPROC_BASE_LINK + "/clusters/{resource}/monitoring?region={region}&project={project_id}"
)


class DataprocLink(BaseOperatorLink):
    """
    Helper class for constructing Dataproc resource link.

    .. warning::
       This link is deprecated.
    """

    warnings.warn(
        "This DataprocLink is deprecated.",
        AirflowProviderDeprecationWarning,
    )
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
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        conf = XCom.get_value(key=self.key, ti_key=ti_key)
        return (
            conf["url"].format(
                region=conf["region"], project_id=conf["project_id"], resource=conf["resource"]
            )
            if conf
            else ""
        )


class DataprocListLink(BaseOperatorLink):
    """
    Helper class for constructing list of Dataproc resources link.

    .. warning::
       This link is deprecated.
    """

    warnings.warn("This DataprocListLink is deprecated.", AirflowProviderDeprecationWarning)
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
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        list_conf = XCom.get_value(key=self.key, ti_key=ti_key)
        return (
            list_conf["url"].format(
                project_id=list_conf["project_id"],
            )
            if list_conf
            else ""
        )


class DataprocClusterLink(BaseGoogleLink):
    """Helper class for constructing Dataproc Cluster Link."""

    name = "Dataproc Cluster"
    key = "dataproc_cluster"
    format_str = DATAPROC_CLUSTER_LINK

    @staticmethod
    def persist(
        context: Context,
        operator: BaseOperator,
        cluster_id: str,
        region: str,
        project_id: str,
    ):
        operator.xcom_push(
            context,
            key=DataprocClusterLink.key,
            value={"cluster_id": cluster_id, "region": region, "project_id": project_id},
        )


class DataprocJobLink(BaseGoogleLink):
    """Helper class for constructing Dataproc Job Link."""

    name = "Dataproc Job"
    key = "dataproc_job"
    format_str = DATAPROC_JOB_LINK

    @staticmethod
    def persist(
        context: Context,
        operator: BaseOperator,
        job_id: str,
        region: str,
        project_id: str,
    ):
        operator.xcom_push(
            context,
            key=DataprocJobLink.key,
            value={"job_id": job_id, "region": region, "project_id": project_id},
        )


class DataprocWorkflowLink(BaseGoogleLink):
    """Helper class for constructing Dataproc Workflow Link."""

    name = "Dataproc Workflow"
    key = "dataproc_workflow"
    format_str = DATAPROC_WORKFLOW_LINK

    @staticmethod
    def persist(context: Context, operator: BaseOperator, workflow_id: str, project_id: str, region: str):
        operator.xcom_push(
            context,
            key=DataprocWorkflowLink.key,
            value={"workflow_id": workflow_id, "region": region, "project_id": project_id},
        )


class DataprocWorkflowTemplateLink(BaseGoogleLink):
    """Helper class for constructing Dataproc Workflow Template Link."""

    name = "Dataproc Workflow Template"
    key = "dataproc_workflow_template"
    format_str = DATAPROC_WORKFLOW_TEMPLATE_LINK

    @staticmethod
    def persist(
        context: Context,
        operator: BaseOperator,
        workflow_template_id: str,
        project_id: str,
        region: str,
    ):
        operator.xcom_push(
            context,
            key=DataprocWorkflowTemplateLink.key,
            value={"workflow_template_id": workflow_template_id, "region": region, "project_id": project_id},
        )


class DataprocBatchLink(BaseGoogleLink):
    """Helper class for constructing Dataproc Batch Link."""

    name = "Dataproc Batch"
    key = "dataproc_batch"
    format_str = DATAPROC_BATCH_LINK

    @staticmethod
    def persist(
        context: Context,
        operator: BaseOperator,
        batch_id: str,
        project_id: str,
        region: str,
    ):
        operator.xcom_push(
            context,
            key=DataprocBatchLink.key,
            value={"batch_id": batch_id, "region": region, "project_id": project_id},
        )


class DataprocBatchesListLink(BaseGoogleLink):
    """Helper class for constructing Dataproc Batches List Link."""

    name = "Dataproc Batches List"
    key = "dataproc_batches_list"
    format_str = DATAPROC_BATCHES_LINK

    @staticmethod
    def persist(
        context: Context,
        operator: BaseOperator,
        project_id: str,
    ):
        operator.xcom_push(
            context,
            key=DataprocBatchesListLink.key,
            value={"project_id": project_id},
        )
