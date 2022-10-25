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
"""This module contains a Google Cloud Dataproc hook."""
from __future__ import annotations

import time
import uuid
from typing import Any, Dict, Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import ServerError
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.operation_async import AsyncOperation
from google.api_core.retry import Retry
from google.cloud.dataproc_v1 import (
    Batch,
    BatchControllerAsyncClient,
    BatchControllerClient,
    Cluster,
    ClusterControllerAsyncClient,
    ClusterControllerClient,
    Job,
    JobControllerAsyncClient,
    JobControllerClient,
    JobStatus,
    WorkflowTemplate,
    WorkflowTemplateServiceAsyncClient,
    WorkflowTemplateServiceClient,
)
from google.protobuf.duration_pb2 import Duration
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.version import version as airflow_version


class DataProcJobBuilder:
    """A helper class for building Dataproc job."""

    def __init__(
        self,
        project_id: str,
        task_id: str,
        cluster_name: str,
        job_type: str,
        properties: dict[str, str] | None = None,
    ) -> None:
        name = f"{task_id.replace('.', '_')}_{uuid.uuid4()!s:.8}"
        self.job_type = job_type
        self.job = {
            "job": {
                "reference": {"project_id": project_id, "job_id": name},
                "placement": {"cluster_name": cluster_name},
                "labels": {"airflow-version": "v" + airflow_version.replace(".", "-").replace("+", "-")},
                job_type: {},
            }
        }  # type: Dict[str, Any]
        if properties is not None:
            self.job["job"][job_type]["properties"] = properties

    def add_labels(self, labels: dict | None = None) -> None:
        """
        Set labels for Dataproc job.

        :param labels: Labels for the job query.
        """
        if labels:
            self.job["job"]["labels"].update(labels)

    def add_variables(self, variables: dict | None = None) -> None:
        """
        Set variables for Dataproc job.

        :param variables: Variables for the job query.
        """
        if variables is not None:
            self.job["job"][self.job_type]["script_variables"] = variables

    def add_args(self, args: list[str] | None = None) -> None:
        """
        Set args for Dataproc job.

        :param args: Args for the job query.
        """
        if args is not None:
            self.job["job"][self.job_type]["args"] = args

    def add_query(self, query: str) -> None:
        """
        Set query for Dataproc job.

        :param query: query for the job.
        """
        self.job["job"][self.job_type]["query_list"] = {"queries": [query]}

    def add_query_uri(self, query_uri: str) -> None:
        """
        Set query uri for Dataproc job.

        :param query_uri: URI for the job query.
        """
        self.job["job"][self.job_type]["query_file_uri"] = query_uri

    def add_jar_file_uris(self, jars: list[str] | None = None) -> None:
        """
        Set jars uris for Dataproc job.

        :param jars: List of jars URIs
        """
        if jars is not None:
            self.job["job"][self.job_type]["jar_file_uris"] = jars

    def add_archive_uris(self, archives: list[str] | None = None) -> None:
        """
        Set archives uris for Dataproc job.

        :param archives: List of archives URIs
        """
        if archives is not None:
            self.job["job"][self.job_type]["archive_uris"] = archives

    def add_file_uris(self, files: list[str] | None = None) -> None:
        """
        Set file uris for Dataproc job.

        :param files: List of files URIs
        """
        if files is not None:
            self.job["job"][self.job_type]["file_uris"] = files

    def add_python_file_uris(self, pyfiles: list[str] | None = None) -> None:
        """
        Set python file uris for Dataproc job.

        :param pyfiles: List of python files URIs
        """
        if pyfiles is not None:
            self.job["job"][self.job_type]["python_file_uris"] = pyfiles

    def set_main(self, main_jar: str | None = None, main_class: str | None = None) -> None:
        """
        Set Dataproc main class.

        :param main_jar: URI for the main file.
        :param main_class: Name of the main class.
        :raises: Exception
        """
        if main_class is not None and main_jar is not None:
            raise Exception("Set either main_jar or main_class")
        if main_jar:
            self.job["job"][self.job_type]["main_jar_file_uri"] = main_jar
        else:
            self.job["job"][self.job_type]["main_class"] = main_class

    def set_python_main(self, main: str) -> None:
        """
        Set Dataproc main python file uri.

        :param main: URI for the python main file.
        """
        self.job["job"][self.job_type]["main_python_file_uri"] = main

    def set_job_name(self, name: str) -> None:
        """
        Set Dataproc job name. Job name is sanitized, replacing dots by underscores.

        :param name: Job name.
        """
        sanitized_name = f"{name.replace('.', '_')}_{uuid.uuid4()!s:.8}"
        self.job["job"]["reference"]["job_id"] = sanitized_name

    def build(self) -> dict:
        """
        Returns Dataproc job.

        :return: Dataproc job
        :rtype: dict
        """
        return self.job


class DataprocHook(GoogleBaseHook):
    """
    Hook for Google Cloud Dataproc APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to, impersonation_chain)

    def get_cluster_client(self, region: str | None = None) -> ClusterControllerClient:
        """Returns ClusterControllerClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return ClusterControllerClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_template_client(self, region: str | None = None) -> WorkflowTemplateServiceClient:
        """Returns WorkflowTemplateServiceClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return WorkflowTemplateServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_job_client(self, region: str | None = None) -> JobControllerClient:
        """Returns JobControllerClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return JobControllerClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_batch_client(self, region: str | None = None) -> BatchControllerClient:
        """Returns BatchControllerClient"""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return BatchControllerClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def wait_for_operation(
        self,
        operation: Operation,
        timeout: float | None = None,
        result_retry: Retry | _MethodDefault = DEFAULT,
    ):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout, retry=result_retry)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_cluster(
        self,
        region: str,
        project_id: str,
        cluster_name: str,
        cluster_config: dict | Cluster | None = None,
        virtual_cluster_config: dict | None = None,
        labels: dict[str, str] | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Creates a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Name of the cluster to create
        :param labels: Labels that will be assigned to created cluster
        :param cluster_config: Required. The cluster config to create.
            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.ClusterConfig`
        :param virtual_cluster_config: Optional. The virtual cluster config, used when creating a Dataproc
            cluster that does not directly control the underlying compute resources, for example, when
            creating a `Dataproc-on-GKE cluster`
            :class:`~google.cloud.dataproc_v1.types.VirtualClusterConfig`
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``CreateClusterRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        # Dataproc labels must conform to the following regex:
        # [a-z]([-a-z0-9]*[a-z0-9])? (current airflow version string follows
        # semantic versioning spec: x.y.z).
        labels = labels or {}
        labels.update({"airflow-version": "v" + airflow_version.replace(".", "-").replace("+", "-")})

        cluster = {
            "project_id": project_id,
            "cluster_name": cluster_name,
        }
        if virtual_cluster_config is not None:
            cluster["virtual_cluster_config"] = virtual_cluster_config  # type: ignore
        if cluster_config is not None:
            cluster["config"] = cluster_config  # type: ignore
            cluster["labels"] = labels  # type: ignore

        client = self.get_cluster_client(region=region)
        result = client.create_cluster(
            request={
                "project_id": project_id,
                "region": region,
                "cluster": cluster,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        cluster_uuid: str | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Deletes a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Required. The cluster name.
        :param cluster_uuid: Optional. Specifying the ``cluster_uuid`` means the RPC should fail
            if cluster with specified UUID does not exist.
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_cluster_client(region=region)
        result = client.delete_cluster(
            request={
                "project_id": project_id,
                "region": region,
                "cluster_name": cluster_name,
                "cluster_uuid": cluster_uuid,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def diagnose_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Gets cluster diagnostic information. After the operation completes GCS uri to
        diagnose is returned

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Required. The cluster name.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_cluster_client(region=region)
        operation = client.diagnose_cluster(
            request={"project_id": project_id, "region": region, "cluster_name": cluster_name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        operation.result()
        gcs_uri = str(operation.operation.response.value)
        return gcs_uri

    @GoogleBaseHook.fallback_to_default_project_id
    def get_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Gets the resource representation for a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Required. The cluster name.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_cluster_client(region=region)
        result = client.get_cluster(
            request={"project_id": project_id, "region": region, "cluster_name": cluster_name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_clusters(
        self,
        region: str,
        filter_: str,
        project_id: str,
        page_size: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Lists all regions/{region}/clusters in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param filter_: Optional. A filter constraining the clusters to list. Filters are case-sensitive.
        :param page_size: The maximum number of resources contained in the underlying API response. If page
            streaming is performed per- resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number of resources in a page.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_cluster_client(region=region)
        result = client.list_clusters(
            request={"project_id": project_id, "region": region, "filter": filter_, "page_size": page_size},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_cluster(
        self,
        cluster_name: str,
        cluster: dict | Cluster,
        update_mask: dict | FieldMask,
        project_id: str,
        region: str,
        graceful_decommission_timeout: dict | Duration | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Updates a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Required. The cluster name.
        :param cluster: Required. The changes to the cluster.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.Cluster`
        :param update_mask: Required. Specifies the path, relative to ``Cluster``, of the field to update. For
            example, to change the number of workers in a cluster to 5, the ``update_mask`` parameter would be
            specified as ``config.worker_config.num_instances``, and the ``PATCH`` request body would specify
            the new value, as follows:

            ::

                 { "config":{ "workerConfig":{ "numInstances":"5" } } }

            Similarly, to change the number of preemptible workers in a cluster to 5, the ``update_mask``
            parameter would be ``config.secondary_worker_config.num_instances``, and the ``PATCH`` request
            body would be set as follows:

            ::

                 { "config":{ "secondaryWorkerConfig":{ "numInstances":"5" } } }

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.FieldMask`
        :param graceful_decommission_timeout: Optional. Timeout for graceful YARN decommissioning. Graceful
            decommissioning allows removing nodes from the cluster without interrupting jobs in progress.
            Timeout specifies how long to wait for jobs in progress to finish before forcefully removing nodes
            (and potentially interrupting jobs). Default timeout is 0 (for forceful decommission), and the
            maximum allowed timeout is 1 day.

            Only supported on Dataproc image versions 1.2 and higher.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.Duration`
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``UpdateClusterRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        client = self.get_cluster_client(region=region)
        operation = client.update_cluster(
            request={
                "project_id": project_id,
                "region": region,
                "cluster_name": cluster_name,
                "cluster": cluster,
                "update_mask": update_mask,
                "graceful_decommission_timeout": graceful_decommission_timeout,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def create_workflow_template(
        self,
        template: dict | WorkflowTemplate,
        project_id: str,
        region: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> WorkflowTemplate:
        """
        Creates new workflow template.

        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param template: The Dataproc workflow template to create. If a dict is provided,
            it must be of the same form as the protobuf message WorkflowTemplate.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        metadata = metadata or ()
        client = self.get_template_client(region)
        parent = f"projects/{project_id}/regions/{region}"
        return client.create_workflow_template(
            request={"parent": parent, "template": template}, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def instantiate_workflow_template(
        self,
        template_name: str,
        project_id: str,
        region: str,
        version: int | None = None,
        request_id: str | None = None,
        parameters: dict[str, str] | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Instantiates a template and begins execution.

        :param template_name: Name of template to instantiate.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param version: Optional. The version of workflow template to instantiate. If specified,
            the workflow will be instantiated only if the current version of
            the workflow template has the supplied version.
            This option cannot be used to instantiate a previous version of
            workflow template.
        :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
            with the same tag from running. This mitigates risk of concurrent
            instances started due to retries.
        :param parameters: Optional. Map from parameter names to values that should be used for those
            parameters. Values may not exceed 100 characters.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        metadata = metadata or ()
        client = self.get_template_client(region)
        name = f"projects/{project_id}/regions/{region}/workflowTemplates/{template_name}"
        operation = client.instantiate_workflow_template(
            request={"name": name, "version": version, "request_id": request_id, "parameters": parameters},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def instantiate_inline_workflow_template(
        self,
        template: dict | WorkflowTemplate,
        project_id: str,
        region: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Instantiates a template and begins execution.

        :param template: The workflow template to instantiate. If a dict is provided,
            it must be of the same form as the protobuf message WorkflowTemplate
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
            with the same tag from running. This mitigates risk of concurrent
            instances started due to retries.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        metadata = metadata or ()
        client = self.get_template_client(region)
        parent = f"projects/{project_id}/regions/{region}"
        operation = client.instantiate_inline_workflow_template(
            request={"parent": parent, "template": template, "request_id": request_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def wait_for_job(
        self,
        job_id: str,
        project_id: str,
        region: str,
        wait_time: int = 10,
        timeout: int | None = None,
    ) -> None:
        """
        Helper method which polls a job to check if it finishes.

        :param job_id: Id of the Dataproc job
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param wait_time: Number of seconds between checks
        :param timeout: How many seconds wait for job to be ready. Used only if ``asynchronous`` is False
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        state = None
        start = time.monotonic()
        while state not in (JobStatus.State.ERROR, JobStatus.State.DONE, JobStatus.State.CANCELLED):
            if timeout and start + timeout < time.monotonic():
                raise AirflowException(f"Timeout: dataproc job {job_id} is not ready after {timeout}s")
            time.sleep(wait_time)
            try:
                job = self.get_job(project_id=project_id, region=region, job_id=job_id)
                state = job.status.state
            except ServerError as err:
                self.log.info("Retrying. Dataproc API returned server error when waiting for job: %s", err)

        if state == JobStatus.State.ERROR:
            raise AirflowException(f"Job failed:\n{job}")
        if state == JobStatus.State.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{job}")

    @GoogleBaseHook.fallback_to_default_project_id
    def get_job(
        self,
        job_id: str,
        project_id: str,
        region: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """
        Gets the resource representation for a job in a project.

        :param job_id: Id of the Dataproc job
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        client = self.get_job_client(region=region)
        job = client.get_job(
            request={"project_id": project_id, "region": region, "job_id": job_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job

    @GoogleBaseHook.fallback_to_default_project_id
    def submit_job(
        self,
        job: dict | Job,
        project_id: str,
        region: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """
        Submits a job to a cluster.

        :param job: The job resource. If a dict is provided,
            it must be of the same form as the protobuf message Job
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
            with the same tag from running. This mitigates risk of concurrent
            instances started due to retries.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        client = self.get_job_client(region=region)
        return client.submit_job(
            request={"project_id": project_id, "region": region, "job": job, "request_id": request_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_job(
        self,
        job_id: str,
        project_id: str,
        region: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """
        Starts a job cancellation request.

        :param project_id: Required. The ID of the Google Cloud project that the job belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param job_id: Required. The job ID.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_job_client(region=region)

        job = client.cancel_job(
            request={"project_id": project_id, "region": region, "job_id": job_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job

    @GoogleBaseHook.fallback_to_default_project_id
    def create_batch(
        self,
        region: str,
        project_id: str,
        batch: dict | Batch,
        batch_id: str | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Creates a batch workload.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param batch: Required. The batch to create.
        :param batch_id: Optional. The ID to use for the batch, which will become the final component
            of the batch's resource name.
            This value must be 4-63 characters. Valid characters are /[a-z][0-9]-/.
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``CreateBatchRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        parent = f"projects/{project_id}/regions/{region}"

        result = client.create_batch(
            request={
                "parent": parent,
                "batch": batch,
                "batch_id": batch_id,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_batch(
        self,
        batch_id: str,
        region: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes the batch workload resource.

        :param batch_id: Required. The ID to use for the batch, which will become the final component
            of the batch's resource name.
            This value must be 4-63 characters. Valid characters are /[a-z][0-9]-/.
        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        name = f"projects/{project_id}/regions/{region}/batches/{batch_id}"

        client.delete_batch(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_batch(
        self,
        batch_id: str,
        region: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Batch:
        """
        Gets the batch workload resource representation.

        :param batch_id: Required. The ID to use for the batch, which will become the final component
            of the batch's resource name.
            This value must be 4-63 characters. Valid characters are /[a-z][0-9]-/.
        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        name = f"projects/{project_id}/regions/{region}/batches/{batch_id}"

        result = client.get_batch(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_batches(
        self,
        region: str,
        project_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Lists batch workloads.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param page_size: Optional. The maximum number of batches to return in each response. The service may
            return fewer than this value. The default page size is 20; the maximum page size is 1000.
        :param page_token: Optional. A page token received from a previous ``ListBatches`` call.
            Provide this token to retrieve the subsequent page.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        parent = f"projects/{project_id}/regions/{region}"

        result = client.list_batches(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result


class DataprocAsyncHook(GoogleBaseHook):
    """
    Asynchronous Hook for Google Cloud Dataproc APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to, impersonation_chain)

    def get_cluster_client(self, region: str | None = None) -> ClusterControllerAsyncClient:
        """Returns ClusterControllerAsyncClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return ClusterControllerAsyncClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_template_client(self, region: str | None = None) -> WorkflowTemplateServiceAsyncClient:
        """Returns WorkflowTemplateServiceAsyncClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return WorkflowTemplateServiceAsyncClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_job_client(self, region: str | None = None) -> JobControllerAsyncClient:
        """Returns JobControllerAsyncClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return JobControllerAsyncClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=client_options,
        )

    def get_batch_client(self, region: str | None = None) -> BatchControllerAsyncClient:
        """Returns BatchControllerAsyncClient"""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return BatchControllerAsyncClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def create_cluster(
        self,
        region: str,
        project_id: str,
        cluster_name: str,
        cluster_config: dict | Cluster | None = None,
        virtual_cluster_config: dict | None = None,
        labels: dict[str, str] | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Creates a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Name of the cluster to create
        :param labels: Labels that will be assigned to created cluster
        :param cluster_config: Required. The cluster config to create.
            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.ClusterConfig`
        :param virtual_cluster_config: Optional. The virtual cluster config, used when creating a Dataproc
            cluster that does not directly control the underlying compute resources, for example, when
            creating a `Dataproc-on-GKE cluster`
            :class:`~google.cloud.dataproc_v1.types.VirtualClusterConfig`
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``CreateClusterRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        # Dataproc labels must conform to the following regex:
        # [a-z]([-a-z0-9]*[a-z0-9])? (current airflow version string follows
        # semantic versioning spec: x.y.z).
        labels = labels or {}
        labels.update({"airflow-version": "v" + airflow_version.replace(".", "-").replace("+", "-")})

        cluster = {
            "project_id": project_id,
            "cluster_name": cluster_name,
        }
        if virtual_cluster_config is not None:
            cluster["virtual_cluster_config"] = virtual_cluster_config  # type: ignore
        if cluster_config is not None:
            cluster["config"] = cluster_config  # type: ignore
            cluster["labels"] = labels  # type: ignore

        client = self.get_cluster_client(region=region)
        result = await client.create_cluster(
            request={
                "project_id": project_id,
                "region": region,
                "cluster": cluster,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    async def delete_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        cluster_uuid: str | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Deletes a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Required. The cluster name.
        :param cluster_uuid: Optional. Specifying the ``cluster_uuid`` means the RPC should fail
            if cluster with specified UUID does not exist.
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_cluster_client(region=region)
        result = client.delete_cluster(
            request={
                "project_id": project_id,
                "region": region,
                "cluster_name": cluster_name,
                "cluster_uuid": cluster_uuid,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    async def diagnose_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Gets cluster diagnostic information. After the operation completes GCS uri to
        diagnose is returned

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Required. The cluster name.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_cluster_client(region=region)
        operation = await client.diagnose_cluster(
            request={"project_id": project_id, "region": region, "cluster_name": cluster_name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        operation.result()
        gcs_uri = str(operation.operation.response.value)
        return gcs_uri

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Gets the resource representation for a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Required. The cluster name.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_cluster_client(region=region)
        result = await client.get_cluster(
            request={"project_id": project_id, "region": region, "cluster_name": cluster_name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    async def list_clusters(
        self,
        region: str,
        filter_: str,
        project_id: str,
        page_size: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Lists all regions/{region}/clusters in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param filter_: Optional. A filter constraining the clusters to list. Filters are case-sensitive.
        :param page_size: The maximum number of resources contained in the underlying API response. If page
            streaming is performed per- resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number of resources in a page.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_cluster_client(region=region)
        result = await client.list_clusters(
            request={"project_id": project_id, "region": region, "filter": filter_, "page_size": page_size},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    async def update_cluster(
        self,
        cluster_name: str,
        cluster: dict | Cluster,
        update_mask: dict | FieldMask,
        project_id: str,
        region: str,
        graceful_decommission_timeout: dict | Duration | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Updates a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param cluster_name: Required. The cluster name.
        :param cluster: Required. The changes to the cluster.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.Cluster`
        :param update_mask: Required. Specifies the path, relative to ``Cluster``, of the field to update. For
            example, to change the number of workers in a cluster to 5, the ``update_mask`` parameter would be
            specified as ``config.worker_config.num_instances``, and the ``PATCH`` request body would specify
            the new value, as follows:

            ::

                 { "config":{ "workerConfig":{ "numInstances":"5" } } }

            Similarly, to change the number of preemptible workers in a cluster to 5, the ``update_mask``
            parameter would be ``config.secondary_worker_config.num_instances``, and the ``PATCH`` request
            body would be set as follows:

            ::

                 { "config":{ "secondaryWorkerConfig":{ "numInstances":"5" } } }

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.FieldMask`
        :param graceful_decommission_timeout: Optional. Timeout for graceful YARN decommissioning. Graceful
            decommissioning allows removing nodes from the cluster without interrupting jobs in progress.
            Timeout specifies how long to wait for jobs in progress to finish before forcefully removing nodes
            (and potentially interrupting jobs). Default timeout is 0 (for forceful decommission), and the
            maximum allowed timeout is 1 day.

            Only supported on Dataproc image versions 1.2 and higher.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.Duration`
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``UpdateClusterRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        client = self.get_cluster_client(region=region)
        operation = await client.update_cluster(
            request={
                "project_id": project_id,
                "region": region,
                "cluster_name": cluster_name,
                "cluster": cluster,
                "update_mask": update_mask,
                "graceful_decommission_timeout": graceful_decommission_timeout,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    async def create_workflow_template(
        self,
        template: dict | WorkflowTemplate,
        project_id: str,
        region: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> WorkflowTemplate:
        """
        Creates new workflow template.

        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param template: The Dataproc workflow template to create. If a dict is provided,
            it must be of the same form as the protobuf message WorkflowTemplate.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        metadata = metadata or ()
        client = self.get_template_client(region)
        parent = f"projects/{project_id}/regions/{region}"
        return await client.create_workflow_template(
            request={"parent": parent, "template": template}, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def instantiate_workflow_template(
        self,
        template_name: str,
        project_id: str,
        region: str,
        version: int | None = None,
        request_id: str | None = None,
        parameters: dict[str, str] | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Instantiates a template and begins execution.

        :param template_name: Name of template to instantiate.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param version: Optional. The version of workflow template to instantiate. If specified,
            the workflow will be instantiated only if the current version of
            the workflow template has the supplied version.
            This option cannot be used to instantiate a previous version of
            workflow template.
        :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
            with the same tag from running. This mitigates risk of concurrent
            instances started due to retries.
        :param parameters: Optional. Map from parameter names to values that should be used for those
            parameters. Values may not exceed 100 characters.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        metadata = metadata or ()
        client = self.get_template_client(region)
        name = f"projects/{project_id}/regions/{region}/workflowTemplates/{template_name}"
        operation = await client.instantiate_workflow_template(
            request={"name": name, "version": version, "request_id": request_id, "parameters": parameters},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    async def instantiate_inline_workflow_template(
        self,
        template: dict | WorkflowTemplate,
        project_id: str,
        region: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Instantiates a template and begins execution.

        :param template: The workflow template to instantiate. If a dict is provided,
            it must be of the same form as the protobuf message WorkflowTemplate
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
            with the same tag from running. This mitigates risk of concurrent
            instances started due to retries.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        metadata = metadata or ()
        client = self.get_template_client(region)
        parent = f"projects/{project_id}/regions/{region}"
        operation = await client.instantiate_inline_workflow_template(
            request={"parent": parent, "template": template, "request_id": request_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_job(
        self,
        job_id: str,
        project_id: str,
        region: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """
        Gets the resource representation for a job in a project.

        :param job_id: Id of the Dataproc job
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        client = self.get_job_client(region=region)
        job = await client.get_job(
            request={"project_id": project_id, "region": region, "job_id": job_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job

    @GoogleBaseHook.fallback_to_default_project_id
    async def submit_job(
        self,
        job: dict | Job,
        project_id: str,
        region: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """
        Submits a job to a cluster.

        :param job: The job resource. If a dict is provided,
            it must be of the same form as the protobuf message Job
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
            with the same tag from running. This mitigates risk of concurrent
            instances started due to retries.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        client = self.get_job_client(region=region)
        return await client.submit_job(
            request={"project_id": project_id, "region": region, "job": job, "request_id": request_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def cancel_job(
        self,
        job_id: str,
        project_id: str,
        region: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """
        Starts a job cancellation request.

        :param project_id: Required. The ID of the Google Cloud project that the job belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param job_id: Required. The job ID.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_job_client(region=region)

        job = await client.cancel_job(
            request={"project_id": project_id, "region": region, "job_id": job_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job

    @GoogleBaseHook.fallback_to_default_project_id
    async def create_batch(
        self,
        region: str,
        project_id: str,
        batch: dict | Batch,
        batch_id: str | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Creates a batch workload.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param batch: Required. The batch to create.
        :param batch_id: Optional. The ID to use for the batch, which will become the final component
            of the batch's resource name.
            This value must be 4-63 characters. Valid characters are /[a-z][0-9]-/.
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``CreateBatchRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        parent = f"projects/{project_id}/regions/{region}"

        result = await client.create_batch(
            request={
                "parent": parent,
                "batch": batch,
                "batch_id": batch_id,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    async def delete_batch(
        self,
        batch_id: str,
        region: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes the batch workload resource.

        :param batch_id: Required. The ID to use for the batch, which will become the final component
            of the batch's resource name.
            This value must be 4-63 characters. Valid characters are /[a-z][0-9]-/.
        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        name = f"projects/{project_id}/regions/{region}/batches/{batch_id}"

        await client.delete_batch(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_batch(
        self,
        batch_id: str,
        region: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Batch:
        """
        Gets the batch workload resource representation.

        :param batch_id: Required. The ID to use for the batch, which will become the final component
            of the batch's resource name.
            This value must be 4-63 characters. Valid characters are /[a-z][0-9]-/.
        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        name = f"projects/{project_id}/regions/{region}/batches/{batch_id}"

        result = await client.get_batch(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    async def list_batches(
        self,
        region: str,
        project_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Lists batch workloads.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param page_size: Optional. The maximum number of batches to return in each response. The service may
            return fewer than this value. The default page size is 20; the maximum page size is 1000.
        :param page_token: Optional. A page token received from a previous ``ListBatches`` call.
            Provide this token to retrieve the subsequent page.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        parent = f"projects/{project_id}/regions/{region}"

        result = await client.list_batches(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
