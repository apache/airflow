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

import shlex
import subprocess
import time
import uuid
from collections.abc import MutableSequence, Sequence
from typing import TYPE_CHECKING, Any

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import ServerError
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
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

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseAsyncHook, GoogleBaseHook
from airflow.version import version as airflow_version

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.operation_async import AsyncOperation
    from google.api_core.operations_v1.operations_client import OperationsClient
    from google.api_core.retry import Retry
    from google.api_core.retry_async import AsyncRetry
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.field_mask_pb2 import FieldMask
    from google.type.interval_pb2 import Interval


class DataprocResourceIsNotReadyError(AirflowException):
    """Raise when resource is not ready for create Dataproc cluster."""


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
        self.job: dict[str, Any] = {
            "job": {
                "reference": {"project_id": project_id, "job_id": name},
                "placement": {"cluster_name": cluster_name},
                "labels": {"airflow-version": "v" + airflow_version.replace(".", "-").replace("+", "-")},
                job_type: {},
            }
        }
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

    def add_query(self, query: str | list[str]) -> None:
        """
        Add query for Dataproc job.

        :param query: query for the job.
        """
        queries = self.job["job"][self.job_type].setdefault("query_list", {"queries": []})["queries"]
        if isinstance(query, str):
            queries.append(query)
        elif isinstance(query, list):
            queries.extend(query)

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
        :raises: ValueError
        """
        if main_class is not None and main_jar is not None:
            raise ValueError("Set either main_jar or main_class")
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
        Set Dataproc job name.

        Job name is sanitized, replacing dots by underscores.

        :param name: Job name.
        """
        sanitized_name = f"{name.replace('.', '_')}_{uuid.uuid4()!s:.8}"
        self.job["job"]["reference"]["job_id"] = sanitized_name

    def build(self) -> dict:
        """
        Return Dataproc job.

        :return: Dataproc job
        """
        return self.job


class DataprocHook(GoogleBaseHook):
    """
    Google Cloud Dataproc APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def get_cluster_client(self, region: str | None = None) -> ClusterControllerClient:
        """Create a ClusterControllerClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return ClusterControllerClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_template_client(self, region: str | None = None) -> WorkflowTemplateServiceClient:
        """Create a WorkflowTemplateServiceClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return WorkflowTemplateServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_job_client(self, region: str | None = None) -> JobControllerClient:
        """Create a JobControllerClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return JobControllerClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_batch_client(self, region: str | None = None) -> BatchControllerClient:
        """Create a BatchControllerClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        return BatchControllerClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_operations_client(self, region: str | None):
        """Create a OperationsClient."""
        return self.get_batch_client(region=region).transport.operations_client

    def dataproc_options_to_args(self, options: dict) -> list[str]:
        """
        Return a formatted cluster parameters from a dictionary of arguments.

        :param options: Dictionary with options
        :return: List of arguments
        """
        if not options:
            return []

        args: list[str] = []
        for attr, value in options.items():
            if value is None or (isinstance(value, bool) and value):
                args.append(f"--{attr}")
            elif isinstance(value, bool) and not value:
                continue
            elif isinstance(value, list):
                args.extend([f"--{attr}={v}" for v in value])
            else:
                args.append(f"--{attr}={value}")
        return args

    def _build_gcloud_command(self, command: list[str], parameters: dict[str, str]) -> list[str]:
        return [*command, *(self.dataproc_options_to_args(parameters))]

    def _create_dataproc_cluster_with_gcloud(self, cmd: list[str]) -> str:
        """Create a Dataproc cluster with a gcloud command and return the job's ID."""
        self.log.info("Executing command: %s", " ".join(shlex.quote(c) for c in cmd))
        success_code = 0

        with self.provide_authorized_gcloud():
            proc = subprocess.run(cmd, check=False, capture_output=True)

        if proc.returncode != success_code:
            stderr_last_20_lines = "\n".join(proc.stderr.decode().strip().splitlines()[-20:])
            raise AirflowException(
                f"Process exit with non-zero exit code. Exit code: {proc.returncode}. Error Details : "
                f"{stderr_last_20_lines}"
            )

        response = proc.stdout.decode().strip()
        return response

    def wait_for_operation(
        self,
        operation: Operation,
        timeout: float | None = None,
        result_retry: AsyncRetry | _MethodDefault | Retry = DEFAULT,
    ) -> Any:
        """Wait for a long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout, retry=result_retry)
        except Exception:
            error = operation.exception(timeout=timeout)
            if self.check_error_for_resource_is_not_ready_msg(error.message):
                raise DataprocResourceIsNotReadyError(error.message)
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
    ) -> Operation | str:
        """
        Create a cluster in a specified project.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region in which to handle the request.
        :param cluster_name: Name of the cluster to create.
        :param labels: Labels that will be assigned to created cluster.
        :param cluster_config: The cluster config to create. If a dict is
            provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.ClusterConfig`.
        :param virtual_cluster_config: The virtual cluster config, used when
            creating a Dataproc cluster that does not directly control the
            underlying compute resources, for example, when creating a
            Dataproc-on-GKE cluster with
            :class:`~google.cloud.dataproc_v1.types.VirtualClusterConfig`.
        :param request_id: A unique id used to identify the request. If the
            server receives two *CreateClusterRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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

        if virtual_cluster_config and "kubernetes_cluster_config" in virtual_cluster_config:
            kube_config = virtual_cluster_config["kubernetes_cluster_config"]["gke_cluster_config"]
            try:
                spark_engine_version = virtual_cluster_config["kubernetes_cluster_config"][
                    "kubernetes_software_config"
                ]["component_version"]["SPARK"]
            except KeyError:
                spark_engine_version = "latest"
            gke_cluster_name = kube_config["gke_cluster_target"].rsplit("/", 1)[1]
            gke_pools = kube_config["node_pool_target"][0]
            gke_pool_name = gke_pools["node_pool"].rsplit("/", 1)[1]
            gke_pool_role = gke_pools["roles"][0]
            gke_pool_machine_type = gke_pools["node_pool_config"]["config"]["machine_type"]
            gcp_flags = {
                "region": region,
                "gke-cluster": gke_cluster_name,
                "spark-engine-version": spark_engine_version,
                "pools": f"name={gke_pool_name},roles={gke_pool_role.lower()},machineType={gke_pool_machine_type},min=1,max=10",
                "setup-workload-identity": None,
            }
            cmd = self._build_gcloud_command(
                command=["gcloud", "dataproc", "clusters", "gke", "create", cluster_name],
                parameters=gcp_flags,
            )
            response = self._create_dataproc_cluster_with_gcloud(cmd=cmd)
            return response

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
    ) -> Operation:
        """
        Delete a cluster in a project.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region in which to handle the request.
        :param cluster_name: Name of the cluster to delete.
        :param cluster_uuid: If specified, the RPC should fail if cluster with
            the UUID does not exist.
        :param request_id: A unique id used to identify the request. If the
            server receives two *DeleteClusterRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
        tarball_gcs_dir: str | None = None,
        diagnosis_interval: dict | Interval | None = None,
        jobs: MutableSequence[str] | None = None,
        yarn_application_ids: MutableSequence[str] | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Get cluster diagnostic information.

        After the operation completes, the response contains the Cloud Storage URI of the diagnostic output report containing a summary of collected diagnostics.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region in which to handle the request.
        :param cluster_name: Name of the cluster.
        :param tarball_gcs_dir:  The output Cloud Storage directory for the diagnostic tarball. If not specified, a task-specific directory in the cluster's staging bucket will be used.
        :param diagnosis_interval: Time interval in which diagnosis should be carried out on the cluster.
        :param jobs: Specifies a list of jobs on which diagnosis is to be performed. Format: `projects/{project}/regions/{region}/jobs/{job}`
        :param yarn_application_ids: Specifies a list of yarn applications on which diagnosis is to be performed.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_cluster_client(region=region)
        result = client.diagnose_cluster(
            request={
                "project_id": project_id,
                "region": region,
                "cluster_name": cluster_name,
                "tarball_gcs_dir": tarball_gcs_dir,
                "diagnosis_interval": diagnosis_interval,
                "jobs": jobs,
                "yarn_application_ids": yarn_application_ids,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Cluster:
        """
        Get the resource representation for a cluster in a project.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param cluster_name: The cluster name.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
        List all regions/{region}/clusters in a project.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param filter_: To constrain the clusters to. Case-sensitive.
        :param page_size: The maximum number of resources contained in the
            underlying API response. If page streaming is performed
            per-resource, this parameter does not affect the return value. If
            page streaming is performed per-page, this determines the maximum
            number of resources in a page.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
    ) -> Operation:
        """
        Update a cluster in a project.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param cluster_name: The cluster name.
        :param cluster: Changes to the cluster. If a dict is provided, it must
            be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.Cluster`.
        :param update_mask: Specifies the path, relative to ``Cluster``, of the
            field to update. For example, to change the number of workers in a
            cluster to 5, this would be specified as
            ``config.worker_config.num_instances``, and the ``PATCH`` request
            body would specify the new value:

            .. code-block:: python

                {"config": {"workerConfig": {"numInstances": "5"}}}

            Similarly, to change the number of preemptible workers in a cluster
            to 5, this would be ``config.secondary_worker_config.num_instances``
            and the ``PATCH`` request body would be:

            .. code-block:: python

                {"config": {"secondaryWorkerConfig": {"numInstances": "5"}}}

            If a dict is provided, it must be of the same form as the protobuf
            message :class:`~google.cloud.dataproc_v1.types.FieldMask`.
        :param graceful_decommission_timeout: Timeout for graceful YARN
            decommissioning. Graceful decommissioning allows removing nodes from
            the cluster without interrupting jobs in progress. Timeout specifies
            how long to wait for jobs in progress to finish before forcefully
            removing nodes (and potentially interrupting jobs). Default timeout
            is 0 (for forceful decommission), and the maximum allowed timeout is
            one day.

            Only supported on Dataproc image versions 1.2 and higher.

            If a dict is provided, it must be of the same form as the protobuf
            message :class:`~google.cloud.dataproc_v1.types.Duration`.
        :param request_id: A unique id used to identify the request. If the
            server receives two *UpdateClusterRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
    def start_cluster(
        self,
        region: str,
        project_id: str,
        cluster_name: str,
        cluster_uuid: str | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Start a cluster in a project.

        :param region: Cloud Dataproc region to handle the request.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param cluster_name: The cluster name.
        :param cluster_uuid: The cluster UUID
        :param request_id: A unique id used to identify the request. If the
            server receives two *UpdateClusterRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: An instance of ``google.api_core.operation.Operation``
        """
        client = self.get_cluster_client(region=region)
        return client.start_cluster(
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

    @GoogleBaseHook.fallback_to_default_project_id
    def stop_cluster(
        self,
        region: str,
        project_id: str,
        cluster_name: str,
        cluster_uuid: str | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Start a cluster in a project.

        :param region: Cloud Dataproc region to handle the request.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param cluster_name: The cluster name.
        :param cluster_uuid: The cluster UUID
        :param request_id: A unique id used to identify the request. If the
            server receives two *UpdateClusterRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: An instance of ``google.api_core.operation.Operation``
        """
        client = self.get_cluster_client(region=region)
        return client.stop_cluster(
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
        Create a new workflow template.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param template: The Dataproc workflow template to create. If a dict is
            provided, it must be of the same form as the protobuf message
            WorkflowTemplate.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
    ) -> Operation:
        """
        Instantiate a template and begins execution.

        :param template_name: Name of template to instantiate.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param version: Version of workflow template to instantiate. If
            specified, the workflow will be instantiated only if the current
            version of the workflow template has the supplied version. This
            option cannot be used to instantiate a previous version of workflow
            template.
        :param request_id: A tag that prevents multiple concurrent workflow
            instances with the same tag from running. This mitigates risk of
            concurrent instances started due to retries.
        :param parameters: Map from parameter names to values that should be
            used for those parameters. Values may not exceed 100 characters.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
    ) -> Operation:
        """
        Instantiate a template and begin execution.

        :param template: The workflow template to instantiate. If a dict is
            provided, it must be of the same form as the protobuf message
            WorkflowTemplate.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param request_id: A tag that prevents multiple concurrent workflow
            instances with the same tag from running. This mitigates risk of
            concurrent instances started due to retries.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
        Poll a job to check if it has finished.

        :param job_id: Dataproc job ID.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param wait_time: Number of seconds between checks.
        :param timeout: How many seconds wait for job to be ready.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        state = None
        start = time.monotonic()
        while state not in (JobStatus.State.ERROR, JobStatus.State.DONE, JobStatus.State.CANCELLED):
            self.log.debug("Waiting for job %s to complete", job_id)
            if timeout and start + timeout < time.monotonic():
                raise AirflowException(f"Timeout: dataproc job {job_id} is not ready after {timeout}s")
            self.log.debug("Sleeping for %s seconds", wait_time)
            time.sleep(wait_time)
            try:
                job = self.get_job(project_id=project_id, region=region, job_id=job_id)
                state = job.status.state
                self.log.debug("Job %s is in state %s", job_id, state)
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
        Get the resource representation for a job in a project.

        :param job_id: Dataproc job ID.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
        Submit a job to a cluster.

        :param job: The job resource. If a dict is provided, it must be of the
            same form as the protobuf message Job.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param request_id: A tag that prevents multiple concurrent workflow
            instances with the same tag from running. This mitigates risk of
            concurrent instances started due to retries.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
        Start a job cancellation request.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param job_id: The job ID.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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
        Create a batch workload.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param batch: The batch to create.
        :param batch_id: The ID to use for the batch, which will become the
            final component of the batch's resource name. This value must be of
            4-63 characters. Valid characters are ``[a-z][0-9]-``.
        :param request_id: A unique id used to identify the request. If the
            server receives two *CreateBatchRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        self.log.debug("Creating batch: %s", batch)
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
        Delete the batch workload resource.

        :param batch_id: The batch ID.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        name = f"projects/{project_id}/locations/{region}/batches/{batch_id}"

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
        Get the batch workload resource representation.

        :param batch_id: The batch ID.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_batch_client(region)
        name = f"projects/{project_id}/locations/{region}/batches/{batch_id}"

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
        filter: str | None = None,
        order_by: str | None = None,
    ):
        """
        List batch workloads.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param page_size: The maximum number of batches to return in each
            response. The service may return fewer than this value. The default
            page size is 20; the maximum page size is 1000.
        :param page_token: A page token received from a previous ``ListBatches``
            call. Provide this token to retrieve the subsequent page.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :param filter: Result filters as specified in ListBatchesRequest
        :param order_by: How to order results as specified in ListBatchesRequest
        """
        client = self.get_batch_client(region)
        parent = f"projects/{project_id}/regions/{region}"

        result = client.list_batches(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "order_by": order_by,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def wait_for_batch(
        self,
        batch_id: str,
        region: str,
        project_id: str,
        wait_check_interval: int = 10,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Batch:
        """
        Wait for a batch job to complete.

        After submission of a batch job, the operator waits for the job to
        complete. This hook is, however, useful in the case when Airflow is
        restarted or the task pid is killed for any reason. In this case, the
        creation would happen again, catching the raised AlreadyExists, and fail
        to this function for waiting on completion.

        :param batch_id: The batch ID.
        :param region: Cloud Dataproc region to handle the request.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param wait_check_interval: The amount of time to pause between checks
            for job completion.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        state = None
        first_loop: bool = True
        while state not in [
            Batch.State.CANCELLED,
            Batch.State.FAILED,
            Batch.State.SUCCEEDED,
            Batch.State.STATE_UNSPECIFIED,
        ]:
            try:
                if not first_loop:
                    time.sleep(wait_check_interval)
                first_loop = False
                self.log.debug("Waiting for batch %s", batch_id)
                result = self.get_batch(
                    batch_id=batch_id,
                    region=region,
                    project_id=project_id,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )
                state = result.state
            except ServerError as err:
                self.log.info(
                    "Retrying. Dataproc API returned server error when waiting for batch id %s: %s",
                    batch_id,
                    err,
                )

        return result

    def check_error_for_resource_is_not_ready_msg(self, error_msg: str) -> bool:
        """Check that reason of error is resource is not ready."""
        key_words = ["The resource", "is not ready"]
        return all([word in error_msg for word in key_words])


class DataprocAsyncHook(GoogleBaseAsyncHook):
    """
    Asynchronous interaction with Google Cloud Dataproc APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    sync_hook_class = DataprocHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)
        self._cached_client: JobControllerAsyncClient | None = None

    async def get_cluster_client(self, region: str | None = None) -> ClusterControllerAsyncClient:
        """Create a ClusterControllerAsyncClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        sync_hook = await self.get_sync_hook()
        return ClusterControllerAsyncClient(
            credentials=sync_hook.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    async def get_template_client(self, region: str | None = None) -> WorkflowTemplateServiceAsyncClient:
        """Create a WorkflowTemplateServiceAsyncClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        sync_hook = await self.get_sync_hook()
        return WorkflowTemplateServiceAsyncClient(
            credentials=sync_hook.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    async def get_job_client(self, region: str | None = None) -> JobControllerAsyncClient:
        """Create a JobControllerAsyncClient."""
        if self._cached_client is None:
            client_options = None
            if region and region != "global":
                client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

            sync_hook = await self.get_sync_hook()
            self._cached_client = JobControllerAsyncClient(
                credentials=sync_hook.get_credentials(),
                client_info=CLIENT_INFO,
                client_options=client_options,
            )
        return self._cached_client

    async def get_batch_client(self, region: str | None = None) -> BatchControllerAsyncClient:
        """Create a BatchControllerAsyncClient."""
        client_options = None
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")

        sync_hook = await self.get_sync_hook()
        return BatchControllerAsyncClient(
            credentials=sync_hook.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    async def get_operations_client(self, region: str) -> OperationsClient:
        """Create a OperationsClient."""
        template_client = await self.get_template_client(region=region)
        return template_client.transport.operations_client

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Cluster:
        """
        Get a cluster.

        :param region: Cloud Dataproc region in which to handle the request.
        :param cluster_name: Name of the cluster to get.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_cluster_client(region=region)
        result = await client.get_cluster(
            request={"project_id": project_id, "region": region, "cluster_name": cluster_name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Create a cluster in a project.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region in which to handle the request.
        :param cluster_name: Name of the cluster to create.
        :param labels: Labels that will be assigned to created cluster.
        :param cluster_config: The cluster config to create. If a dict is
            provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.ClusterConfig`.
        :param virtual_cluster_config: The virtual cluster config, used when
            creating a Dataproc cluster that does not directly control the
            underlying compute resources, for example, when creating a
            Dataproc-on-GKE cluster with
            :class:`~google.cloud.dataproc_v1.types.VirtualClusterConfig`.
        :param request_id: A unique id used to identify the request. If the
            server receives two *CreateClusterRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
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

        client = await self.get_cluster_client(region=region)
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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Delete a cluster in a project.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region in which to handle the request.
        :param cluster_name: Name of the cluster to delete.
        :param cluster_uuid: If specified, the RPC should fail if cluster with
            the UUID does not exist.
        :param request_id: A unique id used to identify the request. If the
            server receives two *DeleteClusterRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_cluster_client(region=region)
        result = await client.delete_cluster(
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
        tarball_gcs_dir: str | None = None,
        diagnosis_interval: dict | Interval | None = None,
        jobs: MutableSequence[str] | None = None,
        yarn_application_ids: MutableSequence[str] | None = None,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Get cluster diagnostic information.

        After the operation completes, the response contains the Cloud Storage URI of the diagnostic output report containing a summary of collected diagnostics.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region in which to handle the request.
        :param cluster_name: Name of the cluster.
        :param tarball_gcs_dir:  The output Cloud Storage directory for the diagnostic tarball. If not specified, a task-specific directory in the cluster's staging bucket will be used.
        :param diagnosis_interval: Time interval in which diagnosis should be carried out on the cluster.
        :param jobs: Specifies a list of jobs on which diagnosis is to be performed. Format: `projects/{project}/regions/{region}/jobs/{job}`
        :param yarn_application_ids: Specifies a list of yarn applications on which diagnosis is to be performed.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_cluster_client(region=region)
        result = await client.diagnose_cluster(
            request={
                "project_id": project_id,
                "region": region,
                "cluster_name": cluster_name,
                "tarball_gcs_dir": tarball_gcs_dir,
                "diagnosis_interval": diagnosis_interval,
                "jobs": jobs,
                "yarn_application_ids": yarn_application_ids,
            },
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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        List all regions/{region}/clusters in a project.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param filter_: To constrain the clusters to. Case-sensitive.
        :param page_size: The maximum number of resources contained in the
            underlying API response. If page streaming is performed
            per-resource, this parameter does not affect the return value. If
            page streaming is performed per-page, this determines the maximum
            number of resources in a page.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_cluster_client(region=region)
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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Update a cluster in a project.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param cluster_name: The cluster name.
        :param cluster: Changes to the cluster. If a dict is provided, it must
            be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.Cluster`.
        :param update_mask: Specifies the path, relative to ``Cluster``, of the
            field to update. For example, to change the number of workers in a
            cluster to 5, this would be specified as
            ``config.worker_config.num_instances``, and the ``PATCH`` request
            body would specify the new value:

            .. code-block:: python

                {"config": {"workerConfig": {"numInstances": "5"}}}

            Similarly, to change the number of preemptible workers in a cluster
            to 5, this would be ``config.secondary_worker_config.num_instances``
            and the ``PATCH`` request body would be:

            .. code-block:: python

                {"config": {"secondaryWorkerConfig": {"numInstances": "5"}}}

            If a dict is provided, it must be of the same form as the protobuf
            message :class:`~google.cloud.dataproc_v1.types.FieldMask`.
        :param graceful_decommission_timeout: Timeout for graceful YARN
            decommissioning. Graceful decommissioning allows removing nodes from
            the cluster without interrupting jobs in progress. Timeout specifies
            how long to wait for jobs in progress to finish before forcefully
            removing nodes (and potentially interrupting jobs). Default timeout
            is 0 (for forceful decommission), and the maximum allowed timeout is
            one day.

            Only supported on Dataproc image versions 1.2 and higher.

            If a dict is provided, it must be of the same form as the protobuf
            message :class:`~google.cloud.dataproc_v1.types.Duration`.
        :param request_id: A unique id used to identify the request. If the
            server receives two *UpdateClusterRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        client = await self.get_cluster_client(region=region)
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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> WorkflowTemplate:
        """
        Create a new workflow template.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param template: The Dataproc workflow template to create. If a dict is
            provided, it must be of the same form as the protobuf message
            WorkflowTemplate.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = await self.get_template_client(region)
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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Instantiate a template and begins execution.

        :param template_name: Name of template to instantiate.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param version: Version of workflow template to instantiate. If
            specified, the workflow will be instantiated only if the current
            version of the workflow template has the supplied version. This
            option cannot be used to instantiate a previous version of workflow
            template.
        :param request_id: A tag that prevents multiple concurrent workflow
            instances with the same tag from running. This mitigates risk of
            concurrent instances started due to retries.
        :param parameters: Map from parameter names to values that should be
            used for those parameters. Values may not exceed 100 characters.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = await self.get_template_client(region)
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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Instantiate a template and begin execution.

        :param template: The workflow template to instantiate. If a dict is
            provided, it must be of the same form as the protobuf message
            WorkflowTemplate.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param request_id: A tag that prevents multiple concurrent workflow
            instances with the same tag from running. This mitigates risk of
            concurrent instances started due to retries.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = await self.get_template_client(region)
        parent = f"projects/{project_id}/regions/{region}"
        operation = await client.instantiate_inline_workflow_template(
            request={"parent": parent, "template": template, "request_id": request_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    async def get_operation(self, region, operation_name):
        operations_client = await self.get_operations_client(region)
        return await operations_client.get_operation(name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_job(
        self,
        job_id: str,
        project_id: str,
        region: str,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """
        Get the resource representation for a job in a project.

        :param job_id: Dataproc job ID.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_job_client(region=region)
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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """
        Submit a job to a cluster.

        :param job: The job resource. If a dict is provided, it must be of the
            same form as the protobuf message Job.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param request_id: A tag that prevents multiple concurrent workflow
            instances with the same tag from running. This mitigates risk of
            concurrent instances started due to retries.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_job_client(region=region)
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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """
        Start a job cancellation request.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param job_id: The job ID.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_job_client(region=region)

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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Create a batch workload.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param batch: The batch to create.
        :param batch_id: The ID to use for the batch, which will become the
            final component of the batch's resource name. This value must be of
            4-63 characters. Valid characters are ``[a-z][0-9]-``.
        :param request_id: A unique id used to identify the request. If the
            server receives two *CreateBatchRequest* requests with the same
            ID, the second request will be ignored, and an operation created
            for the first one and stored in the backend is returned.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_batch_client(region)
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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Delete the batch workload resource.

        :param batch_id: The batch ID.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_batch_client(region)
        name = f"projects/{project_id}/locations/{region}/batches/{batch_id}"

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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Batch:
        """
        Get the batch workload resource representation.

        :param batch_id: The batch ID.
        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_batch_client(region)
        name = f"projects/{project_id}/locations/{region}/batches/{batch_id}"

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
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        filter: str | None = None,
        order_by: str | None = None,
    ):
        """
        List batch workloads.

        :param project_id: Google Cloud project ID that the cluster belongs to.
        :param region: Cloud Dataproc region to handle the request.
        :param page_size: The maximum number of batches to return in each
            response. The service may return fewer than this value. The default
            page size is 20; the maximum page size is 1000.
        :param page_token: A page token received from a previous ``ListBatches``
            call. Provide this token to retrieve the subsequent page.
        :param retry: A retry object used to retry requests. If *None*, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. If *retry* is specified, the timeout applies to each
            individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :param filter: Result filters as specified in ListBatchesRequest
        :param order_by: How to order results as specified in ListBatchesRequest
        """
        client = await self.get_batch_client(region)
        parent = f"projects/{project_id}/regions/{region}"

        result = await client.list_batches(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "order_by": order_by,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
