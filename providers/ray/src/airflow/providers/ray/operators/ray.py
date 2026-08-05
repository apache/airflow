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

from __future__ import annotations

from datetime import timedelta
from functools import cached_property
from typing import Any

from ray.job_submission import JobStatus

from airflow.providers.ray.constants import TERMINAL_JOB_STATUSES
from airflow.providers.ray.exceptions import RayAirflowException
from airflow.providers.ray.hooks.ray import RayHook
from airflow.providers.ray.triggers.ray import RayJobTrigger
from airflow.sdk import BaseOperator, Context


class SetupRayCluster(BaseOperator):
    """
    Operator to set up a Ray cluster on Kubernetes.

    :param conn_id: The connection ID for the Ray cluster.
    :param ray_cluster_yaml: Path to the YAML file defining the Ray cluster.
    :param kuberay_version: Version of KubeRay to install. Defaults to "1.0.0".
    :param gpu_device_plugin_yaml: URL or path to the GPU device plugin YAML. Example value: https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml.
    :param update_if_exists: Whether to update the cluster if it already exists. Defaults to False.
    """

    def __init__(
        self,
        conn_id: str,
        ray_cluster_yaml: str,
        kuberay_version: str = "1.0.0",
        gpu_device_plugin_yaml: str = "",
        update_if_exists: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.ray_cluster_yaml = ray_cluster_yaml
        self.kuberay_version = kuberay_version
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml
        self.update_if_exists = update_if_exists

    @cached_property
    def hook(self) -> RayHook:
        """Lazily initialize and return the RayHook."""
        return RayHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> None:
        """
        Execute the setup of the Ray cluster.

        :param context: The context in which the operator is being executed.
        """
        self.log.info("Trying to setup the Ray cluster defined in %s", self.ray_cluster_yaml)
        self.hook.setup_ray_cluster(
            context=context,
            ray_cluster_yaml=self.ray_cluster_yaml,
            kuberay_version=self.kuberay_version,
            gpu_device_plugin_yaml=self.gpu_device_plugin_yaml,
            update_if_exists=self.update_if_exists,
        )
        self.log.info("Finished setting up the ray cluster.")


class DeleteRayCluster(BaseOperator):
    """
    Operator to delete a Ray cluster from Kubernetes.

    :param conn_id: The connection ID for the Ray cluster.
    :param ray_cluster_yaml: Path to the YAML file defining the Ray cluster.
    :param gpu_device_plugin_yaml: URL or path to the GPU device plugin YAML. Example value: https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml
    """

    def __init__(
        self,
        conn_id: str,
        ray_cluster_yaml: str,
        gpu_device_plugin_yaml: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.ray_cluster_yaml = ray_cluster_yaml
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml

    @cached_property
    def hook(self) -> RayHook:
        """Lazily initialize and return the RayHook."""
        return RayHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> None:
        """
        Execute the deletion of the Ray cluster.

        :param context: The context in which the operator is being executed.
        """
        self.log.info("Trying to delete the Ray cluster defined in %s", self.ray_cluster_yaml)
        self.hook.delete_ray_cluster(self.ray_cluster_yaml, self.gpu_device_plugin_yaml)
        self.log.info("Finished deleting the ray cluster.")


class SubmitRayJob(BaseOperator):
    """
    Operator to submit and monitor a Ray job.

    This operator handles the submission of a Ray job to a Ray cluster and monitors its status until completion.
    It supports deferring execution and resuming based on job status changes, making it suitable for long-running jobs.

    :param conn_id: The connection ID for the Ray cluster.
    :param entrypoint: The command or script to execute as the Ray job.
    :param runtime_env: The runtime environment configuration for the Ray job.
    :param num_cpus: Number of CPUs required for the job. Defaults to 0.
    :param num_gpus: Number of GPUs required for the job. Defaults to 0.
    :param memory: Amount of memory required for the job in bytes. Defaults to 0.
    :param resources: Additional custom resources required for the job. Defaults to None.
    :param ray_cluster_yaml: Path to the Ray cluster YAML configuration file. If provided, the operator will set up and tear down the cluster.
    :param kuberay_version: Version of KubeRay to use when setting up the Ray cluster. Defaults to "1.0.0".
    :param update_if_exists: Whether to update the Ray cluster if it already exists. Defaults to True.
    :param gpu_device_plugin_yaml: URL or path to the GPU device plugin YAML file. Example value: https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml
    :param fetch_logs: Whether to fetch logs from the Ray job. Defaults to True.
    :param wait_for_completion: Whether to wait for the job to complete before marking the task as finished. Defaults to True.
    :param job_timeout_seconds: Maximum time to wait for job completion in seconds. Defaults to 600 seconds. Set to 0 if you want the job to run indefinitely without timeouts.
    :param poll_interval: Interval between job status checks in seconds. Defaults to 60 seconds.
    :param xcom_task_key: XCom key to retrieve the dashboard URL. Defaults to None.
    """

    template_fields = (
        "conn_id",
        "entrypoint",
        "runtime_env",
        "num_cpus",
        "num_gpus",
        "memory",
        "xcom_task_key",
        "ray_cluster_yaml",
        "job_timeout_seconds",
    )

    def __init__(
        self,
        *,
        conn_id: str,
        entrypoint: str,
        runtime_env: dict[str, Any],
        num_cpus: int | float = 0,
        num_gpus: int | float = 0,
        memory: int | float = 0,
        resources: dict[str, Any] | None = None,
        ray_cluster_yaml: str | None = None,
        kuberay_version: str = "1.0.0",
        update_if_exists: bool = True,
        gpu_device_plugin_yaml: str = "",
        fetch_logs: bool = True,
        wait_for_completion: bool = True,
        job_timeout_seconds: int = 600,
        poll_interval: int = 60,
        xcom_task_key: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.entrypoint = entrypoint
        self.runtime_env = runtime_env
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.ray_resources = resources
        self.ray_cluster_yaml = ray_cluster_yaml
        self.update_if_exists = update_if_exists
        self.kuberay_version = kuberay_version
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml
        self.fetch_logs = fetch_logs
        self.wait_for_completion = wait_for_completion
        self.job_timeout_seconds = job_timeout_seconds
        self.poll_interval = poll_interval
        self.xcom_task_key = xcom_task_key
        self.dashboard_url: str | None = None
        self.job_id = ""

    def on_kill(self) -> None:
        """
        Delete the Ray job if the task is killed.

        This method is called when the task is externally killed. It ensures that the associated
        Ray job is also terminated to avoid orphaned jobs.
        """
        if self.job_id:
            self.log.info("Deleting Ray job %s due to task kill.", self.job_id)
            self.hook.delete_ray_job(self.dashboard_url, self.job_id)
        self._delete_cluster()

    @cached_property
    def hook(self) -> RayHook:
        """Lazily initialize and return the RayHook."""
        return RayHook(conn_id=self.conn_id)

    def _get_dashboard_url(self, context: Context) -> str | None:
        """
        Retrieve the Ray dashboard URL from XCom.

        :param context: The context in which the task is being executed.
        :return: The Ray dashboard URL if available, None otherwise.
        """
        if self.xcom_task_key:
            parts = self.xcom_task_key.split(".", 1)
            task: str | None
            key: str
            if len(parts) == 2:
                task, key = parts
            else:
                task, key = None, self.xcom_task_key

            ti = context["ti"]

            if task is None:
                current_task = context["task"]
                dashboard_url = ti.xcom_pull(task_ids=current_task.task_id, key=key)
            else:
                dashboard_url = ti.xcom_pull(task_ids=task, key=key)

            self.log.info("Dashboard URL retrieved from XCom: %s", dashboard_url)
            return str(dashboard_url) if dashboard_url is not None else None

        return None

    def _job_timeout(self) -> timedelta | None:
        """Convert the rendered timeout value to the duration expected by the Ray hook."""
        timeout_seconds = int(self.job_timeout_seconds)
        return timedelta(seconds=timeout_seconds) if timeout_seconds > 0 else None

    def _setup_cluster(self, context: Context) -> None:
        """
        Set up the Ray cluster if a cluster YAML is provided.

        :param context: The context in which the task is being executed.
        """
        if self.ray_cluster_yaml:
            try:
                self.hook.setup_ray_cluster(
                    context=context,
                    ray_cluster_yaml=self.ray_cluster_yaml,
                    kuberay_version=self.kuberay_version,
                    gpu_device_plugin_yaml=self.gpu_device_plugin_yaml,
                    update_if_exists=self.update_if_exists,
                )
            except Exception:
                self.log.info("Unable to setup the Ray cluster using %s", self.ray_cluster_yaml)
                self.log.exception("Exception details")
                self.log.info("Trying to delete any parts of the RayCluster that may have been spun up...")
                try:
                    self._delete_cluster()
                except Exception:
                    self.log.exception("Unable to clean up the partially created Ray cluster")
                raise
        else:
            self.log.info("Skipping setting up a Ray cluster because no `ray_cluster_yaml` was given.")

    def _delete_cluster(self) -> None:
        """Delete the Ray cluster if a cluster YAML is provided."""
        if self.ray_cluster_yaml:
            self.hook.delete_ray_cluster(
                ray_cluster_yaml=self.ray_cluster_yaml,
                gpu_device_plugin_yaml=self.gpu_device_plugin_yaml,
            )
        else:
            self.log.info("Skipping deleting the Ray cluster because no `ray_cluster_yaml` was given.")

    def execute(self, context: Context) -> str:
        """
        Execute the Ray job submission and monitoring.

        This method submits the Ray job to the cluster and, if configured to wait for completion,
        monitors the job status until it reaches a terminal state or times out.

        :param context: The context in which the task is being executed.
        :return: The job ID of the submitted Ray job.
        """
        self.log.info("::group:: (SubmitJob 1/5) Setup Cluster")
        if self.ray_cluster_yaml and not self.wait_for_completion:
            raise RayAirflowException(
                "wait_for_completion must be enabled when SubmitRayJob manages the Ray cluster lifecycle"
            )
        self._setup_cluster(context=context)
        self.log.info("::endgroup::")

        try:
            self.log.info("::group:: (SubmitJob 2/5) Identify Dashboard URL")
            self.dashboard_url = self._get_dashboard_url(context)
            self.log.info("::endgroup::")

            self.log.info("::group:: (SubmitJob 3/5) Submit job")
            self.job_id = self.hook.submit_ray_job(
                dashboard_url=self.dashboard_url,
                entrypoint=self.entrypoint,
                runtime_env=self.runtime_env,
                entrypoint_num_cpus=self.num_cpus,
                entrypoint_num_gpus=self.num_gpus,
                entrypoint_memory=self.memory,
                entrypoint_resources=self.ray_resources,
            )
            self.log.info("Ray job with id %s submitted", self.job_id)
            self.log.info("::endgroup::")

            if not self.wait_for_completion:
                return self.job_id

            self.log.info("::group:: (SubmitJob 4/5) Wait for completion")
            current_status = self.hook.get_ray_job_status(self.dashboard_url, self.job_id)
            self.log.info("Current job status for %s is: %s", self.job_id, current_status)

        except Exception:
            try:
                self._delete_cluster()
            except Exception:
                self.log.exception("Unable to clean up the Ray cluster after job submission failed")
            raise

        if current_status in TERMINAL_JOB_STATUSES:
            self.execute_complete(
                context,
                {
                    "status": current_status,
                    "message": f"Job {self.job_id} completed with status {current_status}",
                    "job_id": self.job_id,
                },
            )
            return self.job_id

        self.log.info("Deferring the polling to RayJobTrigger...")
        self.defer(
            trigger=RayJobTrigger(
                job_id=self.job_id,
                conn_id=self.conn_id,
                xcom_dashboard_url=self.dashboard_url,
                ray_cluster_yaml=self.ray_cluster_yaml,
                gpu_device_plugin_yaml=self.gpu_device_plugin_yaml,
                poll_interval=self.poll_interval,
                fetch_logs=self.fetch_logs,
            ),
            method_name="execute_complete",
            timeout=self._job_timeout(),
        )

        return self.job_id

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Handle the completion of a deferred Ray job execution.

        This method is called when the deferred job execution completes. It processes the final
        job status and raises exceptions for failed or cancelled jobs. It finally deletes the cluster when the ray spec is provided

        :param context: The context in which the task is being executed.
        :param event: The event containing the job execution result.
        :raises RayAirflowException: If the job execution fails, is cancelled, or reaches an unexpected state.
        """
        self.log.info("::endgroup::")
        self.log.info("::group:: (SubmitJob 5/5) Execution completed")

        self._delete_cluster()

        job_status = event["status"]
        if job_status == JobStatus.SUCCEEDED:
            self.log.info("Job %s completed successfully", self.job_id)
            return
        self.log.info("Ray job %s execution not completed successfully...", self.job_id)
        if job_status in (JobStatus.FAILED, JobStatus.STOPPED):
            msg = f"Job {self.job_id} {job_status.lower()}: {event['message']}"
        elif job_status == "EXCEPTION":
            msg = f"Unable to monitor job {self.job_id}: {event['message']}"
        else:
            msg = f"Encountered unexpected state `{job_status}` for job_id `{self.job_id}`"

        self.log.info("::endgroup::")

        raise RayAirflowException(msg)
