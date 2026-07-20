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
from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.apache.spark.hooks.spark_submit import _K8S_WAIT_APP_COMPLETION_CONF, SparkSubmitHook
from airflow.providers.common.compat.openlineage.utils.spark import (
    inject_parent_job_information_into_spark_properties,
    inject_transport_information_into_spark_properties,
)
from airflow.providers.common.compat.sdk import BaseOperator, conf

try:
    from airflow.providers.cncf.kubernetes import kube_client
except ImportError:
    kube_client = None  # type: ignore[assignment]

try:
    from airflow.sdk import ResumableJobMixin
except ImportError:
    # Airflow 2 compat.
    # ResumableJobMixin does not exist in Airflow 2, so we need to add a stub to make it
    # behave as before
    class ResumableJobMixin:  # type: ignore[no-redef]
        """Airflow 2 stub — no task_state_store, always submits fresh."""

        external_id_key: str = "remote_job_id"

        def __init__(self, *, durable: bool = True, **kwargs: Any) -> None:
            # Accept durable so the kwarg doesn't leak to BaseOperator; crash recovery is a no-op here.
            super().__init__(**kwargs)
            self.durable = durable

        def execute_resumable(self, context):
            external_id = self.submit_job(context)
            self.poll_until_complete(external_id, context)
            return self.get_job_result(external_id, context)


if TYPE_CHECKING:
    from pydantic import JsonValue
    from requests.auth import AuthBase

    from airflow.providers.common.compat.sdk import Context


class SparkSubmitOperator(ResumableJobMixin, BaseOperator):
    """
    Wrap the spark-submit binary to kick off a spark-submit job; requires "spark-submit" binary in the PATH.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SparkSubmitOperator`

    :param application: The application that submitted as a job, either jar or py file. (templated)
    :param conf: Arbitrary Spark configuration properties (templated)
    :param conn_id: The :ref:`spark connection id <howto/connection:spark>` as configured
        in Airflow administration. When an invalid connection_id is supplied, it will default to yarn.
    :param files: Upload additional files to the executor running the job, separated by a
                  comma. Files will be placed in the working directory of each executor.
                  For example, serialized objects. (templated)
    :param py_files: Additional python files used by the job, can be .zip, .egg or .py. (templated)
    :param jars: Submit additional jars to upload and place them in driver and executor classpaths. (templated)
    :param driver_class_path: Additional, driver-specific, classpath settings. (templated)
    :param java_class: the main class of the Java application
    :param packages: Comma-separated list of maven coordinates of jars to include on the
                     driver and executor classpaths. (templated)
    :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
                             while resolving the dependencies provided in 'packages' (templated)
    :param repositories: Comma-separated list of additional remote repositories to search
                         for the maven coordinates given with 'packages'
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
                                 (Default: all the available cores on the worker)
    :param executor_cores: (Standalone & YARN only) Number of cores per executor (Default: 2)
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
    :param keytab: Full path to the file that contains the keytab (templated)
                        (will overwrite any keytab defined in the connection's extra JSON)
    :param principal: The name of the kerberos principal used for keytab (templated)
                        (will overwrite any principal defined in the connection's extra JSON)
    :param proxy_user: User to impersonate when submitting the application (templated)
    :param name: Name of the job (default airflow-spark). (templated)
    :param num_executors: Number of executors to launch
    :param status_poll_interval: Seconds to wait between polls of driver status in cluster
        mode. Used both by the Spark standalone driver-status tracker and (when
        ``yarn_track_via_rm_api=True``) by the YARN ResourceManager REST API
        polling loop. The YARN ResourceManager REST API polling loop uses at
        least 10 seconds to avoid flooding the ResourceManager on long-running
        jobs (Default: 1).
    :param application_args: Arguments for the application being submitted (templated)
    :param env_vars: Environment variables for spark-submit. It supports yarn and k8s mode too. (templated)
    :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
    :param spark_binary: The command to use for spark submit.
                         Some distros may use spark2-submit or spark3-submit.
                         (will overwrite any spark_binary defined in the connection's extra JSON)
    :param properties_file: Path to a file from which to load extra properties. If not
                              specified, this will look for conf/spark-defaults.conf.
    :param yarn_queue: The name of the YARN queue to which the application is submitted.
                        (will overwrite any yarn queue defined in the connection's extra JSON)
    :param deploy_mode: Whether to deploy your driver on the worker nodes (cluster) or locally as a client.
                        (will overwrite any deployment mode defined in the connection's extra JSON)
    :param use_krb5ccache: if True, configure spark to use ticket cache instead of relying
                           on keytab for Kerberos login
    :param post_submit_commands: Optional list of shell commands to run after the Spark job finishes.
        Useful for cleaning up sidecars such as Istio. Failures produce a warning but do not fail the task.
    :param track_driver_via_k8s_api: If True (when master is Kubernetes and ``deploy_mode``
        is ``cluster``), release the ``spark-submit`` JVM once the driver pod has been
        created, then poll the Kubernetes API for the pod phase until the application
        reaches a terminal state. The polling interval is controlled by
        ``status_poll_interval`` with a 20-second minimum. This frees the worker from
        holding the long-lived submit JVM. Defaults to ``False``.
    :param yarn_track_via_rm_api: If True (when master is YARN and ``deploy_mode``
        is ``cluster``), release the ``spark-submit`` JVM once the application has
        been submitted to YARN, then poll the YARN ResourceManager REST API
        (``GET /ws/v1/cluster/apps/{appId}``) until the application reaches a
        final state. The polling interval is controlled by ``status_poll_interval``
        with a 10-second minimum. This frees the worker from holding the
        long-lived submit JVM. Requires the Spark connection's ``extra``
        JSON to set ``yarn_resourcemanager_webapp_address`` (e.g. ``http://rm:8088``).
        Cluster-side driver logs should be used after the switch to polling.
        Defaults to ``False``.
    :param yarn_rm_auth: Optional ``requests.auth.AuthBase`` instance used for every
        call to the YARN ResourceManager REST API (status polling and kill). When
        omitted, Kerberos-enabled Spark connections with both ``keytab`` and
        ``principal`` configured use ``requests-kerberos`` automatically.
        Defaults to ``None`` (no auth for non-Kerberos connections).
    :param durable: When ``True`` (the default), the external job ID is persisted to task state
        store before polling begins so that a worker crash and retry reconnects to the existing job
        instead of submitting a fresh one. Set to ``False`` to always submit a new job on retry.
    """

    # Generic key used across all Spark deployment modes (standalone driver ID,
    # YARN application ID, K8s driver pod name).
    external_id_key = "spark_job_id"

    # Used only for k8s cluster mode. Caches the pod phase ("Succeeded" / "Failed") to task_store at the end of
    # poll_until_complete. On retry, get_job_status reads this before querying the K8s API
    # so that a completed job can be identified even after the driver pod is garbage collected.
    _K8S_DRIVER_STATUS_KEY = "k8s_driver_status"

    template_fields: Sequence[str] = (
        "application",
        "conf",
        "files",
        "py_files",
        "jars",
        "driver_class_path",
        "packages",
        "exclude_packages",
        "keytab",
        "principal",
        "proxy_user",
        "name",
        "application_args",
        "env_vars",
        "post_submit_commands",
        "properties_file",
    )

    def __init__(
        self,
        *,
        application: str = "",
        conf: dict[Any, Any] | None = None,
        conn_id: str = "spark_default",
        files: str | None = None,
        py_files: str | None = None,
        archives: str | None = None,
        driver_class_path: str | None = None,
        jars: str | None = None,
        java_class: str | None = None,
        packages: str | None = None,
        exclude_packages: str | None = None,
        repositories: str | None = None,
        total_executor_cores: int | None = None,
        executor_cores: int | None = None,
        executor_memory: str | None = None,
        driver_memory: str | None = None,
        keytab: str | None = None,
        principal: str | None = None,
        proxy_user: str | None = None,
        name: str = "arrow-spark",
        num_executors: int | None = None,
        status_poll_interval: int = 1,
        application_args: list[Any] | None = None,
        env_vars: dict[str, Any] | None = None,
        verbose: bool = False,
        spark_binary: str | None = None,
        properties_file: str | None = None,
        yarn_queue: str | None = None,
        deploy_mode: str | None = None,
        use_krb5ccache: bool = False,
        post_submit_commands: list[str] | None = None,
        track_driver_via_k8s_api: bool = False,
        yarn_track_via_rm_api: bool = False,
        yarn_rm_auth: AuthBase | None = None,
        openlineage_inject_parent_job_info: bool = conf.getboolean(
            "openlineage", "spark_inject_parent_job_info", fallback=False
        ),
        openlineage_inject_transport_info: bool = conf.getboolean(
            "openlineage", "spark_inject_transport_info", fallback=False
        ),
        reconnect_on_retry: bool | None = None,
        **kwargs: Any,
    ) -> None:
        if reconnect_on_retry is not None:
            warnings.warn(
                "reconnect_on_retry is renamed to durable.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            kwargs.setdefault("durable", reconnect_on_retry)
        super().__init__(**kwargs)
        self.application = application
        self.conf = conf
        self.files = files
        self.py_files = py_files
        self._archives = archives
        self.driver_class_path = driver_class_path
        self.jars = jars
        self._java_class = java_class
        self.packages = packages
        self.exclude_packages = exclude_packages
        self._repositories = repositories
        self._total_executor_cores = total_executor_cores
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self.keytab = keytab
        self.principal = principal
        self.proxy_user = proxy_user
        self.name = name
        self._num_executors = num_executors
        self._status_poll_interval = status_poll_interval
        self.application_args = application_args
        self.env_vars = env_vars
        self._verbose = verbose
        self._spark_binary = spark_binary
        self.properties_file = properties_file
        self._yarn_queue = yarn_queue
        self._deploy_mode = deploy_mode
        self._hook: SparkSubmitHook | None = None
        self.post_submit_commands = post_submit_commands
        self._post_submit_commands = list(post_submit_commands) if post_submit_commands else []
        self._conn_id = conn_id
        self._use_krb5ccache = use_krb5ccache
        self._yarn_track_via_rm_api = yarn_track_via_rm_api
        self._yarn_rm_auth = yarn_rm_auth

        self._track_driver_via_k8s_api = track_driver_via_k8s_api
        self._openlineage_inject_parent_job_info = openlineage_inject_parent_job_info
        self._openlineage_inject_transport_info = openlineage_inject_transport_info

    def execute(self, context: Context) -> None:
        """Call the SparkSubmitHook to run the provided spark job."""
        self.conf = self.conf or {}
        if self._openlineage_inject_parent_job_info:
            self.log.debug("Injecting OpenLineage parent job information into Spark properties.")
            self.conf = inject_parent_job_information_into_spark_properties(self.conf, context)
        if self._openlineage_inject_transport_info:
            self.log.debug("Injecting OpenLineage transport information into Spark properties.")
            self.conf = inject_transport_information_into_spark_properties(self.conf, context)
        if self._hook is None:
            self._hook = self._get_hook()
        hook = self._hook
        if self._track_driver_via_k8s_api:
            hook._validate_track_driver_via_k8s_api_config()
        if hook._should_track_driver_status:
            return self.execute_resumable(context)
        if hook._should_track_driver_via_k8s_api():
            return self.execute_resumable(context)
        if hook._is_yarn_cluster_mode:
            if self.durable and not hook._yarn_track_via_rm_api:
                raise ValueError(
                    "YARN cluster mode with durable=True requires yarn_track_via_rm_api=True. "
                    "The RM REST API is needed to check application status on retry."
                )
            if hook._yarn_track_via_rm_api:
                hook._validate_yarn_track_via_rm_api_config()
                return self.execute_resumable(context)
        hook.submit(self.application)

    def submit_job(self, context: Context) -> str | None:
        if self._hook is None:
            self._hook = self._get_hook()
        if self._hook._is_kubernetes:
            self._hook._conf[_K8S_WAIT_APP_COMPLETION_CONF] = "false"
            self._hook.submit(self.application)
            pod_name = self._hook._kubernetes_driver_pod
            namespace = self._hook._connection["namespace"]
            if not pod_name:
                raise RuntimeError("spark-submit did not capture a K8s driver pod name")
            external_id = f"{namespace}:{pod_name}"
            self.log.info("Spark K8s driver pod submitted: %s", external_id)
            return external_id
        if self._hook._is_yarn_cluster_mode:
            if self._hook._conf.get("spark.yarn.submit.waitAppCompletion", "").strip().lower() == "true":
                raise ValueError(
                    "spark.yarn.submit.waitAppCompletion=true cannot be set for cluster mode as it conflicts"
                    "with the need to exit spark-submit immediately to persist the application ID for tracking. "
                    "Either remove the explicit conf or set durable=False."
                )
            self._hook._conf["spark.yarn.submit.waitAppCompletion"] = "false"
            self._hook.submit(self.application)
            app_id = self._hook._yarn_application_id
            if not app_id:
                raise RuntimeError("spark-submit did not produce a YARN application ID")
            self.log.info("YARN application submitted: %s", app_id)
            return app_id
        driver_id = self._hook.submit(self.application)
        if not driver_id:
            raise RuntimeError("spark-submit did not return a driver ID")
        self.log.info("Spark driver submitted: %s", driver_id)
        return driver_id

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        # called from submit_job which always returns a str (Spark driver IDs are strings)
        external_id = cast("str", external_id)
        if self._hook is None:
            self._hook = self._get_hook()
        if self._hook._is_yarn_cluster_mode:
            return self._hook.query_yarn_application_status(external_id)
        if self._hook._is_kubernetes:
            if (task_state_store := context.get("task_state_store")) is not None:
                if (cached := task_state_store.get(self._K8S_DRIVER_STATUS_KEY)) is not None:
                    if not isinstance(cached, str):
                        raise ValueError(f"Cached K8s driver status is not a string: {cached!r}")
                    return cached
            if kube_client is None:
                raise RuntimeError(
                    "apache-airflow-providers-cncf-kubernetes is required to query K8s pod status"
                )
            namespace, pod_name = self._parse_k8s_external_id(external_id)
            try:
                client = kube_client.get_kube_client()
                pod = client.read_namespaced_pod(pod_name, namespace)
                return pod.status.phase or "Pending"
            except kube_client.ApiException as e:
                if e.status == 404:
                    return "NotFound"
                raise
        scheme = self._hook._connection.get("rest_scheme", "http")
        rest_port = self._hook._connection.get("rest_port", 6066)
        # HA master URLs can look like spark://m1:7077,m2:7077 — try each host in order.
        # The master URL port (e.g. 7077) is the RPC port — not the REST API port.
        # Use rest-port connection extra to override spark.master.rest.port (default 6066).
        master_urls = self._hook._connection["master"].replace("spark://", "").split(",")
        last_exc: Exception = RuntimeError("No Spark masters to query")
        for m in master_urls:
            host = m.strip().split(":")[0]
            url = f"{scheme}://{host}:{rest_port}/v1/submissions/status/{external_id}"
            try:
                status = self._fetch_driver_status(url, external_id)
                return status
            except Exception as e:
                self.log.warning("Could not reach Spark master %s: %s", host, e)
                last_exc = e
        raise last_exc

    @staticmethod
    def _parse_k8s_external_id(external_id: str) -> tuple[str, str]:
        """Parse a K8s external ID of the form 'namespace:pod_name' into its components."""
        parts = external_id.split(":", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid K8s external ID format {external_id!r}; expected 'namespace:pod_name'")
        return parts[0], parts[1]

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1), reraise=True)
    def _fetch_driver_status(self, url: str, external_id: str) -> str:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        # "success:false" means the master does not recognise the driver ID or is in recovery.
        # https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/deploy/master/DriverState.scala
        data = response.json()
        if not data.get("success"):
            raise RuntimeError(
                f"Spark REST API returned failure for {external_id}: {data.get('message', 'unknown error')}"
            )
        status = data["driverState"]
        self.log.info("Driver %s status: %s", external_id, status)
        return status

    def is_job_active(self, status: str) -> bool:
        if self._hook is None:
            self._hook = self._get_hook()
        status = status.upper()
        if self._hook._is_yarn_cluster_mode:
            # https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
            return status in {"NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING"}
        if self._hook._is_kubernetes:
            return status in ("PENDING", "RUNNING")
        # RELAUNCHING: driver is being restarted after a failure, still alive.
        # UNKNOWN: master is in failure recovery, state is temporarily unavailable.
        # https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/deploy/master/DriverState.scala
        return status in ("SUBMITTED", "RUNNING", "RELAUNCHING", "UNKNOWN")

    def is_job_succeeded(self, status: str) -> bool:
        if self._hook is None:
            self._hook = self._get_hook()
        status = status.upper()
        if self._hook._is_yarn_cluster_mode:
            return status == "SUCCEEDED"
        if self._hook._is_kubernetes:
            return status == "SUCCEEDED"
        # standalone and YARN both use FINISHED
        return status == "FINISHED"

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        # called from submit_job which always returns a str (Spark driver IDs are strings)
        external_id = cast("str", external_id)
        if self._hook is None:
            self._hook = self._get_hook()
        if self._hook._is_yarn_cluster_mode:
            try:
                self._hook._start_yarn_application_status_tracking(external_id)
            finally:
                self._hook._run_post_submit_commands()
            return
        if self._hook._is_kubernetes:
            if external_id is not None:
                _, pod_name = self._parse_k8s_external_id(external_id)
                self._hook._kubernetes_driver_pod = pod_name
            terminal_phase = self._hook._poll_k8s_driver_via_api()
            # Cache only when the pod actually reached Succeeded, the 404/vanished path
            # returns None for cases like: pod deleted by on_kill or garbage collected after failure)
            # and must not be cached, otherwise a retry would see "Succeeded" and skip resubmission.
            if terminal_phase == "Succeeded" and self.durable:
                if (task_state_store := context.get("task_state_store")) is not None:
                    task_state_store.set(self._K8S_DRIVER_STATUS_KEY, "Succeeded")
            return

        self.log.info("Polling driver %s until completion", external_id)
        self._hook._driver_id = external_id
        try:
            self._hook._start_driver_status_tracking()
            if self._hook._driver_status != "FINISHED":
                raise RuntimeError(f"Driver {external_id} exited with status {self._hook._driver_status}")
        finally:
            # post-submit commands must fire whether the job succeeded or failed.
            self._hook._run_post_submit_commands()

    def get_job_result(self, external_id: JsonValue, context: Context) -> None:
        return None

    def on_kill(self) -> None:
        if self._hook is None:
            self._hook = self._get_hook()
        if self._hook._is_yarn_cluster_mode and self._hook._yarn_application_id:
            # spark-submit has already exited (waitAppCompletion=false), so the hook's
            # CLI-based kill has nothing to terminate. Kill the YARN app via REST API instead.
            self._hook._kill_yarn_application(self._hook._yarn_application_id)
        else:
            self._hook.on_kill()

    def _get_hook(self) -> SparkSubmitHook:
        return SparkSubmitHook(
            conf=self.conf,
            conn_id=self._conn_id,
            files=self.files,
            py_files=self.py_files,
            archives=self._archives,
            driver_class_path=self.driver_class_path,
            jars=self.jars,
            java_class=self._java_class,
            packages=self.packages,
            exclude_packages=self.exclude_packages,
            repositories=self._repositories,
            total_executor_cores=self._total_executor_cores,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            keytab=self.keytab,
            principal=self.principal,
            proxy_user=self.proxy_user,
            name=self.name,
            num_executors=self._num_executors,
            status_poll_interval=self._status_poll_interval,
            application_args=self.application_args,
            env_vars=self.env_vars,
            verbose=self._verbose,
            spark_binary=self._spark_binary,
            properties_file=self.properties_file,
            yarn_queue=self._yarn_queue,
            deploy_mode=self._deploy_mode,
            use_krb5ccache=self._use_krb5ccache,
            post_submit_commands=self.post_submit_commands,
            track_driver_via_k8s_api=self._track_driver_via_k8s_api,
            yarn_track_via_rm_api=self._yarn_track_via_rm_api,
            yarn_rm_auth=self._yarn_rm_auth,
        )
