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

import contextlib
import os
import re
import subprocess
import time
from typing import Any, Iterator

from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.security.kerberos import renew_from_kt
from airflow.utils.log.logging_mixin import LoggingMixin

with contextlib.suppress(ImportError, NameError):
    from airflow.providers.cncf.kubernetes import kube_client

ALLOWED_SPARK_BINARIES = ["spark-submit", "spark2-submit", "spark3-submit"]


class SparkSubmitHook(BaseHook, LoggingMixin):
    """
    Wrap the spark-submit binary to kick off a spark-submit job; requires "spark-submit" binary in the PATH.

    :param conf: Arbitrary Spark configuration properties
    :param spark_conn_id: The :ref:`spark connection id <howto/connection:spark>` as configured
        in Airflow administration. When an invalid connection_id is supplied, it will default
        to yarn.
    :param files: Upload additional files to the executor running the job, separated by a
        comma. Files will be placed in the working directory of each executor.
        For example, serialized objects.
    :param py_files: Additional python files used by the job, can be .zip, .egg or .py.
    :param archives: Archives that spark should unzip (and possibly tag with #ALIAS) into
        the application working directory.
    :param driver_class_path: Additional, driver-specific, classpath settings.
    :param jars: Submit additional jars to upload and place them in executor classpath.
    :param java_class: the main class of the Java application
    :param packages: Comma-separated list of maven coordinates of jars to include on the
        driver and executor classpaths
    :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
        while resolving the dependencies provided in 'packages'
    :param repositories: Comma-separated list of additional remote repositories to search
        for the maven coordinates given with 'packages'
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
        (Default: all the available cores on the worker)
    :param executor_cores: (Standalone, YARN and Kubernetes only) Number of cores per
        executor (Default: 2)
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
    :param keytab: Full path to the file that contains the keytab
    :param principal: The name of the kerberos principal used for keytab
    :param proxy_user: User to impersonate when submitting the application
    :param name: Name of the job (default airflow-spark)
    :param num_executors: Number of executors to launch
    :param status_poll_interval: Seconds to wait between polls of driver status in cluster
        mode (Default: 1)
    :param application_args: Arguments for the application being submitted
    :param env_vars: Environment variables for spark-submit. It
        supports yarn and k8s mode too.
    :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
    :param spark_binary: The command to use for spark submit.
                         Some distros may use spark2-submit or spark3-submit.
                         (will overwrite any spark_binary defined in the connection's extra JSON)
    :param properties_file: Path to a file from which to load extra properties. If not
                              specified, this will look for conf/spark-defaults.conf.
    :param queue: The name of the YARN queue to which the application is submitted.
                        (will overwrite any yarn queue defined in the connection's extra JSON)
    :param deploy_mode: Whether to deploy your driver on the worker nodes (cluster) or locally as an    client.
                        (will overwrite any deployment mode defined in the connection's extra JSON)
    :param use_krb5ccache: if True, configure spark to use ticket cache instead of relying
        on keytab for Kerberos login
    """

    conn_name_attr = "conn_id"
    default_conn_name = "spark_default"
    conn_type = "spark"
    hook_name = "Spark"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "login", "password"],
            "relabeling": {},
        }

    def __init__(
        self,
        conf: dict[str, Any] | None = None,
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
        name: str = "default-name",
        num_executors: int | None = None,
        status_poll_interval: int = 1,
        application_args: list[Any] | None = None,
        env_vars: dict[str, Any] | None = None,
        verbose: bool = False,
        spark_binary: str | None = None,
        properties_file: str | None = None,
        queue: str | None = None,
        deploy_mode: str | None = None,
        *,
        use_krb5ccache: bool = False,
    ) -> None:
        super().__init__()
        self._conf = conf or {}
        self._conn_id = conn_id
        self._files = files
        self._py_files = py_files
        self._archives = archives
        self._driver_class_path = driver_class_path
        self._jars = jars
        self._java_class = java_class
        self._packages = packages
        self._exclude_packages = exclude_packages
        self._repositories = repositories
        self._total_executor_cores = total_executor_cores
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._keytab = keytab
        self._principal = self._resolve_kerberos_principal(principal) if use_krb5ccache else principal
        self._use_krb5ccache = use_krb5ccache
        self._proxy_user = proxy_user
        self._name = name
        self._num_executors = num_executors
        self._status_poll_interval = status_poll_interval
        self._application_args = application_args
        self._env_vars = env_vars
        self._verbose = verbose
        self._submit_sp: Any | None = None
        self._yarn_application_id: str | None = None
        self._kubernetes_driver_pod: str | None = None
        self.spark_binary = spark_binary
        self._properties_file = properties_file
        self._queue = queue
        self._deploy_mode = deploy_mode
        self._connection = self._resolve_connection()
        self._is_yarn = "yarn" in self._connection["master"]
        self._is_kubernetes = "k8s" in self._connection["master"]
        if self._is_kubernetes and kube_client is None:
            raise RuntimeError(
                f"{self._connection['master']} specified by kubernetes dependencies are not installed!"
            )

        self._should_track_driver_status = self._resolve_should_track_driver_status()
        self._driver_id: str | None = None
        self._driver_status: str | None = None
        self._spark_exit_code: int | None = None
        self._env: dict[str, Any] | None = None

    def _resolve_should_track_driver_status(self) -> bool:
        """Check if we should track the driver status.

        If so, we should send subsequent spark-submit status requests after the
        initial spark-submit request.

        :return: if the driver status should be tracked
        """
        return "spark://" in self._connection["master"] and self._connection["deploy_mode"] == "cluster"

    def _resolve_connection(self) -> dict[str, Any]:
        # Build from connection master or default to yarn if not available
        conn_data = {
            "master": "yarn",
            "queue": None,
            "deploy_mode": None,
            "spark_binary": self.spark_binary or "spark-submit",
            "namespace": None,
        }

        try:
            # Master can be local, yarn, spark://HOST:PORT, mesos://HOST:PORT and
            # k8s://https://<HOST>:<PORT>
            conn = self.get_connection(self._conn_id)
            if conn.port:
                conn_data["master"] = f"{conn.host}:{conn.port}"
            else:
                conn_data["master"] = conn.host

            # Determine optional yarn queue from the extra field
            extra = conn.extra_dejson
            conn_data["queue"] = self._queue if self._queue else extra.get("queue")
            conn_data["deploy_mode"] = self._deploy_mode if self._deploy_mode else extra.get("deploy-mode")
            if not self.spark_binary:
                self.spark_binary = extra.get("spark-binary", "spark-submit")
                if self.spark_binary is not None and self.spark_binary not in ALLOWED_SPARK_BINARIES:
                    raise RuntimeError(
                        f"The spark-binary extra can be on of {ALLOWED_SPARK_BINARIES} and it"
                        f" was `{self.spark_binary}`. Please make sure your spark binary is one of the"
                        f" allowed ones and that it is available on the PATH"
                    )
            conn_spark_home = extra.get("spark-home")
            if conn_spark_home:
                raise RuntimeError(
                    "The `spark-home` extra is not allowed any more. Please make sure one of"
                    f" {ALLOWED_SPARK_BINARIES} is available on the PATH, and set `spark-binary`"
                    " if needed."
                )
            conn_data["spark_binary"] = self.spark_binary
            conn_data["namespace"] = extra.get("namespace")
        except AirflowException:
            self.log.info(
                "Could not load connection string %s, defaulting to %s", self._conn_id, conn_data["master"]
            )

        if "spark.kubernetes.namespace" in self._conf:
            conn_data["namespace"] = self._conf["spark.kubernetes.namespace"]

        return conn_data

    def get_conn(self) -> Any:
        pass

    def _get_spark_binary_path(self) -> list[str]:
        # Assume that spark-submit is present in the path to the executing user
        return [self._connection["spark_binary"]]

    def _mask_cmd(self, connection_cmd: str | list[str]) -> str:
        # Mask any password related fields in application args with key value pair
        # where key contains password (case insensitive), e.g. HivePassword='abc'
        connection_cmd_masked = re.sub(
            r"("
            r"\S*?"  # Match all non-whitespace characters before...
            r"(?:secret|password)"  # ...literally a "secret" or "password"
            # word (not capturing them).
            r"\S*?"  # All non-whitespace characters before either...
            r"(?:=|\s+)"  # ...an equal sign or whitespace characters
            # (not capturing them).
            r"(['\"]?)"  # An optional single or double quote.
            r")"  # This is the end of the first capturing group.
            r"(?:(?!\2\s).)*"  # All characters between optional quotes
            # (matched above); if the value is quoted,
            # it may contain whitespace.
            r"(\2)",  # Optional matching quote.
            r"\1******\3",
            " ".join(connection_cmd),
            flags=re.I,
        )

        return connection_cmd_masked

    def _build_spark_submit_command(self, application: str) -> list[str]:
        """
        Construct the spark-submit command to execute.

        :param application: command to append to the spark-submit command
        :return: full command to be executed
        """
        connection_cmd = self._get_spark_binary_path()

        # The url of the spark master
        connection_cmd += ["--master", self._connection["master"]]

        for key in self._conf:
            connection_cmd += ["--conf", f"{key}={self._conf[key]}"]
        if self._env_vars and (self._is_kubernetes or self._is_yarn):
            if self._is_yarn:
                tmpl = "spark.yarn.appMasterEnv.{}={}"
                # Allow dynamic setting of hadoop/yarn configuration environments
                self._env = self._env_vars
            else:
                tmpl = "spark.kubernetes.driverEnv.{}={}"
            for key in self._env_vars:
                connection_cmd += ["--conf", tmpl.format(key, str(self._env_vars[key]))]
        elif self._env_vars and self._connection["deploy_mode"] != "cluster":
            self._env = self._env_vars  # Do it on Popen of the process
        elif self._env_vars and self._connection["deploy_mode"] == "cluster":
            raise AirflowException("SparkSubmitHook env_vars is not supported in standalone-cluster mode.")
        if self._is_kubernetes and self._connection["namespace"]:
            connection_cmd += [
                "--conf",
                f"spark.kubernetes.namespace={self._connection['namespace']}",
            ]
        if self._properties_file:
            connection_cmd += ["--properties-file", self._properties_file]
        if self._files:
            connection_cmd += ["--files", self._files]
        if self._py_files:
            connection_cmd += ["--py-files", self._py_files]
        if self._archives:
            connection_cmd += ["--archives", self._archives]
        if self._driver_class_path:
            connection_cmd += ["--driver-class-path", self._driver_class_path]
        if self._jars:
            connection_cmd += ["--jars", self._jars]
        if self._packages:
            connection_cmd += ["--packages", self._packages]
        if self._exclude_packages:
            connection_cmd += ["--exclude-packages", self._exclude_packages]
        if self._repositories:
            connection_cmd += ["--repositories", self._repositories]
        if self._num_executors:
            connection_cmd += ["--num-executors", str(self._num_executors)]
        if self._total_executor_cores:
            connection_cmd += ["--total-executor-cores", str(self._total_executor_cores)]
        if self._executor_cores:
            connection_cmd += ["--executor-cores", str(self._executor_cores)]
        if self._executor_memory:
            connection_cmd += ["--executor-memory", self._executor_memory]
        if self._driver_memory:
            connection_cmd += ["--driver-memory", self._driver_memory]
        if self._keytab:
            connection_cmd += ["--keytab", self._keytab]
        if self._principal:
            connection_cmd += ["--principal", self._principal]
        if self._use_krb5ccache:
            if not os.getenv("KRB5CCNAME"):
                raise AirflowException(
                    "KRB5CCNAME environment variable required to use ticket ccache is missing."
                )
            connection_cmd += ["--conf", "spark.kerberos.renewal.credentials=ccache"]
        if self._proxy_user:
            connection_cmd += ["--proxy-user", self._proxy_user]
        if self._name:
            connection_cmd += ["--name", self._name]
        if self._java_class:
            connection_cmd += ["--class", self._java_class]
        if self._verbose:
            connection_cmd += ["--verbose"]
        if self._connection["queue"]:
            connection_cmd += ["--queue", self._connection["queue"]]
        if self._connection["deploy_mode"]:
            connection_cmd += ["--deploy-mode", self._connection["deploy_mode"]]

        # The actual script to execute
        connection_cmd += [application]

        # Append any application arguments
        if self._application_args:
            connection_cmd += self._application_args

        self.log.info("Spark-Submit cmd: %s", self._mask_cmd(connection_cmd))

        return connection_cmd

    def _build_track_driver_status_command(self) -> list[str]:
        """
        Construct the command to poll the driver status.

        :return: full command to be executed
        """
        curl_max_wait_time = 30
        spark_host = self._connection["master"]
        if spark_host.endswith(":6066"):
            spark_host = spark_host.replace("spark://", "http://")
            connection_cmd = [
                "/usr/bin/curl",
                "--max-time",
                str(curl_max_wait_time),
                f"{spark_host}/v1/submissions/status/{self._driver_id}",
            ]
            self.log.info(connection_cmd)

            # The driver id so we can poll for its status
            if not self._driver_id:
                raise AirflowException(
                    "Invalid status: attempted to poll driver status but no driver id is known. Giving up."
                )

        else:
            connection_cmd = self._get_spark_binary_path()

            # The url to the spark master
            connection_cmd += ["--master", self._connection["master"]]

            # The driver id so we can poll for its status
            if self._driver_id:
                connection_cmd += ["--status", self._driver_id]
            else:
                raise AirflowException(
                    "Invalid status: attempted to poll driver status but no driver id is known. Giving up."
                )

        self.log.debug("Poll driver status cmd: %s", connection_cmd)

        return connection_cmd

    def _resolve_kerberos_principal(self, principal: str | None) -> str:
        """Resolve kerberos principal if airflow > 2.8.

        TODO: delete when min airflow version >= 2.8 and import directly from airflow.security.kerberos
        """
        from packaging.version import Version

        from airflow.version import version

        if Version(version) < Version("2.8"):
            from airflow.utils.net import get_hostname

            return principal or airflow_conf.get_mandatory_value("kerberos", "principal").replace(
                "_HOST", get_hostname()
            )
        else:
            from airflow.security.kerberos import get_kerberos_principle

            return get_kerberos_principle(principal)

    def submit(self, application: str = "", **kwargs: Any) -> None:
        """
        Remote Popen to execute the spark-submit job.

        :param application: Submitted application, jar or py file
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        spark_submit_cmd = self._build_spark_submit_command(application)

        if self._env:
            env = os.environ.copy()
            env.update(self._env)
            kwargs["env"] = env

        self._submit_sp = subprocess.Popen(
            spark_submit_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=-1,
            universal_newlines=True,
            **kwargs,
        )

        self._process_spark_submit_log(iter(self._submit_sp.stdout))  # type: ignore
        returncode = self._submit_sp.wait()

        # Check spark-submit return code. In Kubernetes mode, also check the value
        # of exit code in the log, as it may differ.
        if returncode or (self._is_kubernetes and self._spark_exit_code != 0):
            if self._is_kubernetes:
                raise AirflowException(
                    f"Cannot execute: {self._mask_cmd(spark_submit_cmd)}. Error code is: {returncode}. "
                    f"Kubernetes spark exit code is: {self._spark_exit_code}"
                )
            else:
                raise AirflowException(
                    f"Cannot execute: {self._mask_cmd(spark_submit_cmd)}. Error code is: {returncode}."
                )

        self.log.debug("Should track driver: %s", self._should_track_driver_status)

        # We want the Airflow job to wait until the Spark driver is finished
        if self._should_track_driver_status:
            if self._driver_id is None:
                raise AirflowException(
                    "No driver id is known: something went wrong when executing the spark submit command"
                )

            # We start with the SUBMITTED status as initial status
            self._driver_status = "SUBMITTED"

            # Start tracking the driver status (blocking function)
            self._start_driver_status_tracking()

            if self._driver_status != "FINISHED":
                raise AirflowException(
                    f"ERROR : Driver {self._driver_id} badly exited with status {self._driver_status}"
                )

    def _process_spark_submit_log(self, itr: Iterator[Any]) -> None:
        """
        Process the log files and extract useful information out of it.

        If the deploy-mode is 'client', log the output of the submit command as those
        are the output logs of the Spark worker directly.

        Remark: If the driver needs to be tracked for its status, the log-level of the
        spark deploy needs to be at least INFO (log4j.logger.org.apache.spark.deploy=INFO)

        :param itr: An iterator which iterates over the input of the subprocess
        """
        # Consume the iterator
        for line in itr:
            line = line.strip()
            # If we run yarn cluster mode, we want to extract the application id from
            # the logs so we can kill the application when we stop it unexpectedly
            if self._is_yarn and self._connection["deploy_mode"] == "cluster":
                match = re.search("application[0-9_]+", line)
                if match:
                    self._yarn_application_id = match.group(0)
                    self.log.info("Identified spark driver id: %s", self._yarn_application_id)

            # If we run Kubernetes cluster mode, we want to extract the driver pod id
            # from the logs so we can kill the application when we stop it unexpectedly
            elif self._is_kubernetes:
                match = re.search(r"\s*pod name: ((.+?)-([a-z0-9]+)-driver)", line)
                if match:
                    self._kubernetes_driver_pod = match.group(1)
                    self.log.info("Identified spark driver pod: %s", self._kubernetes_driver_pod)

                # Store the Spark Exit code
                match_exit_code = re.search(r"\s*[eE]xit code: (\d+)", line)
                if match_exit_code:
                    self._spark_exit_code = int(match_exit_code.group(1))

            # if we run in standalone cluster mode and we want to track the driver status
            # we need to extract the driver id from the logs. This allows us to poll for
            # the status using the driver id. Also, we can kill the driver when needed.
            elif self._should_track_driver_status and not self._driver_id:
                match_driver_id = re.search(r"driver-[0-9\-]+", line)
                if match_driver_id:
                    self._driver_id = match_driver_id.group(0)
                    self.log.info("identified spark driver id: %s", self._driver_id)

            self.log.info(line)

    def _process_spark_status_log(self, itr: Iterator[Any]) -> None:
        """
        Parse the logs of the spark driver status query process.

        :param itr: An iterator which iterates over the input of the subprocess
        """
        driver_found = False
        valid_response = False
        # Consume the iterator
        for line in itr:
            line = line.strip()

            # A valid Spark status response should contain a submissionId
            if "submissionId" in line:
                valid_response = True

            # Check if the log line is about the driver status and extract the status.
            if "driverState" in line:
                self._driver_status = line.split(" : ")[1].replace(",", "").replace('"', "").strip()
                driver_found = True

            self.log.debug("spark driver status log: %s", line)

        if valid_response and not driver_found:
            self._driver_status = "UNKNOWN"

    def _start_driver_status_tracking(self) -> None:
        """
        Poll the driver based on self._driver_id to get the status.

        Finish successfully when the status is FINISHED.
        Finish failed when the status is ERROR/UNKNOWN/KILLED/FAILED.

        Possible status:

        SUBMITTED
            Submitted but not yet scheduled on a worker
        RUNNING
            Has been allocated to a worker to run
        FINISHED
            Previously ran and exited cleanly
        RELAUNCHING
            Exited non-zero or due to worker failure, but has not yet
            started running again
        UNKNOWN
            The status of the driver is temporarily not known due to
            master failure recovery
        KILLED
            A user manually killed this driver
        FAILED
            The driver exited non-zero and was not supervised
        ERROR
            Unable to run or restart due to an unrecoverable error
            (e.g. missing jar file)
        """
        # When your Spark Standalone cluster is not performing well
        # due to misconfiguration or heavy loads.
        # it is possible that the polling request will timeout.
        # Therefore we use a simple retry mechanism.
        missed_job_status_reports = 0
        max_missed_job_status_reports = 10

        # Keep polling as long as the driver is processing
        while self._driver_status not in ["FINISHED", "UNKNOWN", "KILLED", "FAILED", "ERROR"]:
            # Sleep for n seconds as we do not want to spam the cluster
            time.sleep(self._status_poll_interval)

            self.log.debug("polling status of spark driver with id %s", self._driver_id)

            poll_drive_status_cmd = self._build_track_driver_status_command()
            status_process: Any = subprocess.Popen(
                poll_drive_status_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=-1,
                universal_newlines=True,
            )

            self._process_spark_status_log(iter(status_process.stdout))
            returncode = status_process.wait()

            if returncode:
                if missed_job_status_reports < max_missed_job_status_reports:
                    missed_job_status_reports += 1
                else:
                    raise AirflowException(
                        f"Failed to poll for the driver status {max_missed_job_status_reports} times: "
                        f"returncode = {returncode}"
                    )

    def _build_spark_driver_kill_command(self) -> list[str]:
        """
        Construct the spark-submit command to kill a driver.

        :return: full command to kill a driver
        """
        # Assume that spark-submit is present in the path to the executing user
        connection_cmd = [self._connection["spark_binary"]]

        # The url to the spark master
        connection_cmd += ["--master", self._connection["master"]]

        # The actual kill command
        if self._driver_id:
            connection_cmd += ["--kill", self._driver_id]

        self.log.debug("Spark-Kill cmd: %s", connection_cmd)

        return connection_cmd

    def on_kill(self) -> None:
        """Kill Spark submit command."""
        self.log.debug("Kill Command is being called")

        if self._should_track_driver_status and self._driver_id:
            self.log.info("Killing driver %s on cluster", self._driver_id)

            kill_cmd = self._build_spark_driver_kill_command()
            with subprocess.Popen(kill_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as driver_kill:
                self.log.info(
                    "Spark driver %s killed with return code: %s", self._driver_id, driver_kill.wait()
                )

        if self._submit_sp and self._submit_sp.poll() is None:
            self.log.info("Sending kill signal to %s", self._connection["spark_binary"])
            self._submit_sp.kill()

            if self._yarn_application_id:
                kill_cmd = f"yarn application -kill {self._yarn_application_id}".split()
                env = {**os.environ, **(self._env or {})}
                if self._keytab is not None and self._principal is not None:
                    # we are ignoring renewal failures from renew_from_kt
                    # here as the failure could just be due to a non-renewable ticket,
                    # we still attempt to kill the yarn application
                    renew_from_kt(self._principal, self._keytab, exit_on_fail=False)
                    env = os.environ.copy()
                    ccacche = airflow_conf.get_mandatory_value("kerberos", "ccache")
                    env["KRB5CCNAME"] = ccacche

                with subprocess.Popen(
                    kill_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                ) as yarn_kill:
                    self.log.info("YARN app killed with return code: %s", yarn_kill.wait())

            if self._kubernetes_driver_pod:
                self.log.info("Killing pod %s on Kubernetes", self._kubernetes_driver_pod)

                # Currently only instantiate Kubernetes client for killing a spark pod.
                try:
                    import kubernetes

                    client = kube_client.get_kube_client()
                    api_response = client.delete_namespaced_pod(
                        self._kubernetes_driver_pod,
                        self._connection["namespace"],
                        body=kubernetes.client.V1DeleteOptions(),
                        pretty=True,
                    )

                    self.log.info("Spark on K8s killed with response: %s", api_response)

                except kube_client.ApiException:
                    self.log.exception("Exception when attempting to kill Spark on K8s")
