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
# This file provides better type hinting and editor autocompletion support for
# dynamically generated task decorators. Functions declared in this stub do not
# necessarily exist at run time. See "Creating Custom @task Decorators"
# documentation for more details.
from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable, Iterable, Mapping, overload

from kubernetes.client import models as k8s

from airflow.decorators.base import FParams, FReturn, Task, TaskDecorator
from airflow.decorators.branch_python import branch_task
from airflow.decorators.external_python import external_python_task
from airflow.decorators.python import python_task
from airflow.decorators.python_virtualenv import virtualenv_task
from airflow.decorators.sensor import sensor_task
from airflow.decorators.short_circuit import short_circuit_task
from airflow.decorators.task_group import task_group
from airflow.kubernetes.secret import Secret
from airflow.models.dag import dag

# Please keep this in sync with __init__.py's __all__.
__all__ = [
    "TaskDecorator",
    "TaskDecoratorCollection",
    "dag",
    "task",
    "task_group",
    "python_task",
    "virtualenv_task",
    "external_python_task",
    "branch_task",
    "short_circuit_task",
    "sensor_task",
]

class TaskDecoratorCollection:
    @overload
    def python(
        self,
        *,
        multiple_outputs: bool | None = None,
        # 'python_callable', 'op_args' and 'op_kwargs' since they are filled by
        # _PythonDecoratedOperator.
        templates_dict: Mapping[str, Any] | None = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert the decorated callable to a task.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied.
        :param show_return_value_in_logs: a bool value whether to show return_value
            logs. Defaults to True, which allows return value log output.
            It can be set to False to prevent log output of return value when you return huge data
            such as transmission a large amount of XCom to TaskAPI.
        """
    # [START mixin_for_typing]
    @overload
    def python(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]: ...
    # [END mixin_for_typing]
    @overload
    def __call__(
        self,
        *,
        multiple_outputs: bool | None = None,
        templates_dict: Mapping[str, Any] | None = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Aliasing ``python``; signature should match exactly."""
    @overload
    def __call__(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]:
        """Aliasing ``python``; signature should match exactly."""
    @overload
    def virtualenv(
        self,
        *,
        multiple_outputs: bool | None = None,
        # 'python_callable', 'op_args' and 'op_kwargs' since they are filled by
        # _PythonVirtualenvDecoratedOperator.
        requirements: None | Iterable[str] | str = None,
        python_version: None | str | int | float = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        templates_dict: Mapping[str, Any] | None = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert the decorated callable to a virtual environment task.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param requirements: Either a list of requirement strings, or a (templated)
            "requirements file" as specified by pip.
        :param python_version: The Python version to run the virtualenv with. Note that
            both 2 and 2.7 are acceptable forms.
        :param use_dill: Whether to use dill to serialize
            the args and result (pickle is default). This allow more complex types
            but requires you to include dill in your requirements.
        :param system_site_packages: Whether to include
            system_site_packages in your virtualenv.
            See virtualenv documentation for more information.
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied.
        :param show_return_value_in_logs: a bool value whether to show return_value
            logs. Defaults to True, which allows return value log output.
            It can be set to False to prevent log output of return value when you return huge data
            such as transmission a large amount of XCom to TaskAPI.
        """
    @overload
    def virtualenv(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]: ...
    def external_python(
        self,
        *,
        python: str,
        multiple_outputs: bool | None = None,
        # 'python_callable', 'op_args' and 'op_kwargs' since they are filled by
        # _PythonVirtualenvDecoratedOperator.
        use_dill: bool = False,
        templates_dict: Mapping[str, Any] | None = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert the decorated callable to a virtual environment task.

        :param python: Full path string (file-system specific) that points to a Python binary inside
            a virtualenv that should be used (in ``VENV/bin`` folder). Should be absolute path
            (so usually start with "/" or "X:/" depending on the filesystem/os used).
        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param use_dill: Whether to use dill to serialize
            the args and result (pickle is default). This allow more complex types
            but requires you to include dill in your requirements.
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied.
        :param show_return_value_in_logs: a bool value whether to show return_value
            logs. Defaults to True, which allows return value log output.
            It can be set to False to prevent log output of return value when you return huge data
            such as transmission a large amount of XCom to TaskAPI.
        """
    @overload
    def branch(self, *, multiple_outputs: bool | None = None, **kwargs) -> TaskDecorator:
        """Create a decorator to wrap the decorated callable into a BranchPythonOperator.

        For more information on how to use this decorator, see :ref:`concepts:branching`.
        Accepts arbitrary for operator kwarg. Can be reused in a single DAG.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        """
    @overload
    def branch(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]: ...
    @overload
    def short_circuit(
        self,
        *,
        multiple_outputs: bool | None = None,
        ignore_downstream_trigger_rules: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to wrap the decorated callable into a ShortCircuitOperator.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param ignore_downstream_trigger_rules: If set to True, all downstream tasks from this operator task
            will be skipped. This is the default behavior. If set to False, the direct, downstream task(s)
            will be skipped but the ``trigger_rule`` defined for a other downstream tasks will be respected.
            Defaults to True.
        """
    @overload
    def short_circuit(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]: ...
    # [START decorator_signature]
    def docker(
        self,
        *,
        multiple_outputs: bool | None = None,
        use_dill: bool = False,  # Added by _DockerDecoratedOperator.
        python_command: str = "python3",
        # 'command', 'retrieve_output', and 'retrieve_output_path' are filled by
        # _DockerDecoratedOperator.
        image: str,
        api_version: str | None = None,
        container_name: str | None = None,
        cpus: float = 1.0,
        docker_url: str = "unix://var/run/docker.sock",
        environment: dict[str, str] | None = None,
        private_environment: dict[str, str] | None = None,
        force_pull: bool = False,
        mem_limit: float | str | None = None,
        host_tmp_dir: str | None = None,
        network_mode: str | None = None,
        tls_ca_cert: str | None = None,
        tls_client_cert: str | None = None,
        tls_client_key: str | None = None,
        tls_hostname: str | bool | None = None,
        tls_ssl_version: str | None = None,
        tmp_dir: str = "/tmp/airflow",
        user: str | int | None = None,
        mounts: list[str] | None = None,
        working_dir: str | None = None,
        xcom_all: bool = False,
        docker_conn_id: str | None = None,
        dns: list[str] | None = None,
        dns_search: list[str] | None = None,
        auto_remove: bool = False,
        shm_size: int | None = None,
        tty: bool = False,
        privileged: bool = False,
        cap_add: str | None = None,
        extra_hosts: dict[str, str] | None = None,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert the decorated callable to a Docker task.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param use_dill: Whether to use dill or pickle for serialization
        :param python_command: Python command for executing functions, Default: python3
        :param image: Docker image from which to create the container.
            If image tag is omitted, "latest" will be used.
        :param api_version: Remote API version. Set to ``auto`` to automatically
            detect the server's version.
        :param container_name: Name of the container. Optional (templated)
        :param cpus: Number of CPUs to assign to the container. This value gets multiplied with 1024.
        :param docker_url: URL of the host running the docker daemon.
            Default is unix://var/run/docker.sock
        :param environment: Environment variables to set in the container. (templated)
        :param private_environment: Private environment variables to set in the container.
            These are not templated, and hidden from the website.
        :param force_pull: Pull the docker image on every run. Default is False.
        :param mem_limit: Maximum amount of memory the container can use.
            Either a float value, which represents the limit in bytes,
            or a string like ``128m`` or ``1g``.
        :param host_tmp_dir: Specify the location of the temporary directory on the host which will
            be mapped to tmp_dir. If not provided defaults to using the standard system temp directory.
        :param network_mode: Network mode for the container.
            It can be one of the following:
            bridge - Create new network stack for the container with default docker bridge network
            None - No networking for this container
            container:<name|id> - Use the network stack of another container specified via <name|id>
            host - Use the host network stack. Incompatible with `port_bindings`
            '<network-name>|<network-id>' - Connects the container to user created network(using `docker
            network create` command)
        :param tls_ca_cert: Path to a PEM-encoded certificate authority
            to secure the docker connection.
        :param tls_client_cert: Path to the PEM-encoded certificate
            used to authenticate docker client.
        :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
        :param tls_hostname: Hostname to match against
            the docker server certificate or False to disable the check.
        :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
        :param tmp_dir: Mount point inside the container to
            a temporary directory created on the host by the operator.
            The path is also made available via the environment variable
            ``AIRFLOW_TMP_DIR`` inside the container.
        :param user: Default user inside the docker container.
        :param mounts: List of mounts to mount into the container, e.g.
            ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
        :param working_dir: Working directory to
            set on the container (equivalent to the -w switch the docker client)
        :param xcom_all: Push all the stdout or just the last line.
            The default is False (last line).
        :param docker_conn_id: ID of the Airflow connection to use
        :param dns: Docker custom DNS servers
        :param dns_search: Docker custom DNS search domain
        :param auto_remove: Auto-removal of the container on daemon side when the
            container's process exits.
            The default is False.
        :param shm_size: Size of ``/dev/shm`` in bytes. The size must be
            greater than 0. If omitted uses system default.
        :param tty: Allocate pseudo-TTY to the container
            This needs to be set see logs of the Docker container.
        :param privileged: Give extended privileges to this container.
        :param cap_add: Include container capabilities
        """
        # [END decorator_signature]
    def kubernetes(
        self,
        *,
        image: str,
        kubernetes_conn_id: str = ...,
        namespace: str = "default",
        name: str = ...,
        random_name_suffix: bool = True,
        ports: list[k8s.V1ContainerPort] | None = None,
        volume_mounts: list[k8s.V1VolumeMount] | None = None,
        volumes: list[k8s.V1Volume] | None = None,
        env_vars: list[k8s.V1EnvVar] | None = None,
        env_from: list[k8s.V1EnvFromSource] | None = None,
        secrets: list[Secret] | None = None,
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        labels: dict | None = None,
        reattach_on_restart: bool = True,
        startup_timeout_seconds: int = 120,
        get_logs: bool = True,
        image_pull_policy: str | None = None,
        annotations: dict | None = None,
        container_resources: k8s.V1ResourceRequirements | None = None,
        affinity: k8s.V1Affinity | None = None,
        config_file: str = ...,
        node_selector: dict | None = None,
        image_pull_secrets: list[k8s.V1LocalObjectReference] | None = None,
        service_account_name: str | None = None,
        is_delete_operator_pod: bool = True,
        hostnetwork: bool = False,
        tolerations: list[k8s.V1Toleration] | None = None,
        security_context: dict | None = None,
        dnspolicy: str | None = None,
        schedulername: str | None = None,
        init_containers: list[k8s.V1Container] | None = None,
        log_events_on_failure: bool = False,
        do_xcom_push: bool = False,
        pod_template_file: str | None = None,
        priority_class_name: str | None = None,
        pod_runtime_info_envs: list[k8s.V1EnvVar] | None = None,
        termination_grace_period: int | None = None,
        configmaps: list[str] | None = None,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert a callable to a Kubernetes Pod task.

        :param kubernetes_conn_id: The Kubernetes cluster's
            :ref:`connection ID <howto/connection:kubernetes>`.
        :param namespace: Namespace to run within Kubernetes. Defaults to *default*.
        :param image: Docker image to launch. Defaults to *hub.docker.com*, but
            a fully qualified URL will point to a custom repository. (templated)
        :param name: Name of the pod to run. This will be used (plus a random
            suffix if *random_name_suffix* is *True*) to generate a pod ID
            (DNS-1123 subdomain, containing only ``[a-z0-9.-]``). Defaults to
            ``k8s_airflow_pod_{RANDOM_UUID}``.
        :param random_name_suffix: If *True*, will generate a random suffix.
        :param ports: Ports for the launched pod.
        :param volume_mounts: *volumeMounts* for the launched pod.
        :param volumes: Volumes for the launched pod. Includes *ConfigMaps* and
            *PersistentVolumes*.
        :param env_vars: Environment variables initialized in the container.
            (templated)
        :param env_from: List of sources to populate environment variables in
            the container.
        :param secrets: Kubernetes secrets to inject in the container. They can
            be exposed as environment variables or files in a volume.
        :param in_cluster: Run kubernetes client with *in_cluster* configuration.
        :param cluster_context: Context that points to the Kubernetes cluster.
            Ignored when *in_cluster* is *True*. If *None*, current-context will
            be used.
        :param reattach_on_restart: If the worker dies while the pod is running,
            reattach and monitor during the next try. If *False*, always create
            a new pod for each try.
        :param labels: Labels to apply to the pod. (templated)
        :param startup_timeout_seconds: Timeout in seconds to startup the pod.
        :param get_logs: Get the stdout of the container as logs of the tasks.
        :param image_pull_policy: Specify a policy to cache or always pull an
            image.
        :param annotations: Non-identifying metadata you can attach to the pod.
            Can be a large range of data, and can include characters that are
            not permitted by labels.
        :param container_resources: Resources for the launched pod.
        :param affinity: Affinity scheduling rules for the launched pod.
        :param config_file: The path to the Kubernetes config file. If not
            specified, default value is ``~/.kube/config``. (templated)
        :param node_selector: A dict containing a group of scheduling rules.
        :param image_pull_secrets: Any image pull secrets to be given to the
            pod. If more than one secret is required, provide a comma separated
            list, e.g. ``secret_a,secret_b``.
        :param service_account_name: Name of the service account.
        :param is_delete_operator_pod: What to do when the pod reaches its final
            state, or the execution is interrupted. If *True* (default), delete
            the pod; otherwise leave the pod.
        :param hostnetwork: If *True*, enable host networking on the pod.
        :param tolerations: A list of Kubernetes tolerations.
        :param security_context: Security options the pod should run with
            (PodSecurityContext).
        :param dnspolicy: DNS policy for the pod.
        :param schedulername: Specify a scheduler name for the pod
        :param init_containers: Init containers for the launched pod.
        :param log_events_on_failure: Log the pod's events if a failure occurs.
        :param do_xcom_push: If *True*, the content of
            ``/airflow/xcom/return.json`` in the container will also be pushed
            to an XCom when the container completes.
        :param pod_template_file: Path to pod template file (templated)
        :param priority_class_name: Priority class name for the launched pod.
        :param pod_runtime_info_envs: A list of environment variables
            to be set in the container.
        :param termination_grace_period: Termination grace period if task killed
            in UI, defaults to kubernetes default
        :param configmaps: A list of names of config maps from which it collects
            ConfigMaps to populate the environment variables with. The contents
            of the target ConfigMap's Data field will represent the key-value
            pairs as environment variables. Extends env_from.
        """
    @overload
    def sensor(
        self,
        *,
        poke_interval: float = ...,
        timeout: float = ...,
        soft_fail: bool = False,
        mode: str = ...,
        exponential_backoff: bool = False,
        max_wait: timedelta | float | None = None,
        **kwargs,
    ) -> TaskDecorator:
        """
        Wraps a Python function into a sensor operator.

        :param poke_interval: Time in seconds that the job should wait in
            between each try
        :param timeout: Time, in seconds before the task times out and fails.
        :param soft_fail: Set to true to mark the task as SKIPPED on failure
        :param mode: How the sensor operates.
            Options are: ``{ poke | reschedule }``, default is ``poke``.
            When set to ``poke`` the sensor is taking up a worker slot for its
            whole execution time and sleeps between pokes. Use this mode if the
            expected runtime of the sensor is short or if a short poke interval
            is required. Note that the sensor will hold onto a worker slot and
            a pool slot for the duration of the sensor's runtime in this mode.
            When set to ``reschedule`` the sensor task frees the worker slot when
            the criteria is not yet met and it's rescheduled at a later time. Use
            this mode if the time before the criteria is met is expected to be
            quite long. The poke interval should be more than one minute to
            prevent too much load on the scheduler.
        :param exponential_backoff: allow progressive longer waits between
            pokes by using exponential backoff algorithm
        :param max_wait: maximum wait interval between pokes, can be ``timedelta`` or ``float`` seconds
        """
    @overload
    def sensor(self, python_callable: FParams | FReturn | None = None) -> Task[FParams, FReturn]: ...

task: TaskDecoratorCollection
