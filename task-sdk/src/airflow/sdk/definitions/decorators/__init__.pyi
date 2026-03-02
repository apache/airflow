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

from collections.abc import Callable, Collection, Container, Iterable, Mapping
from datetime import timedelta
from typing import Any, Literal, TypeVar, overload

from docker.types import Mount
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sdk.bases.decorator import FParams, FReturn, Task, TaskDecorator, _TaskDecorator
from airflow.sdk.definitions.dag import dag
from airflow.sdk.definitions.decorators.condition import AnyConditionFunc
from airflow.sdk.definitions.decorators.task_group import task_group

# Please keep this in sync with __init__.py's __all__.
__all__ = [
    "TaskDecorator",
    "TaskDecoratorCollection",
    "dag",
    "task",
    "task_group",
    "setup",
    "teardown",
]

_T = TypeVar("_T", bound=Task[..., Any] | _TaskDecorator[..., Any, Any])

class TaskDecoratorCollection:
    @overload
    def python(  # type: ignore[misc]
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
    def __call__(  # type: ignore[misc]
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
    def virtualenv(  # type: ignore[misc]
        self,
        *,
        multiple_outputs: bool | None = None,
        # 'python_callable', 'op_args' and 'op_kwargs' since they are filled by
        # _PythonVirtualenvDecoratedOperator.
        requirements: None | Iterable[str] | str = None,
        python_version: None | str | int | float = None,
        serializer: Literal["pickle", "cloudpickle", "dill"] | None = None,
        system_site_packages: bool = True,
        templates_dict: Mapping[str, Any] | None = None,
        pip_install_options: list[str] | None = None,
        skip_on_exit_code: int | Container[int] | None = None,
        index_urls: None | Collection[str] | str = None,
        venv_cache_path: None | str = None,
        show_return_value_in_logs: bool = True,
        env_vars: dict[str, str] | None = None,
        inherit_env: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert the decorated callable to a virtual environment task.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param requirements: Either a list of requirement strings, or a (templated)
            "requirements file" as specified by pip.
        :param python_version: The Python version to run the virtual environment with. Note that
            both 2 and 2.7 are acceptable forms.
        :param serializer: Which serializer use to serialize the args and result. It can be one of the following:

            - ``"pickle"``: (default) Use pickle for serialization. Included in the Python Standard Library.
            - ``"cloudpickle"``: Use cloudpickle for serialize more complex types,
              this requires to include cloudpickle in your requirements.
            - ``"dill"``: Use dill for serialize more complex types,
              this requires to include dill in your requirements.
        :param system_site_packages: Whether to include
            system_site_packages in your virtual environment.
            See virtualenv documentation for more information.
        :param pip_install_options: a list of pip install options when installing requirements
            See 'pip install -h' for available options
        :param skip_on_exit_code: If python_callable exits with this exit code, leave the task
            in ``skipped`` state (default: None). If set to ``None``, any non-zero
            exit code will be treated as a failure.
        :param index_urls: an optional list of index urls to load Python packages from.
            If not provided the system pip conf will be used to source packages from.
        :param venv_cache_path: Optional path to the virtual environment parent folder in which the
            virtual environment will be cached, creates a sub-folder venv-{hash} whereas hash will be
            replaced with a checksum of requirements. If not provided the virtual environment will be
            created and deleted in a temp folder for every execution.
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied.
        :param show_return_value_in_logs: a bool value whether to show return_value
            logs. Defaults to True, which allows return value log output.
            It can be set to False to prevent log output of return value when you return huge data
            such as transmission a large amount of XCom to TaskAPI.
        :param env_vars: A dictionary containing additional environment variables to set for the virtual
            environment when it is executed.
        :param inherit_env: Whether to inherit the current environment variables when executing the virtual
            environment. If set to ``True``, the virtual environment will inherit the environment variables
            of the parent process (``os.environ``). If set to ``False``, the virtual environment will be
            executed with a clean environment.
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
        serializer: Literal["pickle", "cloudpickle", "dill"] | None = None,
        templates_dict: Mapping[str, Any] | None = None,
        show_return_value_in_logs: bool = True,
        env_vars: dict[str, str] | None = None,
        inherit_env: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert the decorated callable to a virtual environment task.

        :param python: Full path string (file-system specific) that points to a Python binary inside
            a virtual environment that should be used (in ``VENV/bin`` folder). Should be absolute path
            (so usually start with "/" or "X:/" depending on the filesystem/os used).
        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param serializer: Which serializer use to serialize the args and result. It can be one of the following:

            - ``"pickle"``: (default) Use pickle for serialization. Included in the Python Standard Library.
            - ``"cloudpickle"``: Use cloudpickle for serialize more complex types,
              this requires to include cloudpickle in your requirements.
            - ``"dill"``: Use dill for serialize more complex types,
              this requires to include dill in your requirements.
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied.
        :param show_return_value_in_logs: a bool value whether to show return_value
            logs. Defaults to True, which allows return value log output.
            It can be set to False to prevent log output of return value when you return huge data
            such as transmission a large amount of XCom to TaskAPI.
        :param env_vars: A dictionary containing additional environment variables to set for the virtual
            environment when it is executed.
        :param inherit_env: Whether to inherit the current environment variables when executing the virtual
            environment. If set to ``True``, the virtual environment will inherit the environment variables
            of the parent process (``os.environ``). If set to ``False``, the virtual environment will be
            executed with a clean environment.
        """
    @overload
    def branch(  # type: ignore[misc]
        self, *, multiple_outputs: bool | None = None, **kwargs
    ) -> TaskDecorator:
        """Create a decorator to wrap the decorated callable into a BranchPythonOperator.

        For more information on how to use this decorator, see :ref:`concepts:branching`.
        Accepts arbitrary for operator kwarg. Can be reused in a single DAG.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        """
    @overload
    def branch(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]: ...
    @overload
    def branch_virtualenv(  # type: ignore[misc]
        self,
        *,
        multiple_outputs: bool | None = None,
        # 'python_callable', 'op_args' and 'op_kwargs' since they are filled by
        # _PythonVirtualenvDecoratedOperator.
        requirements: None | Iterable[str] | str = None,
        python_version: None | str | int | float = None,
        serializer: Literal["pickle", "cloudpickle", "dill"] | None = None,
        system_site_packages: bool = True,
        templates_dict: Mapping[str, Any] | None = None,
        pip_install_options: list[str] | None = None,
        skip_on_exit_code: int | Container[int] | None = None,
        index_urls: None | Collection[str] | str = None,
        venv_cache_path: None | str = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to wrap the decorated callable into a BranchPythonVirtualenvOperator.

        For more information on how to use this decorator, see :ref:`concepts:branching`.
        Accepts arbitrary for operator kwarg. Can be reused in a single DAG.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param requirements: Either a list of requirement strings, or a (templated)
            "requirements file" as specified by pip.
        :param python_version: The Python version to run the virtual environment with. Note that
            both 2 and 2.7 are acceptable forms.
        :param serializer: Which serializer use to serialize the args and result. It can be one of the following:

            - ``"pickle"``: (default) Use pickle for serialization. Included in the Python Standard Library.
            - ``"cloudpickle"``: Use cloudpickle for serialize more complex types,
              this requires to include cloudpickle in your requirements.
            - ``"dill"``: Use dill for serialize more complex types,
              this requires to include dill in your requirements.
        :param system_site_packages: Whether to include
            system_site_packages in your virtual environment.
            See virtualenv documentation for more information.
        :param pip_install_options: a list of pip install options when installing requirements
            See 'pip install -h' for available options
        :param skip_on_exit_code: If python_callable exits with this exit code, leave the task
            in ``skipped`` state (default: None). If set to ``None``, any non-zero
            exit code will be treated as a failure.
        :param index_urls: an optional list of index urls to load Python packages from.
            If not provided the system pip conf will be used to source packages from.
        :param venv_cache_path: Optional path to the virtual environment parent folder in which the
            virtual environment will be cached, creates a sub-folder venv-{hash} whereas hash will be replaced
            with a checksum of requirements. If not provided the virtual environment will be created and
            deleted in a temp folder for every execution.
        :param show_return_value_in_logs: a bool value whether to show return_value
            logs. Defaults to True, which allows return value log output.
            It can be set to False to prevent log output of return value when you return huge data
            such as transmission a large amount of XCom to TaskAPI.
        """
    @overload
    def branch_virtualenv(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]: ...
    @overload
    def branch_external_python(
        self,
        *,
        python: str,
        multiple_outputs: bool | None = None,
        # 'python_callable', 'op_args' and 'op_kwargs' since they are filled by
        # _PythonVirtualenvDecoratedOperator.
        serializer: Literal["pickle", "cloudpickle", "dill"] | None = None,
        templates_dict: Mapping[str, Any] | None = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to wrap the decorated callable into a BranchExternalPythonOperator.

        For more information on how to use this decorator, see :ref:`concepts:branching`.
        Accepts arbitrary for operator kwarg. Can be reused in a single DAG.

        :param python: Full path string (file-system specific) that points to a Python binary inside
            a virtual environment that should be used (in ``VENV/bin`` folder). Should be absolute path
            (so usually start with "/" or "X:/" depending on the filesystem/os used).
        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param serializer: Which serializer use to serialize the args and result. It can be one of the following:

            - ``"pickle"``: (default) Use pickle for serialization. Included in the Python Standard Library.
            - ``"cloudpickle"``: Use cloudpickle for serialize more complex types,
              this requires to include cloudpickle in your requirements.
            - ``"dill"``: Use dill for serialize more complex types,
              this requires to include dill in your requirements.
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
    def branch_external_python(
        self, python_callable: Callable[FParams, FReturn]
    ) -> Task[FParams, FReturn]: ...
    @overload
    def short_circuit(  # type: ignore[misc]
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
        python_command: str = "python3",
        serializer: Literal["pickle", "cloudpickle", "dill"] | None = None,
        use_dill: bool = False,  # Added by _DockerDecoratedOperator.
        # 'command', 'retrieve_output', and 'retrieve_output_path' are filled by
        # _DockerDecoratedOperator.
        image: str,
        api_version: str | None = None,
        container_name: str | None = None,
        cpus: float = 1.0,
        docker_url: str | None = None,
        environment: dict[str, str] | None = None,
        private_environment: dict[str, str] | None = None,
        env_file: str | None = None,
        force_pull: bool = False,
        mem_limit: float | str | None = None,
        host_tmp_dir: str | None = None,
        network_mode: str | None = None,
        tls_ca_cert: str | None = None,
        tls_client_cert: str | None = None,
        tls_client_key: str | None = None,
        tls_verify: bool = True,
        tls_hostname: str | bool | None = None,
        tls_ssl_version: str | None = None,
        mount_tmp_dir: bool = True,
        tmp_dir: str = "/tmp/airflow",
        user: str | int | None = None,
        mounts: list[Mount] | None = None,
        entrypoint: str | list[str] | None = None,
        working_dir: str | None = None,
        xcom_all: bool = False,
        docker_conn_id: str | None = None,
        dns: list[str] | None = None,
        dns_search: list[str] | None = None,
        auto_remove: Literal["never", "success", "force"] = "never",
        shm_size: int | None = None,
        tty: bool = False,
        hostname: str | None = None,
        privileged: bool = False,
        cap_add: str | None = None,
        extra_hosts: dict[str, str] | None = None,
        timeout: int = 60,
        device_requests: list[dict] | None = None,
        log_opts_max_size: str | None = None,
        log_opts_max_file: str | None = None,
        ipc_mode: str | None = None,
        skip_on_exit_code: int | Container[int] | None = None,
        port_bindings: dict | None = None,
        ulimits: list[dict] | None = None,
        labels: dict[str, str] | list[str] | None = None,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert the decorated callable to a Docker task.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param python_command: Python command for executing functions, Default: python3
        :param serializer: Which serializer use to serialize the args and result. It can be one of the following:

            - ``"pickle"``: (default) Use pickle for serialization. Included in the Python Standard Library.
            - ``"cloudpickle"``: Use cloudpickle for serialize more complex types,
              this requires to include cloudpickle in your requirements.
            - ``"dill"``: Use dill for serialize more complex types,
              this requires to include dill in your requirements.
        :param use_dill: Deprecated, use ``serializer`` instead. Whether to use dill to serialize
            the args and result (pickle is default). This allows more complex types
            but requires you to include dill in your requirements.
        :param image: Docker image from which to create the container.
            If image tag is omitted, "latest" will be used.
        :param api_version: Remote API version. Set to ``auto`` to automatically
            detect the server's version.
        :param container_name: Name of the container. Optional (templated)
        :param cpus: Number of CPUs to assign to the container.
            This value gets multiplied with 1024. See
            https://docs.docker.com/engine/reference/run/#cpu-share-constraint
        :param docker_url: URL of the host running the docker daemon.
            Default is the value of the ``DOCKER_HOST`` environment variable or unix://var/run/docker.sock
            if it is unset.
        :param environment: Environment variables to set in the container. (templated)
        :param private_environment: Private environment variables to set in the container.
            These are not templated, and hidden from the website.
        :param env_file: Relative path to the ``.env`` file with environment variables to set in the container.
            Overridden by variables in the environment parameter.
        :param force_pull: Pull the docker image on every run. Default is False.
        :param mem_limit: Maximum amount of memory the container can use.
            Either a float value, which represents the limit in bytes,
            or a string like ``128m`` or ``1g``.
        :param host_tmp_dir: Specify the location of the temporary directory on the host which will
            be mapped to tmp_dir. If not provided defaults to using the standard system temp directory.
        :param network_mode: Network mode for the container. It can be one of the following:

            - ``"bridge"``: Create new network stack for the container with default docker bridge network
            - ``"none"``: No networking for this container
            - ``"container:<name|id>"``: Use the network stack of another container specified via <name|id>
            - ``"host"``: Use the host network stack. Incompatible with `port_bindings`
            - ``"<network-name>|<network-id>"``: Connects the container to user created network
              (using ``docker network create`` command)
        :param tls_ca_cert: Path to a PEM-encoded certificate authority
            to secure the docker connection.
        :param tls_client_cert: Path to the PEM-encoded certificate
            used to authenticate docker client.
        :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
        :param tls_verify: Set ``True`` to verify the validity of the provided certificate.
        :param tls_hostname: Hostname to match against
            the docker server certificate or False to disable the check.
        :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
        :param mount_tmp_dir: Specify whether the temporary directory should be bind-mounted
            from the host to the container. Defaults to True
        :param tmp_dir: Mount point inside the container to
            a temporary directory created on the host by the operator.
            The path is also made available via the environment variable
            ``AIRFLOW_TMP_DIR`` inside the container.
        :param user: Default user inside the docker container.
        :param mounts: List of mounts to mount into the container, e.g.
            ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
        :param entrypoint: Overwrite the default ENTRYPOINT of the image
        :param working_dir: Working directory to
            set on the container (equivalent to the -w switch the docker client)
        :param xcom_all: Push all the stdout or just the last line.
            The default is False (last line).
        :param docker_conn_id: The :ref:`Docker connection id <howto/connection:docker>`
        :param dns: Docker custom DNS servers
        :param dns_search: Docker custom DNS search domain
        :param auto_remove: Enable removal of the container when the container's process exits. Possible values:

            - ``never``: (default) do not remove container
            - ``success``: remove on success
            - ``force``: always remove container
        :param shm_size: Size of ``/dev/shm`` in bytes. The size must be
            greater than 0. If omitted uses system default.
        :param tty: Allocate pseudo-TTY to the container
            This needs to be set see logs of the Docker container.
        :param hostname: Optional hostname for the container.
        :param privileged: Give extended privileges to this container.
        :param cap_add: Include container capabilities
        :param extra_hosts: Additional hostnames to resolve inside the container,
            as a mapping of hostname to IP address.
        :param device_requests: Expose host resources such as GPUs to the container.
        :param log_opts_max_size: The maximum size of the log before it is rolled.
            A positive integer plus a modifier representing the unit of measure (k, m, or g).
            Eg: 10m or 1g Defaults to -1 (unlimited).
        :param log_opts_max_file: The maximum number of log files that can be present.
            If rolling the logs creates excess files, the oldest file is removed.
            Only effective when max-size is also set. A positive integer. Defaults to 1.
        :param ipc_mode: Set the IPC mode for the container.
        :param skip_on_exit_code: If task exits with this exit code, leave the task
            in ``skipped`` state (default: None). If set to ``None``, any non-zero
            exit code will be treated as a failure.
        :param port_bindings: Publish a container's port(s) to the host. It is a
            dictionary of value where the key indicates the port to open inside the container
            and value indicates the host port that binds to the container port.
            Incompatible with ``"host"`` in ``network_mode``.
        :param ulimits: List of ulimit options to set for the container. Each item should
            be a :py:class:`docker.types.Ulimit` instance.
        :param labels: A dictionary of name-value labels (e.g. ``{"label1": "value1", "label2": "value2"}``)
            or a list of names of labels to set with empty values (e.g. ``["label1", "label2"]``)
        """
        # [END decorator_signature]
    @overload
    def kubernetes(  # type: ignore[misc]
        self,
        *,
        multiple_outputs: bool | None = None,
        use_dill: bool = False,  # Added by _KubernetesDecoratedOperator.
        # 'cmds' filled by _KubernetesDecoratedOperator.
        kubernetes_conn_id: str | None = ...,
        namespace: str | None = None,
        image: str | None = None,
        name: str | None = None,
        random_name_suffix: bool = ...,
        arguments: list[str] | None = None,
        ports: list[k8s.V1ContainerPort] | None = None,
        volume_mounts: list[k8s.V1VolumeMount] | None = None,
        volumes: list[k8s.V1Volume] | None = None,
        env_vars: list[k8s.V1EnvVar] | dict[str, str] | None = None,
        env_from: list[k8s.V1EnvFromSource] | None = None,
        secrets: list[Secret] | None = None,
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        labels: dict | None = None,
        reattach_on_restart: bool = ...,
        startup_timeout_seconds: int = ...,
        startup_check_interval_seconds: int = ...,
        get_logs: bool = True,
        container_logs: Iterable[str] | str | Literal[True] = ...,
        image_pull_policy: str | None = None,
        annotations: dict | None = None,
        container_resources: k8s.V1ResourceRequirements | None = None,
        affinity: k8s.V1Affinity | None = None,
        config_file: str | None = None,
        node_selector: dict | None = None,
        image_pull_secrets: list[k8s.V1LocalObjectReference] | None = None,
        service_account_name: str | None = None,
        hostnetwork: bool = False,
        host_aliases: list[k8s.V1HostAlias] | None = None,
        tolerations: list[k8s.V1Toleration] | None = None,
        security_context: k8s.V1PodSecurityContext | dict | None = None,
        container_security_context: k8s.V1SecurityContext | dict | None = None,
        dnspolicy: str | None = None,
        dns_config: k8s.V1PodDNSConfig | None = None,
        hostname: str | None = None,
        subdomain: str | None = None,
        schedulername: str | None = None,
        full_pod_spec: k8s.V1Pod | None = None,
        init_containers: list[k8s.V1Container] | None = None,
        log_events_on_failure: bool = False,
        do_xcom_push: bool = False,
        pod_template_file: str | None = None,
        pod_template_dict: dict | None = None,
        priority_class_name: str | None = None,
        pod_runtime_info_envs: list[k8s.V1EnvVar] | None = None,
        termination_grace_period: int | None = None,
        configmaps: list[str] | None = None,
        skip_on_exit_code: int | Container[int] | None = None,
        base_container_name: str | None = None,
        base_container_status_polling_interval: float = ...,
        deferrable: bool = ...,
        poll_interval: float = ...,
        log_pod_spec_on_failure: bool = ...,
        on_finish_action: str = ...,
        termination_message_policy: str = ...,
        active_deadline_seconds: int | None = None,
        progress_callback: Callable[[str], None] | None = None,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert a callable to a Kubernetes Pod task.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param use_dill: Whether to use dill or pickle for serialization
        :param kubernetes_conn_id: The Kubernetes cluster's
            :ref:`connection ID <howto/connection:kubernetes>`.
        :param namespace: Namespace to run within Kubernetes. Defaults to *default*.
        :param image: Docker image to launch. Defaults to *hub.docker.com*, but
            a fully qualified URL will point to a custom repository. (templated)
        :param name: Name of the pod to run. This will be used (plus a random
            suffix if *random_name_suffix* is *True*) to generate a pod ID
            (DNS-1123 subdomain, containing only ``[a-z0-9.-]``). Defaults to
            ``k8s-airflow-pod-{python_callable.__name__}``.
        :param random_name_suffix: If *True*, will generate a random suffix.
        :param arguments: arguments of the entrypoint. (templated)
            The docker image's CMD is used if this is not provided.
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
        :param startup_check_interval_seconds: interval in seconds to check if the pod has already started
        :param get_logs: Get the stdout of the container as logs of the tasks.
        :param container_logs: list of containers whose logs will be published to stdout
            Takes a sequence of containers, a single container name or True.
            If True, all the containers logs are published. Works in conjunction with ``get_logs`` param.
            The default value is the base container.
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
        :param hostnetwork: If *True*, enable host networking on the pod.
        :param host_aliases: A list of host aliases to apply to the containers in the pod.
        :param tolerations: A list of Kubernetes tolerations.
        :param security_context: Security options the pod should run with
            (PodSecurityContext).
        :param container_security_context: security options the container should run with.
        :param dnspolicy: DNS policy for the pod.
        :param dns_config: dns configuration (ip addresses, searches, options) for the pod.
        :param hostname: hostname for the pod.
        :param subdomain: subdomain for the pod.
        :param schedulername: Specify a scheduler name for the pod
        :param full_pod_spec: The complete podSpec
        :param init_containers: Init containers for the launched pod.
        :param log_events_on_failure: Log the pod's events if a failure occurs.
        :param do_xcom_push: If *True*, the content of
            ``/airflow/xcom/return.json`` in the container will also be pushed
            to an XCom when the container completes.
        :param pod_template_file: Path to pod template file (templated)
        :param pod_template_dict: pod template dictionary (templated)
        :param priority_class_name: Priority class name for the launched pod.
        :param pod_runtime_info_envs: A list of environment variables
            to be set in the container.
        :param termination_grace_period: Termination grace period if task killed
            in UI, defaults to kubernetes default
        :param configmaps: A list of names of config maps from which it collects
            ConfigMaps to populate the environment variables with. The contents
            of the target ConfigMap's Data field will represent the key-value
            pairs as environment variables. Extends env_from.
        :param skip_on_exit_code: If task exits with this exit code, leave the task
            in ``skipped`` state (default: None). If set to ``None``, any non-zero
            exit code will be treated as a failure.
        :param base_container_name: The name of the base container in the pod. This container's logs
            will appear as part of this task's logs if get_logs is True. Defaults to None. If None,
            will consult the class variable BASE_CONTAINER_NAME (which defaults to "base") for the base
            container name to use.
        :param base_container_status_polling_interval: Polling period in seconds to check for the pod base
            container status.
        :param deferrable: Run operator in the deferrable mode.
        :param poll_interval: Polling period in seconds to check for the status. Used only in deferrable mode.
        :param log_pod_spec_on_failure: Log the pod's specification if a failure occurs
        :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
            If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
            only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
        :param termination_message_policy: The termination message policy of the base container.
            Default value is "File"
        :param active_deadline_seconds: The active_deadline_seconds which matches to active_deadline_seconds
            in V1PodSpec.
        :param progress_callback: Callback function for receiving k8s container logs.
        """
    @overload
    def kubernetes(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]: ...
    @overload
    def kubernetes_cmd(  # type: ignore[misc]
        self,
        *,
        args_only: bool = False,  # Added by _KubernetesCmdDecoratedOperator.
        # 'cmds' filled by _KubernetesCmdDecoratedOperator.
        # 'arguments' filled by _KubernetesCmdDecoratedOperator.
        kubernetes_conn_id: str | None = ...,
        namespace: str | None = None,
        image: str | None = None,
        name: str | None = None,
        random_name_suffix: bool = ...,
        ports: list[k8s.V1ContainerPort] | None = None,
        volume_mounts: list[k8s.V1VolumeMount] | None = None,
        volumes: list[k8s.V1Volume] | None = None,
        env_vars: list[k8s.V1EnvVar] | dict[str, str] | None = None,
        env_from: list[k8s.V1EnvFromSource] | None = None,
        secrets: list[Secret] | None = None,
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        labels: dict | None = None,
        reattach_on_restart: bool = ...,
        startup_timeout_seconds: int = ...,
        startup_check_interval_seconds: int = ...,
        get_logs: bool = True,
        container_logs: Iterable[str] | str | Literal[True] = ...,
        image_pull_policy: str | None = None,
        annotations: dict | None = None,
        container_resources: k8s.V1ResourceRequirements | None = None,
        affinity: k8s.V1Affinity | None = None,
        config_file: str | None = None,
        node_selector: dict | None = None,
        image_pull_secrets: list[k8s.V1LocalObjectReference] | None = None,
        service_account_name: str | None = None,
        hostnetwork: bool = False,
        host_aliases: list[k8s.V1HostAlias] | None = None,
        tolerations: list[k8s.V1Toleration] | None = None,
        security_context: k8s.V1PodSecurityContext | dict | None = None,
        container_security_context: k8s.V1SecurityContext | dict | None = None,
        dnspolicy: str | None = None,
        dns_config: k8s.V1PodDNSConfig | None = None,
        hostname: str | None = None,
        subdomain: str | None = None,
        schedulername: str | None = None,
        full_pod_spec: k8s.V1Pod | None = None,
        init_containers: list[k8s.V1Container] | None = None,
        log_events_on_failure: bool = False,
        do_xcom_push: bool = False,
        pod_template_file: str | None = None,
        pod_template_dict: dict | None = None,
        priority_class_name: str | None = None,
        pod_runtime_info_envs: list[k8s.V1EnvVar] | None = None,
        termination_grace_period: int | None = None,
        configmaps: list[str] | None = None,
        skip_on_exit_code: int | Container[int] | None = None,
        base_container_name: str | None = None,
        base_container_status_polling_interval: float = ...,
        deferrable: bool = ...,
        poll_interval: float = ...,
        log_pod_spec_on_failure: bool = ...,
        on_finish_action: str = ...,
        termination_message_policy: str = ...,
        active_deadline_seconds: int | None = None,
        progress_callback: Callable[[str], None] | None = None,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to run a command returned by callable in a Kubernetes pod.

        :param args_only: If True, the decorated function should return a list arguments
            to be passed to the entrypoint of the container image. Defaults to False.
        :param kubernetes_conn_id: The Kubernetes cluster's
            :ref:`connection ID <howto/connection:kubernetes>`.
        :param namespace: Namespace to run within Kubernetes. Defaults to *default*.
        :param image: Docker image to launch. Defaults to *hub.docker.com*, but
            a fully qualified URL will point to a custom repository. (templated)
        :param name: Name of the pod to run. This will be used (plus a random
            suffix if *random_name_suffix* is *True*) to generate a pod ID
            (DNS-1123 subdomain, containing only ``[a-z0-9.-]``). Defaults to
            ``k8s-airflow-pod-{python_callable.__name__}``.
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
        :param startup_check_interval_seconds: interval in seconds to check if the pod has already started
        :param get_logs: Get the stdout of the container as logs of the tasks.
        :param container_logs: list of containers whose logs will be published to stdout
            Takes a sequence of containers, a single container name or True.
            If True, all the containers logs are published. Works in conjunction with ``get_logs`` param.
            The default value is the base container.
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
        :param hostnetwork: If *True*, enable host networking on the pod.
        :param host_aliases: A list of host aliases to apply to the containers in the pod.
        :param tolerations: A list of Kubernetes tolerations.
        :param security_context: Security options the pod should run with
            (PodSecurityContext).
        :param container_security_context: security options the container should run with.
        :param dnspolicy: DNS policy for the pod.
        :param dns_config: dns configuration (ip addresses, searches, options) for the pod.
        :param hostname: hostname for the pod.
        :param subdomain: subdomain for the pod.
        :param schedulername: Specify a scheduler name for the pod
        :param full_pod_spec: The complete podSpec
        :param init_containers: Init containers for the launched pod.
        :param log_events_on_failure: Log the pod's events if a failure occurs.
        :param do_xcom_push: If *True*, the content of
            ``/airflow/xcom/return.json`` in the container will also be pushed
            to an XCom when the container completes.
        :param pod_template_file: Path to pod template file (templated)
        :param pod_template_dict: pod template dictionary (templated)
        :param priority_class_name: Priority class name for the launched pod.
        :param pod_runtime_info_envs: A list of environment variables
            to be set in the container.
        :param termination_grace_period: Termination grace period if task killed
            in UI, defaults to kubernetes default
        :param configmaps: A list of names of config maps from which it collects
            ConfigMaps to populate the environment variables with. The contents
            of the target ConfigMap's Data field will represent the key-value
            pairs as environment variables. Extends env_from.
        :param skip_on_exit_code: If task exits with this exit code, leave the task
            in ``skipped`` state (default: None). If set to ``None``, any non-zero
            exit code will be treated as a failure.
        :param base_container_name: The name of the base container in the pod. This container's logs
            will appear as part of this task's logs if get_logs is True. Defaults to None. If None,
            will consult the class variable BASE_CONTAINER_NAME (which defaults to "base") for the base
            container name to use.
        :param base_container_status_polling_interval: Polling period in seconds to check for the pod base
            container status.
        :param deferrable: Run operator in the deferrable mode.
        :param poll_interval: Polling period in seconds to check for the status. Used only in deferrable mode.
        :param log_pod_spec_on_failure: Log the pod's specification if a failure occurs
        :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
            If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
            only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
        :param termination_message_policy: The termination message policy of the base container.
            Default value is "File"
        :param active_deadline_seconds: The active_deadline_seconds which matches to active_deadline_seconds
            in V1PodSpec.
        :param progress_callback: Callback function for receiving k8s container logs.
        """
    @overload
    def kubernetes_cmd(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]: ...
    @overload
    def sensor(  # type: ignore[misc]
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
    def sensor(self, python_callable: Callable[FParams, FReturn] | None = None) -> Task[FParams, FReturn]: ...
    @overload
    def pyspark(  # type: ignore[misc]
        self,
        *,
        multiple_outputs: bool | None = None,
        conn_id: str | None = None,
        config_kwargs: dict[str, str] | None = None,
        **kwargs,
    ) -> TaskDecorator:
        """
        Wraps a Python function that is to be injected with a SparkSession.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        :param conn_id: The connection ID to use for the SparkSession.
        :param config_kwargs: Additional kwargs to pass to the SparkSession builder. This overrides
            the config from the connection.
        """
    @overload
    def pyspark(
        self, python_callable: Callable[FParams, FReturn] | None = None
    ) -> Task[FParams, FReturn]: ...
    @overload
    def bash(  # type: ignore[misc]
        self,
        *,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_on_exit_code: int = 99,
        cwd: str | None = None,
        **kwargs,
    ) -> TaskDecorator:
        """Decorator to wrap a callable into a BashOperator task.

        :param bash_command: The command, set of commands or reference to a bash script
            (must be '.sh' or '.bash') to be executed. (templated)
        :param env: If env is not None, it must be a dict that defines the environment variables for the new
            process; these are used instead of inheriting the current process environment, which is the
            default behavior. (templated)
        :param append_env: If False(default) uses the environment variables passed in env params and does not
            inherit the current process environment. If True, inherits the environment variables from current
            passes and then environment variable passed by the user will either update the existing inherited
            environment variables or the new variables gets appended to it
        :param output_encoding: Output encoding of bash command
        :param skip_on_exit_code: If task exits with this exit code, leave the task in ``skipped`` state
            (default: 99). If set to ``None``, any non-zero exit code will be treated as a failure.
        :param cwd: Working directory to execute the command in. If None (default), the command is run in a
            temporary directory.
        """
    @overload
    def bash(self, python_callable: Callable[FParams, FReturn]) -> Task[FParams, FReturn]: ...
    def run_if(self, condition: AnyConditionFunc, skip_message: str | None = None) -> Callable[[_T], _T]:
        """
        Decorate a task to run only if a condition is met.

        :param condition: A function that takes a context and returns a boolean.
        :param skip_message: The message to log if the task is skipped.
            If None, a default message is used.
        """
    def skip_if(self, condition: AnyConditionFunc, skip_message: str | None = None) -> Callable[[_T], _T]:
        """
        Decorate a task to skip if a condition is met.

        :param condition: A function that takes a context and returns a boolean.
        :param skip_message: The message to log if the task is skipped.
            If None, a default message is used.
        """
    def __getattr__(self, name: str) -> TaskDecorator: ...

task: TaskDecoratorCollection
setup: Callable
teardown: Callable
