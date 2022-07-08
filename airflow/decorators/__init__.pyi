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

from typing import Any, Dict, Iterable, List, Mapping, Optional, Union, overload

from airflow.decorators.base import Function, Task, TaskDecorator
from airflow.decorators.branch_python import branch_task
from airflow.decorators.python import python_task
from airflow.decorators.python_virtualenv import virtualenv_task
from airflow.decorators.task_group import task_group
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
    "branch_task",
]

class TaskDecoratorCollection:
    @overload
    def python(
        self,
        *,
        multiple_outputs: Optional[bool] = None,
        # 'python_callable', 'op_args' and 'op_kwargs' since they are filled by
        # _PythonDecoratedOperator.
        templates_dict: Optional[Mapping[str, Any]] = None,
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
    def python(self, python_callable: Function) -> Task[Function]: ...
    # [END mixin_for_typing]
    @overload
    def __call__(
        self,
        *,
        multiple_outputs: Optional[bool] = None,
        templates_dict: Optional[Mapping[str, Any]] = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Aliasing ``python``; signature should match exactly."""
    @overload
    def __call__(self, python_callable: Function) -> Task[Function]:
        """Aliasing ``python``; signature should match exactly."""
    @overload
    def virtualenv(
        self,
        *,
        multiple_outputs: Optional[bool] = None,
        # 'python_callable', 'op_args' and 'op_kwargs' since they are filled by
        # _PythonVirtualenvDecoratedOperator.
        requirements: Union[None, Iterable[str], str] = None,
        python_version: Union[None, str, int, float] = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        templates_dict: Optional[Mapping[str, Any]] = None,
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
    def virtualenv(self, python_callable: Function) -> Task[Function]: ...
    @overload
    def branch(self, *, multiple_outputs: Optional[bool] = None, **kwargs) -> TaskDecorator:
        """Create a decorator to wrap the decorated callable into a BranchPythonOperator.

        For more information on how to use this decorator, see :ref:`howto/operator:BranchPythonOperator`.
        Accepts arbitrary for operator kwarg. Can be reused in a single DAG.

        :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
            Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
        """
    @overload
    def branch(self, python_callable: Function) -> Task[Function]: ...
    # [START decorator_signature]
    def docker(
        self,
        *,
        multiple_outputs: Optional[bool] = None,
        use_dill: bool = False,  # Added by _DockerDecoratedOperator.
        python_command: str = "python3",
        # 'command', 'retrieve_output', and 'retrieve_output_path' are filled by
        # _DockerDecoratedOperator.
        image: str,
        api_version: Optional[str] = None,
        container_name: Optional[str] = None,
        cpus: float = 1.0,
        docker_url: str = "unix://var/run/docker.sock",
        environment: Optional[Dict[str, str]] = None,
        private_environment: Optional[Dict[str, str]] = None,
        force_pull: bool = False,
        mem_limit: Optional[Union[float, str]] = None,
        host_tmp_dir: Optional[str] = None,
        network_mode: Optional[str] = None,
        tls_ca_cert: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        tls_hostname: Optional[Union[str, bool]] = None,
        tls_ssl_version: Optional[str] = None,
        tmp_dir: str = "/tmp/airflow",
        user: Optional[Union[str, int]] = None,
        mounts: Optional[List[str]] = None,
        working_dir: Optional[str] = None,
        xcom_all: bool = False,
        docker_conn_id: Optional[str] = None,
        dns: Optional[List[str]] = None,
        dns_search: Optional[List[str]] = None,
        auto_remove: bool = False,
        shm_size: Optional[int] = None,
        tty: bool = False,
        privileged: bool = False,
        cap_add: Optional[Iterable[str]] = None,
        extra_hosts: Optional[Dict[str, str]] = None,
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
            '<network-name>|<network-id>' - Connects the container to user created network(using `docker network create` command)
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

task: TaskDecoratorCollection
