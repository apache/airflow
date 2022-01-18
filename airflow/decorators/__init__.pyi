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

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, TypeVar, Union, overload

__all__ = ["task"]

F = TypeVar("F", bound=Callable)

TaskDecorator = Callable[[F], F]

class TaskDecoratorFactory:
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

        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied
        :type templates_dict: dict of str
        :param show_return_value_in_logs: a bool value whether to show return_value
            logs. Defaults to True, which allows return value log output.
            It can be set to False to prevent log output of return value when you return huge data
            such as transmission a large amount of XCom to TaskAPI.
        :type show_return_value_in_logs: bool
        """
    # [START mixin_for_typing]
    @overload
    def python(self, python_callable: F) -> F: ...
    # [END mixin_for_typing]
    @overload
    def __call__(
        self,
        *,
        multiple_outputs: Optional[bool] = None,
        # Should match 'python()' signature.
        templates_dict: Optional[Mapping[str, Any]] = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> TaskDecorator:
        """Create a decorator to convert the decorated callable to a task.

        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied
        :type templates_dict: dict of str
        :param show_return_value_in_logs: a bool value whether to show return_value
            logs. Defaults to True, which allows return value log output.
            It can be set to False to prevent log output of return value when you return huge data
            such as transmission a large amount of XCom to TaskAPI.
        :type show_return_value_in_logs: bool
        """
    @overload
    def __call__(self, python_callable: F) -> F: ...
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

        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        :param requirements: Either a list of requirement strings, or a (templated)
            "requirements file" as specified by pip.
        :type requirements: list[str] | str
        :param python_version: The Python version to run the virtualenv with. Note that
            both 2 and 2.7 are acceptable forms.
        :type python_version: Optional[Union[str, int, float]]
        :param use_dill: Whether to use dill to serialize
            the args and result (pickle is default). This allow more complex types
            but requires you to include dill in your requirements.
        :type use_dill: bool
        :param system_site_packages: Whether to include
            system_site_packages in your virtualenv.
            See virtualenv documentation for more information.
        :type system_site_packages: bool
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied
        :type templates_dict: dict of str
        :param show_return_value_in_logs: a bool value whether to show return_value
            logs. Defaults to True, which allows return value log output.
            It can be set to False to prevent log output of return value when you return huge data
            such as transmission a large amount of XCom to TaskAPI.
        :type show_return_value_in_logs: bool
        """
    @overload
    def virtualenv(self, python_callable: F) -> F: ...
    # [START decorator_signature]
    @overload
    def docker(
        self,
        *,
        multiple_outputs: Optional[bool] = None,
        use_dill: bool = False,  # Added by _DockerDecoratedOperator.
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

        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        :param use_dill: Whether to use dill or pickle for serialization
        :type use_dill: bool
        :param image: Docker image from which to create the container.
            If image tag is omitted, "latest" will be used.
        :type image: str
        :param api_version: Remote API version. Set to ``auto`` to automatically
            detect the server's version.
        :type api_version: str
        :param container_name: Name of the container. Optional (templated)
        :type container_name: str or None
        :param cpus: Number of CPUs to assign to the container. This value gets multiplied with 1024.
        :type cpus: float
        :param docker_url: URL of the host running the docker daemon.
            Default is unix://var/run/docker.sock
        :type docker_url: str
        :param environment: Environment variables to set in the container. (templated)
        :type environment: dict
        :param private_environment: Private environment variables to set in the container.
            These are not templated, and hidden from the website.
        :type private_environment: dict
        :param force_pull: Pull the docker image on every run. Default is False.
        :type force_pull: bool
        :param mem_limit: Maximum amount of memory the container can use.
            Either a float value, which represents the limit in bytes,
            or a string like ``128m`` or ``1g``.
        :type mem_limit: float or str
        :param host_tmp_dir: Specify the location of the temporary directory on the host which will
            be mapped to tmp_dir. If not provided defaults to using the standard system temp directory.
        :type host_tmp_dir: str
        :param network_mode: Network mode for the container.
        :type network_mode: str
        :param tls_ca_cert: Path to a PEM-encoded certificate authority
            to secure the docker connection.
        :type tls_ca_cert: str
        :param tls_client_cert: Path to the PEM-encoded certificate
            used to authenticate docker client.
        :type tls_client_cert: str
        :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
        :type tls_client_key: str
        :param tls_hostname: Hostname to match against
            the docker server certificate or False to disable the check.
        :type tls_hostname: str or bool
        :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
        :type tls_ssl_version: str
        :param tmp_dir: Mount point inside the container to
            a temporary directory created on the host by the operator.
            The path is also made available via the environment variable
            ``AIRFLOW_TMP_DIR`` inside the container.
        :type tmp_dir: str
        :param user: Default user inside the docker container.
        :type user: int or str
        :param mounts: List of mounts to mount into the container, e.g.
            ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
        :type mounts: list
        :param working_dir: Working directory to
            set on the container (equivalent to the -w switch the docker client)
        :type working_dir: str
        :param xcom_all: Push all the stdout or just the last line.
            The default is False (last line).
        :type xcom_all: bool
        :param docker_conn_id: ID of the Airflow connection to use
        :type docker_conn_id: str
        :param dns: Docker custom DNS servers
        :type dns: list[str]
        :param dns_search: Docker custom DNS search domain
        :type dns_search: list[str]
        :param auto_remove: Auto-removal of the container on daemon side when the
            container's process exits.
            The default is False.
        :type auto_remove: bool
        :param shm_size: Size of ``/dev/shm`` in bytes. The size must be
            greater than 0. If omitted uses system default.
        :type shm_size: int
        :param tty: Allocate pseudo-TTY to the container
            This needs to be set see logs of the Docker container.
        :type tty: bool
        :param privileged: Give extended privileges to this container.
        :type privileged: bool
        :param cap_add: Include container capabilities
        :type cap_add: list[str]
        """
        # [END decorator_signature]

task = TaskDecoratorFactory()
