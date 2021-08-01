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
from typing import Dict, Iterable, List, Optional, Union

from docker.types import Mount

from airflow.exceptions import AirflowException
from airflow.providers.docker.hooks.docker import get_client
from airflow.providers.docker.hooks.docker_client import DockerClientHook
from airflow.sensors.base import BaseSensorOperator


class DockerSensor(BaseSensorOperator):
    """
    A sensor that runs a command in a docker container and detects if the container exited successfully

    See DockerOperator for limitations and considerations

    :param image: Docker image from which to create the container.
        If image tag is omitted, "latest" will be used.
    :type image: str
    :param api_version: Remote API version. Set to ``auto`` to automatically
        detect the server's version.
    :type api_version: str
    :param command: Command to be run in the container. (templated)
    :type command: str or list
    :param container_name: Name of the container. Optional (templated)
    :type container_name: str or None
    :param cpus: Number of CPUs to assign to the container.
        This value gets multiplied with 1024. See
        https://docs.docker.com/engine/reference/run/#cpu-share-constraint
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
    :param mount_tmp_dir: Specify whether the temporary directory should be bind-mounted
        from the host to the container. Defaults to True
    :type mount_tmp_dir: bool
    :param tmp_dir: Mount point inside the container to
        a temporary directory created on the host by the operator.
        The path is also made available via the environment variable
        ``AIRFLOW_TMP_DIR`` inside the container.
    :type tmp_dir: str
    :param user: Default user inside the docker container.
    :type user: int or str
    :param mounts: List of volumes to mount into the container. Each item should
        be a :py:class:`docker.types.Mount` instance.
    :type mounts: list[docker.types.Mount]
    :param entrypoint: Overwrite the default ENTRYPOINT of the image
    :type entrypoint: str or list
    :param working_dir: Working directory to
        set on the container (equivalent to the -w switch the docker client)
    :type working_dir: str
    :param docker_conn_id: The :ref:`Docker connection id <howto/connection:docker>`
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

    template_fields = ('command', 'environment', 'container_name')
    template_ext = (
        '.sh',
        '.bash',
    )

    def __init__(
        self,
        *,
        image: str,
        api_version: Optional[str] = None,
        command: Optional[Union[str, List[str]]] = None,
        container_name: Optional[str] = None,
        cpus: float = 1.0,
        docker_url: str = 'unix://var/run/docker.sock',
        environment: Optional[Dict] = None,
        private_environment: Optional[Dict] = None,
        force_pull: bool = False,
        mem_limit: Optional[Union[float, str]] = None,
        host_tmp_dir: Optional[str] = None,
        network_mode: Optional[str] = None,
        tls_ca_cert: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        tls_hostname: Optional[Union[str, bool]] = None,
        tls_ssl_version: Optional[str] = None,
        mount_tmp_dir: bool = True,
        tmp_dir: str = '/tmp/airflow',
        user: Optional[Union[str, int]] = None,
        mounts: Optional[List[Mount]] = None,
        entrypoint: Optional[Union[str, List[str]]] = None,
        working_dir: Optional[str] = None,
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
    ) -> None:

        super().__init__(**kwargs)
        self.api_version = api_version
        self.auto_remove = auto_remove
        self.command = command
        self.container_name = container_name
        self.cpus = cpus
        self.dns = dns
        self.dns_search = dns_search
        self.docker_url = docker_url
        self.environment = environment or {}
        self._private_environment = private_environment or {}
        self.force_pull = force_pull
        self.image = image
        self.mem_limit = mem_limit
        self.host_tmp_dir = host_tmp_dir
        self.network_mode = network_mode
        self.tls_ca_cert = tls_ca_cert
        self.tls_client_cert = tls_client_cert
        self.tls_client_key = tls_client_key
        self.tls_hostname = tls_hostname
        self.tls_ssl_version = tls_ssl_version
        self.mount_tmp_dir = mount_tmp_dir
        self.tmp_dir = tmp_dir
        self.user = user
        self.mounts = mounts or []
        self.entrypoint = entrypoint
        self.working_dir = working_dir
        self.docker_conn_id = docker_conn_id
        self.shm_size = shm_size
        self.tty = tty
        self.privileged = privileged
        self.cap_add = cap_add
        self.extra_hosts = extra_hosts

        self.cli = None
        self.cli_hook = None

    def poke(self, context) -> Optional[str]:
        self.cli = get_client(
            self.docker_conn_id,
            self.docker_url,
            self.api_version,
            self.tls_ca_cert,
            self.tls_client_cert,
            self.tls_client_key,
            self.tls_ssl_version,
            self.tls_hostname,
        )
        if not self.cli:
            raise Exception("The 'cli' should be initialized before!")

        self.cli_hook = DockerClientHook(
            self.cli,
            self.image,
            self.command,
            self.container_name,
            self.cpus,
            self.environment,
            self._private_environment,
            self.force_pull,
            self.mem_limit,
            self.host_tmp_dir,
            self.network_mode,
            self.mount_tmp_dir,
            self.tmp_dir,
            self.user,
            self.mounts,
            self.entrypoint,
            self.working_dir,
            self.dns,
            self.dns_search,
            self.shm_size,
            self.tty,
            self.privileged,
            self.cap_add,
            self.extra_hosts,
        )

        try:
            self.cli_hook.run_image()
        except AirflowException:
            return False
        return True

    def on_kill(self) -> None:
        if self.cli_hook is not None:
            self.cli_hook.stop()
