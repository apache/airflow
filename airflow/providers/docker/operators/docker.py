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
"""Implements Docker operator"""
from __future__ import annotations

import ast
import pickle
import tarfile
import warnings
from io import BytesIO, StringIO
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Iterable, Sequence

from docker.constants import DEFAULT_TIMEOUT_SECONDS
from docker.errors import APIError
from docker.types import LogConfig, Mount
from dotenv import dotenv_values

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.docker.hooks.docker import DockerHook

if TYPE_CHECKING:
    from docker import APIClient
    from docker.types import DeviceRequest

    from airflow.utils.context import Context


def stringify(line: str | bytes):
    """Make sure string is returned even if bytes are passed. Docker stream can return bytes."""
    decode_method = getattr(line, "decode", None)
    if decode_method:
        return decode_method(encoding="utf-8", errors="surrogateescape")
    else:
        return line


class DockerOperator(BaseOperator):
    """
    Execute a command inside a docker container.

    By default, a temporary directory is
    created on the host and mounted into a container to allow storing files
    that together exceed the default disk size of 10GB in a container.
    In this case The path to the mounted directory can be accessed
    via the environment variable ``AIRFLOW_TMP_DIR``.

    If the volume cannot be mounted, warning is printed and an attempt is made to execute the docker
    command without the temporary folder mounted. This is to make it works by default with remote docker
    engine or when you run docker-in-docker solution and temporary directory is not shared with the
    docker engine. Warning is printed in logs in this case.

    If you know you run DockerOperator with remote engine or via docker-in-docker
    you should set ``mount_tmp_dir`` parameter to False. In this case, you can still use
    ``mounts`` parameter to mount already existing named volumes in your Docker Engine
    to achieve similar capability where you can store files exceeding default disk size
    of the container,

    If a login to a private registry is required prior to pulling the image, a
    Docker connection needs to be configured in Airflow and the connection ID
    be provided with the parameter ``docker_conn_id``.

    :param image: Docker image from which to create the container.
        If image tag is omitted, "latest" will be used. (templated)
    :param api_version: Remote API version. Set to ``auto`` to automatically
        detect the server's version.
    :param command: Command to be run in the container. (templated)
    :param container_name: Name of the container. Optional (templated)
    :param cpus: Number of CPUs to assign to the container.
        This value gets multiplied with 1024. See
        https://docs.docker.com/engine/reference/run/#cpu-share-constraint
    :param docker_url: URL of the host running the docker daemon.
        Default is unix://var/run/docker.sock
    :param environment: Environment variables to set in the container. (templated)
    :param private_environment: Private environment variables to set in the container.
        These are not templated, and hidden from the website.
    :param env_file: Relative path to the .env file with environment variables to set in the container.
        Overridden by variables in the environment parameter. (templated)
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
        '<network-name>|<network-id>' - Connects the container to user created network
        (using `docker network create` command)
    :param tls_ca_cert: Path to a PEM-encoded certificate authority
        to secure the docker connection.
    :param tls_client_cert: Path to the PEM-encoded certificate
        used to authenticate docker client.
    :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
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
    :param mounts: List of volumes to mount into the container. Each item should
        be a :py:class:`docker.types.Mount` instance.
    :param entrypoint: Overwrite the default ENTRYPOINT of the image
    :param working_dir: Working directory to
        set on the container (equivalent to the -w switch the docker client)
    :param xcom_all: Push all the stdout or just the last line.
        The default is False (last line).
    :param docker_conn_id: The :ref:`Docker connection id <howto/connection:docker>`
    :param dns: Docker custom DNS servers
    :param dns_search: Docker custom DNS search domain
    :param auto_remove: Auto-removal of the container on daemon side when the
        container's process exits.
        The default is never.
    :param shm_size: Size of ``/dev/shm`` in bytes. The size must be
        greater than 0. If omitted uses system default.
    :param tty: Allocate pseudo-TTY to the container
        This needs to be set see logs of the Docker container.
    :param hostname: Optional hostname for the container.
    :param privileged: Give extended privileges to this container.
    :param cap_add: Include container capabilities
    :param retrieve_output: Should this docker image consistently attempt to pull from and output
        file before manually shutting down the image. Useful for cases where users want a pickle serialized
        output that is not posted to logs
    :param retrieve_output_path: path for output file that will be retrieved and passed to xcom
    :param device_requests: Expose host resources such as GPUs to the container.
    :param log_opts_max_size: The maximum size of the log before it is rolled.
        A positive integer plus a modifier representing the unit of measure (k, m, or g).
        Eg: 10m or 1g Defaults to -1 (unlimited).
    :param log_opts_max_file: The maximum number of log files that can be present.
        If rolling the logs creates excess files, the oldest file is removed.
        Only effective when max-size is also set. A positive integer. Defaults to 1.
    :param ipc_mode: Set the IPC mode for the container.
    """

    template_fields: Sequence[str] = ("image", "command", "environment", "env_file", "container_name")
    template_fields_renderers = {"env_file": "yaml"}
    template_ext: Sequence[str] = (
        ".sh",
        ".bash",
        ".env",
    )

    def __init__(
        self,
        *,
        image: str,
        api_version: str | None = None,
        command: str | list[str] | None = None,
        container_name: str | None = None,
        cpus: float = 1.0,
        docker_url: str = "unix://var/run/docker.sock",
        environment: dict | None = None,
        private_environment: dict | None = None,
        env_file: str | None = None,
        force_pull: bool = False,
        mem_limit: float | str | None = None,
        host_tmp_dir: str | None = None,
        network_mode: str | None = None,
        tls_ca_cert: str | None = None,
        tls_client_cert: str | None = None,
        tls_client_key: str | None = None,
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
        auto_remove: str = "never",
        shm_size: int | None = None,
        tty: bool = False,
        hostname: str | None = None,
        privileged: bool = False,
        cap_add: Iterable[str] | None = None,
        extra_hosts: dict[str, str] | None = None,
        retrieve_output: bool = False,
        retrieve_output_path: str | None = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        device_requests: list[DeviceRequest] | None = None,
        log_opts_max_size: str | None = None,
        log_opts_max_file: str | None = None,
        ipc_mode: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.api_version = api_version
        if type(auto_remove) == bool:
            warnings.warn(
                "bool value for auto_remove is deprecated, please use 'never', 'success', or 'force' instead",
                DeprecationWarning,
                stacklevel=2,
            )
        if str(auto_remove) == "False":
            self.auto_remove = "never"
        elif str(auto_remove) == "True":
            self.auto_remove = "success"
        elif str(auto_remove) in ("never", "success", "force"):
            self.auto_remove = auto_remove
        else:
            raise ValueError("unsupported auto_remove option, use 'never', 'success', or 'force' instead")
        self.command = command
        self.container_name = container_name
        self.cpus = cpus
        self.dns = dns
        self.dns_search = dns_search
        self.docker_url = docker_url
        self.environment = environment or {}
        self._private_environment = private_environment or {}
        self.env_file = env_file
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
        self.xcom_all = xcom_all
        self.docker_conn_id = docker_conn_id
        self.shm_size = shm_size
        self.tty = tty
        self.hostname = hostname
        self.privileged = privileged
        self.cap_add = cap_add
        self.extra_hosts = extra_hosts

        self.container: dict = None  # type: ignore[assignment]
        self.retrieve_output = retrieve_output
        self.retrieve_output_path = retrieve_output_path
        self.timeout = timeout
        self.device_requests = device_requests
        self.log_opts_max_size = log_opts_max_size
        self.log_opts_max_file = log_opts_max_file
        self.ipc_mode = ipc_mode

    @cached_property
    def hook(self) -> DockerHook:
        """Create and return an DockerHook (cached)."""
        tls_config = DockerHook.construct_tls_config(
            ca_cert=self.tls_ca_cert,
            client_cert=self.tls_client_cert,
            client_key=self.tls_client_key,
            assert_hostname=self.tls_hostname,
            ssl_version=self.tls_ssl_version,
        )
        return DockerHook(
            docker_conn_id=self.docker_conn_id,
            base_url=self.docker_url,
            version=self.api_version,
            tls=tls_config,
            timeout=self.timeout,
        )

    def get_hook(self) -> DockerHook:
        """Create and return an DockerHook (cached)."""
        return self.hook

    @property
    def cli(self) -> APIClient:
        return self.hook.api_client

    def _run_image(self) -> list[str] | str | None:
        """Run a Docker container with the provided image"""
        self.log.info("Starting docker container from image %s", self.image)
        if self.mount_tmp_dir:
            with TemporaryDirectory(prefix="airflowtmp", dir=self.host_tmp_dir) as host_tmp_dir_generated:
                tmp_mount = Mount(self.tmp_dir, host_tmp_dir_generated, "bind")
                try:
                    return self._run_image_with_mounts(self.mounts + [tmp_mount], add_tmp_variable=True)
                except APIError as e:
                    if host_tmp_dir_generated in str(e):
                        self.log.warning(
                            "Using remote engine or docker-in-docker and mounting temporary "
                            "volume from host is not supported. Falling back to "
                            "`mount_tmp_dir=False` mode. You can set `mount_tmp_dir` parameter"
                            " to False to disable mounting and remove the warning"
                        )
                        return self._run_image_with_mounts(self.mounts, add_tmp_variable=False)
                    raise
        else:
            return self._run_image_with_mounts(self.mounts, add_tmp_variable=False)

    def _run_image_with_mounts(self, target_mounts, add_tmp_variable: bool) -> list[str] | str | None:
        if add_tmp_variable:
            self.environment["AIRFLOW_TMP_DIR"] = self.tmp_dir
        else:
            self.environment.pop("AIRFLOW_TMP_DIR", None)
        docker_log_config = {}
        if self.log_opts_max_size is not None:
            docker_log_config["max-size"] = self.log_opts_max_size
        if self.log_opts_max_file is not None:
            docker_log_config["max-file"] = self.log_opts_max_file
        env_file_vars = {}
        if self.env_file is not None:
            env_file_vars = self.unpack_environment_variables(self.env_file)
        self.container = self.cli.create_container(
            command=self.format_command(self.command),
            name=self.container_name,
            environment={**env_file_vars, **self.environment, **self._private_environment},
            host_config=self.cli.create_host_config(
                auto_remove=False,
                mounts=target_mounts,
                network_mode=self.network_mode,
                shm_size=self.shm_size,
                dns=self.dns,
                dns_search=self.dns_search,
                cpu_shares=int(round(self.cpus * 1024)),
                mem_limit=self.mem_limit,
                cap_add=self.cap_add,
                extra_hosts=self.extra_hosts,
                privileged=self.privileged,
                device_requests=self.device_requests,
                log_config=LogConfig(config=docker_log_config),
                ipc_mode=self.ipc_mode,
            ),
            image=self.image,
            user=self.user,
            entrypoint=self.format_command(self.entrypoint),
            working_dir=self.working_dir,
            tty=self.tty,
            hostname=self.hostname,
        )
        logstream = self.cli.attach(container=self.container["Id"], stdout=True, stderr=True, stream=True)
        try:
            self.cli.start(self.container["Id"])

            log_lines = []
            for log_chunk in logstream:
                log_chunk = stringify(log_chunk).strip()
                log_lines.append(log_chunk)
                self.log.info("%s", log_chunk)

            result = self.cli.wait(self.container["Id"])
            if result["StatusCode"] != 0:
                joined_log_lines = "\n".join(log_lines)
                raise AirflowException(f"Docker container failed: {repr(result)} lines {joined_log_lines}")

            if self.retrieve_output:
                return self._attempt_to_retrieve_result()
            elif self.do_xcom_push:
                if len(log_lines) == 0:
                    return None
                try:
                    if self.xcom_all:
                        return log_lines
                    else:
                        return log_lines[-1]
                except StopIteration:
                    # handle the case when there is not a single line to iterate on
                    return None
            return None
        finally:
            if self.auto_remove == "success":
                self.cli.remove_container(self.container["Id"])
            elif self.auto_remove == "force":
                self.cli.remove_container(self.container["Id"], force=True)

    def _attempt_to_retrieve_result(self):
        """
        Attempts to pull the result of the function from the expected file using docker's
        get_archive function.
        If the file is not yet ready, returns None
        :return:
        """

        def copy_from_docker(container_id, src):
            archived_result, stat = self.cli.get_archive(container_id, src)
            if stat["size"] == 0:
                # 0 byte file, it can't be anything else than None
                return None
            # no need to port to a file since we intend to deserialize
            file_standin = BytesIO(b"".join(archived_result))
            tar = tarfile.open(fileobj=file_standin)
            file = tar.extractfile(stat["name"])
            lib = getattr(self, "pickling_library", pickle)
            return lib.loads(file.read())

        try:
            return copy_from_docker(self.container["Id"], self.retrieve_output_path)
        except APIError:
            return None

    def execute(self, context: Context) -> list[str] | str | None:
        # Pull the docker image if `force_pull` is set or image does not exist locally
        if self.force_pull or not self.cli.images(name=self.image):
            self.log.info("Pulling docker image %s", self.image)
            latest_status: dict[str, str] = {}
            for output in self.cli.pull(self.image, stream=True, decode=True):
                if isinstance(output, str):
                    self.log.info("%s", output)
                    continue
                if isinstance(output, dict) and "status" in output:
                    output_status = output["status"]
                    if "id" not in output:
                        self.log.info("%s", output_status)
                        continue

                    output_id = output["id"]
                    if latest_status.get(output_id) != output_status:
                        self.log.info("%s: %s", output_id, output_status)
                        latest_status[output_id] = output_status
        return self._run_image()

    @staticmethod
    def format_command(command: list[str] | str | None) -> list[str] | str | None:
        """
        Retrieve command(s). if command string starts with [, it returns the command list)

        :param command: Docker command or entrypoint

        :return: the command (or commands)
        """
        if isinstance(command, str) and command.strip().find("[") == 0:
            command = ast.literal_eval(command)
        return command

    def on_kill(self) -> None:
        if self.hook.client_created:
            self.log.info("Stopping docker container")
            if self.container is None:
                self.log.info("Not attempting to kill container as it was not created")
                return
            self.cli.stop(self.container["Id"])

    @staticmethod
    def unpack_environment_variables(env_str: str) -> dict:
        r"""
        Parse environment variables from the string

        :param env_str: environment variables in key=value format separated by '\n'

        :return: dictionary containing parsed environment variables
        """
        return dotenv_values(stream=StringIO(env_str))
