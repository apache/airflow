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

import ast
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, Optional, Union

from docker import APIClient
from docker.errors import APIError
from docker.types import Mount

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class DockerClientHook(BaseHook, LoggingMixin):
    """
    Interact with a Docker engine

    :param image: Docker image from which to create the container.
        If image tag is omitted, "latest" will be used.
    :type image: str
    :param command: Command to be run in the container. (templated)
    :type command: str or list
    :param container_name: Name of the container. Optional (templated)
    :type container_name: str or None
    :param cpus: Number of CPUs to assign to the container.
        This value gets multiplied with 1024. See
        https://docs.docker.com/engine/reference/run/#cpu-share-constraint
    :type cpus: float
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

    def __init__(
        self,
        cli: APIClient,
        image: str,
        command: Optional[Union[str, List[str]]] = None,
        container_name: Optional[str] = None,
        cpus: float = 1.0,
        environment: Optional[Dict] = None,
        private_environment: Optional[Dict] = None,
        force_pull: bool = False,
        mem_limit: Optional[Union[float, str]] = None,
        host_tmp_dir: Optional[str] = None,
        network_mode: Optional[str] = None,
        mount_tmp_dir: bool = True,
        tmp_dir: str = '/tmp/airflow',
        user: Optional[Union[str, int]] = None,
        mounts: Optional[List[Mount]] = None,
        entrypoint: Optional[Union[str, List[str]]] = None,
        working_dir: Optional[str] = None,
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
        self.cli = cli
        self.command = command
        self.container_name = container_name
        self.cpus = cpus
        self.dns = dns
        self.dns_search = dns_search
        self.auto_remove = auto_remove
        self.environment = environment or {}
        self._private_environment = private_environment or {}
        self.force_pull = force_pull
        self.image = image
        self.mem_limit = mem_limit
        self.host_tmp_dir = host_tmp_dir
        self.network_mode = network_mode
        self.mount_tmp_dir = mount_tmp_dir
        self.tmp_dir = tmp_dir
        self.user = user
        self.mounts = mounts or []
        self.entrypoint = entrypoint
        self.working_dir = working_dir
        self.shm_size = shm_size
        self.tty = tty
        self.privileged = privileged
        self.cap_add = cap_add
        self.extra_hosts = extra_hosts
        self.container = None

    def run_image(self) -> Optional[str]:
        """Run the docker image and command associated with this hook"""
        self.log.info('Starting docker container from image %s', self.image)
        if not self.cli:
            raise Exception("The 'cli' should be initialized before!")
        if self.force_pull or not self.cli.images(name=self.image):
            self._pull_image()
        if self.mount_tmp_dir:
            with TemporaryDirectory(prefix='airflowtmp', dir=self.host_tmp_dir) as host_tmp_dir_generated:
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

    def _run_image_with_mounts(self, target_mounts, add_tmp_variable: bool) -> Optional[str]:
        if add_tmp_variable:
            self.environment['AIRFLOW_TMP_DIR'] = self.tmp_dir
        else:
            self.environment.pop('AIRFLOW_TMP_DIR', None)
        self.container = self.cli.create_container(
            command=self.format_command(self.command),
            name=self.container_name,
            environment={**self.environment, **self._private_environment},
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
            ),
            image=self.image,
            user=self.user,
            entrypoint=self.format_command(self.entrypoint),
            working_dir=self.working_dir,
            tty=self.tty,
        )
        lines = self.cli.attach(container=self.container['Id'], stdout=True, stderr=True, stream=True)
        try:
            self.cli.start(self.container['Id'])

            line = ''
            res_lines = []
            for line in lines:
                if hasattr(line, 'decode'):
                    # Note that lines returned can also be byte sequences so we have to handle decode here
                    line = line.decode('utf-8')
                line = line.strip()
                res_lines.append(line)
                self.log.info(line)

            result = self.cli.wait(self.container['Id'])
            if result['StatusCode'] != 0:
                res_lines = "\n".join(res_lines)
                raise AirflowException('docker container failed: ' + repr(result) + f"lines {res_lines}")
            return (res_lines, line)
        finally:
            if self.auto_remove:
                self.cli.remove_container(self.container['Id'])

    def _pull_image(self):
        self.log.info('Pulling docker image %s', self.image)
        latest_status = {}
        for output in self.cli.pull(self.image, stream=True, decode=True):
            if isinstance(output, str):
                self.log.info("%s", output)
                continue
            if isinstance(output, dict) and 'status' in output:
                output_status = output["status"]
                if 'id' not in output:
                    self.log.info("%s", output_status)
                    continue

                output_id = output["id"]
                if latest_status.get(output_id) != output_status:
                    self.log.info("%s: %s", output_id, output_status)
                    latest_status[output_id] = output_status

    def stop(self):
        self.log.info('Stopping docker container')
        self.cli.stop(self.container['Id'])

    @staticmethod
    def format_command(command: Union[str, List[str]]) -> Union[List[str], str]:
        """
        Retrieve command(s). if command string starts with [, it returns the command list)

        :param command: Docker command or entrypoint
        :type command: str | List[str]

        :return: the command (or commands)
        :rtype: str | List[str]
        """
        if isinstance(command, str) and command.strip().find('[') == 0:
            return ast.literal_eval(command)
        return command
