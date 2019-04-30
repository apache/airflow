# -*- coding: utf-8 -*-
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

import json

from airflow.hooks.docker_hook import DockerHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from docker import APIClient, tls
import ast


class DockerOperator(BaseOperator):
    """
    Execute a command inside a docker container.

    A temporary directory is created on the host and
    mounted into a container to allow storing files
    that together exceed the default disk size of 10GB in a container.
    The path to the mounted directory can be accessed
    via the environment variable ``AIRFLOW_TMP_DIR``.

    If a login to a private registry is required prior to pulling the image, a
    Docker connection needs to be configured in Airflow and the connection ID
    be provided with the parameter ``docker_conn_id``.

    :param image: Docker image from which to create the container.
        If image tag is omitted, "latest" will be used.
    :type image: str
    :param api_version: Remote API version. Set to ``auto`` to automatically
        detect the server's version.
    :type api_version: str
    :param auto_remove: Auto-removal of the container on daemon side when the
        container's process exits.
        The default is False.
    :type auto_remove: bool
    :param command: Command to be run in the container. (templated)
    :type command: str or list
    :param cpus: Number of CPUs to assign to the container.
        This value gets multiplied with 1024. See
        https://docs.docker.com/engine/reference/run/#cpu-share-constraint
    :type cpus: float
    :param dns: Docker custom DNS servers
    :type dns: list[str]
    :param dns_search: Docker custom DNS search domain
    :type dns_search: list[str]
    :param docker_url: URL of the host running the docker daemon.
        Default is unix://var/run/docker.sock
    :type docker_url: str
    :param environment: Environment variables to set in the container. (templated)
    :type environment: dict
    :param force_pull: Pull the docker image on every run. Default is False.
    :type force_pull: bool
    :param mem_limit: Maximum amount of memory the container can use.
        Either a float value, which represents the limit in bytes,
        or a string like ``128m`` or ``1g``.
    :type mem_limit: float or str
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
    :param volumes: List of volumes to mount into the container, e.g.
        ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
    :type volumes: list
    :param working_dir: Working directory to
        set on the container (equivalent to the -w switch the docker client)
    :type working_dir: str
    :param xcom_all: Push all the stdout or just the last line.
        The default is False (last line).
    :type xcom_all: bool
    :param docker_conn_id: ID of the Airflow connection to use
    :type docker_conn_id: str
    :param shm_size: Size of ``/dev/shm`` in bytes. The size must be
        greater than 0. If omitted uses system default.
    :type shm_size: int
    """
    template_fields = ('command', 'environment',)
    template_ext = ('.sh', '.bash',)

    @apply_defaults
    def __init__(
            self,
            image,
            api_version=None,
            command=None,
            cpus=1.0,
            docker_url='unix://var/run/docker.sock',
            environment=None,
            force_pull=False,
            mem_limit=None,
            network_mode=None,
            tls_ca_cert=None,
            tls_client_cert=None,
            tls_client_key=None,
            tls_hostname=None,
            tls_ssl_version=None,
            tmp_dir='/tmp/airflow',
            user=None,
            volumes=None,
            working_dir=None,
            xcom_all=False,
            docker_conn_id=None,
            dns=None,
            dns_search=None,
            auto_remove=False,
            shm_size=None,
            *args,
            **kwargs):

        super().__init__(*args, **kwargs)
        self.api_version = api_version
        self.auto_remove = auto_remove
        self.command = command
        self.cpus = cpus
        self.dns = dns
        self.dns_search = dns_search
        self.docker_url = docker_url
        self.environment = environment or {}
        self.force_pull = force_pull
        self.image = image
        self.mem_limit = mem_limit
        self.network_mode = network_mode
        self.tls_ca_cert = tls_ca_cert
        self.tls_client_cert = tls_client_cert
        self.tls_client_key = tls_client_key
        self.tls_hostname = tls_hostname
        self.tls_ssl_version = tls_ssl_version
        self.tmp_dir = tmp_dir
        self.user = user
        self.volumes = volumes or []
        self.working_dir = working_dir
        self.xcom_all = xcom_all
        self.docker_conn_id = docker_conn_id
        self.shm_size = shm_size
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

        self.cli = None
        self.container = None

    def get_hook(self):
        return DockerHook(
            docker_conn_id=self.docker_conn_id,
            base_url=self.docker_url,
            version=self.api_version,
            tls=self.__get_tls_config()
        )

    def execute(self, context):
        self.log.info('Starting docker container from image %s', self.image)

        tls_config = self.__get_tls_config()

        if self.docker_conn_id:
            self.cli = self.get_hook().get_conn()
        else:
            self.cli = APIClient(
                base_url=self.docker_url,
                version=self.api_version,
                tls=tls_config
            )

        if self.force_pull or len(self.cli.images(name=self.image)) == 0:
            self.log.info('Pulling docker image %s', self.image)
            for l in self.cli.pull(self.image, stream=True):
                output = json.loads(l.decode('utf-8').strip())
                if 'status' in output:
                    self.log.info("%s", output['status'])

        with TemporaryDirectory(prefix='airflowtmp') as host_tmp_dir:
            self.environment['AIRFLOW_TMP_DIR'] = self.tmp_dir
            self.volumes.append('{0}:{1}'.format(host_tmp_dir, self.tmp_dir))

            self.container = self.cli.create_container(
                command=self.get_command(),
                environment=self.environment,
                host_config=self.cli.create_host_config(
                    auto_remove=self.auto_remove,
                    binds=self.volumes,
                    network_mode=self.network_mode,
                    shm_size=self.shm_size,
                    dns=self.dns,
                    dns_search=self.dns_search,
                    cpu_shares=int(round(self.cpus * 1024)),
                    mem_limit=self.mem_limit),
                image=self.image,
                user=self.user,
                working_dir=self.working_dir
            )
            self.cli.start(self.container['Id'])

            line = ''
            for line in self.cli.logs(container=self.container['Id'], stream=True):
                line = line.strip()
                if hasattr(line, 'decode'):
                    line = line.decode('utf-8')
                self.log.info(line)

            result = self.cli.wait(self.container['Id'])
            if result['StatusCode'] != 0:
                raise AirflowException('docker container failed: ' + repr(result))

            # duplicated conditional logic because of expensive operation
            if self.do_xcom_push:
                return self.cli.logs(container=self.container['Id']) \
                    if self.xcom_all else line.encode('utf-8')

    def get_command(self):
        if self.command is not None and self.command.strip().find('[') == 0:
            commands = ast.literal_eval(self.command)
        else:
            commands = self.command
        return commands

    def on_kill(self):
        if self.cli is not None:
            self.log.info('Stopping docker container')
            self.cli.stop(self.container['Id'])

    def __get_tls_config(self):
        tls_config = None
        if self.tls_ca_cert and self.tls_client_cert and self.tls_client_key:
            tls_config = tls.TLSConfig(
                ca_cert=self.tls_ca_cert,
                client_cert=(self.tls_client_cert, self.tls_client_key),
                verify=True,
                ssl_version=self.tls_ssl_version,
                assert_hostname=self.tls_hostname
            )
            self.docker_url = self.docker_url.replace('tcp://', 'https://')
        return tls_config
