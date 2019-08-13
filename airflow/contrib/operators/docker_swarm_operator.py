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

'''
Run ephemeral Docker Swarm services
'''

from docker import types

from airflow.exceptions import AirflowException
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.strings import get_random_string


class DockerSwarmOperator(DockerOperator):
    """
    Execute a command as an ephemeral docker swarm service.
    Example use-case - Using Docker Swarm orchestration to make one-time
    scripts highly available.

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
    :param docker_conn_id: ID of the Airflow connection to use
    :type docker_conn_id: str
    """

    @apply_defaults
    def __init__(
            self,
            image,
            *args,
            **kwargs):

        super(DockerSwarmOperator, self).__init__(image=image, *args, **kwargs)

        self.service = None

    def _run_image(self):
        self.log.info('Starting docker service from image %s', self.image)

        self.service = self.cli.create_service(
            types.TaskTemplate(
                container_spec=types.ContainerSpec(
                    image=self.image,
                    command=self.get_command(),
                    env=self.environment,
                    user=self.user
                ),
                restart_policy=types.RestartPolicy(condition='none'),
                resources=types.Resources(mem_limit=self.mem_limit)
            ),
            name='airflow-%s' % get_random_string(),
            labels={'name': 'airflow__%s__%s' % (self.dag_id, self.task_id)}
        )

        self.log.info('Service started: %s', str(self.service))

        status = None
        # wait for the service to start the task
        while not self.cli.tasks(filters={'service': self.service['ID']}):
            continue
        while True:

            status = self.cli.tasks(
                filters={'service': self.service['ID']}
            )[0]['Status']['State']
            if status in ['failed', 'complete']:
                self.log.info('Service status before exiting: %s', status)
                break

        if self.auto_remove:
            self.cli.remove_service(self.service['ID'])
        if status == 'failed':
            raise AirflowException('Service failed: ' + repr(self.service))

    def on_kill(self):
        if self.cli is not None:
            self.log.info('Removing docker service: %s', self.service['ID'])
            self.cli.remove_service(self.service['ID'])
