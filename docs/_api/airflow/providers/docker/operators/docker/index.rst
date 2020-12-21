:mod:`airflow.providers.docker.operators.docker`
================================================

.. py:module:: airflow.providers.docker.operators.docker

.. autoapi-nested-parse::

   Implements Docker operator



Module Contents
---------------

.. py:class:: DockerOperator(*, image: str, api_version: Optional[str] = None, command: Optional[Union[str, List[str]]] = None, container_name: Optional[str] = None, cpus: float = 1.0, docker_url: str = 'unix://var/run/docker.sock', environment: Optional[Dict] = None, private_environment: Optional[Dict] = None, force_pull: bool = False, mem_limit: Optional[Union[float, str]] = None, host_tmp_dir: Optional[str] = None, network_mode: Optional[str] = None, tls_ca_cert: Optional[str] = None, tls_client_cert: Optional[str] = None, tls_client_key: Optional[str] = None, tls_hostname: Optional[Union[str, bool]] = None, tls_ssl_version: Optional[str] = None, tmp_dir: str = '/tmp/airflow', user: Optional[Union[str, int]] = None, volumes: Optional[List[str]] = None, working_dir: Optional[str] = None, xcom_all: bool = False, docker_conn_id: Optional[str] = None, dns: Optional[List[str]] = None, dns_search: Optional[List[str]] = None, auto_remove: bool = False, shm_size: Optional[int] = None, tty: bool = False, cap_add: Optional[Iterable[str]] = None, extra_hosts: Optional[Dict[str, str]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param cap_add: Include container capabilities
   :type cap_add: list[str]

   .. attribute:: template_fields
      :annotation: = ['command', 'environment', 'container_name']

      

   .. attribute:: template_ext
      :annotation: = ['.sh', '.bash']

      

   
   .. method:: get_hook(self)

      Retrieves hook for the operator.

      :return: The Docker Hook



   
   .. method:: _run_image(self)

      Run a Docker container with the provided image



   
   .. method:: execute(self, context)



   
   .. method:: _get_cli(self)



   
   .. method:: get_command(self)

      Retrieve command(s). if command string starts with [, it returns the command list)

      :return: the command (or commands)
      :rtype: str | List[str]



   
   .. method:: on_kill(self)



   
   .. method:: __get_tls_config(self)




