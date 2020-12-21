:mod:`airflow.providers.docker.operators.docker_swarm`
======================================================

.. py:module:: airflow.providers.docker.operators.docker_swarm

.. autoapi-nested-parse::

   Run ephemeral Docker Swarm services



Module Contents
---------------

.. py:class:: DockerSwarmOperator(*, image: str, enable_logging: bool = True, **kwargs)

   Bases: :class:`airflow.providers.docker.operators.docker.DockerOperator`

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
   :param tty: Allocate pseudo-TTY to the container of this service
       This needs to be set see logs of the Docker container / service.
   :type tty: bool
   :param enable_logging: Show the application's logs in operator's logs.
       Supported only if the Docker engine is using json-file or journald logging drivers.
       The `tty` parameter should be set to use this with Python applications.
   :type enable_logging: bool

   
   .. method:: execute(self, context)



   
   .. method:: _run_service(self)



   
   .. method:: _service_status(self)



   
   .. method:: _has_service_terminated(self)



   
   .. method:: _stream_logs_to_output(self)



   
   .. method:: on_kill(self)




