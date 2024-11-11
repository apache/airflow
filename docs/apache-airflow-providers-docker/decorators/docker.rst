.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



.. _howto/decorator:docker:

Task Docker Decorator
=====================

Python callable wrapped within the ``@task.docker`` decorator with args are executed within
the docker container.

Parameters
----------

The following parameters are supported in Docker Task decorator.

multiple_outputs
    If set, function return value will be unrolled to multiple XCom values.
    Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
use_dill
    Whether to use dill or pickle for serialization
python_command
    Python command for executing functions, Default python3
image
    Docker image from which to create the container.
    If image tag is omitted, "latest" will be used.
api_version
    Remote API version. Set to ``auto`` to automatically detect the server's version.
container_name
    Name of the container. Optional (templated)
cpus
    Number of CPUs to assign to the container. This value gets multiplied with 1024.
docker_url
    URL of the host running the docker daemon.
    Default is the value of the ``DOCKER_HOST`` environment variable or unix://var/run/docker.sock
    if it is unset.
environment
    Environment variables to set in the container. (templated)
private_environment
    Private environment variables to set in the container.
    These are not templated, and hidden from the website.
env_file
    Relative path to the ``.env`` file with environment variables to set in the container.
    Overridden by variables in the environment parameter.
force_pull
    Pull the docker image on every run. Default is False.
mem_limit
    Maximum amount of memory the container can use.
    Either a float value, which represents the limit in bytes,
    or a string like ``128m`` or ``1g``.
host_tmp_dir
    Specify the location of the temporary directory on the host which will
    be mapped to tmp_dir. If not provided defaults to using the standard system temp directory.
network_mode
    Network mode for the container. It can be one of the following

    - ``"bridge"``: Create new network stack for the container with default docker bridge network
    - ``"none"``: No networking for this container
    - ``"container:<name>"`` or ``"container:<id>"``: Use the network stack of another container specified via <name> or <id>
    - ``"host"``: Use the host network stack. Incompatible with **port_bindings**
    - ``"<network-name>"`` or ``"<network-id>"``: Connects the container to user created network (using ``docker network create`` command)
tls_ca_cert
    Path to a PEM-encoded certificate authority to secure the docker connection.
tls_client_cert
    Path to the PEM-encoded certificate used to authenticate docker client.
tls_client_key
    Path to the PEM-encoded key used to authenticate docker client.
tls_verify
    Set ``True`` to verify the validity of the provided certificate.
tls_hostname
    Hostname to match against the docker server certificate or False to disable the check.
tls_ssl_version
    Version of SSL to use when communicating with docker daemon.
mount_tmp_dir
    Specify whether the temporary directory should be bind-mounted from the host to the container.
tmp_dir
    Mount point inside the container to
    a temporary directory created on the host by the operator.
    The path is also made available via the environment variable
    ``AIRFLOW_TMP_DIR`` inside the container.
user
    Default user inside the docker container.
mounts
    List of mounts to mount into the container, e.g.
    ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
working_dir
    Working directory to set on the container (equivalent to the -w switch the docker client)
entrypoint
    Overwrite the default ENTRYPOINT of the image
xcom_all
    Push all the stdout or just the last line. The default is False (last line).
docker_conn_id
    ID of the Airflow connection to use
dns
    Docker custom DNS servers
dns_search
    Docker custom DNS search domain
auto_remove
    Enable removal of the container when the container's process exits. Possible values

    - ``never``: (default) do not remove container
    - ``success``: remove on success
    - ``force``: always remove container
shm_size
    Size of ``/dev/shm`` in bytes. The size must be greater than 0.
    If omitted uses system default.
tty
    Allocate pseudo-TTY to the container
    This needs to be set see logs of the Docker container.
hostname
    Optional hostname for the container.
privileged
    Give extended privileges to this container.
cap_add
    Include container capabilities
extra_hosts
    Additional hostnames to resolve inside the container, as a mapping of hostname to IP address.
timeout
    Default timeout for API calls, in seconds.
device_requests
    Expose host resources such as GPUs to the container.
log_opts_max_size
    The maximum size of the log before it is rolled.
    A positive integer plus a modifier representing the unit of measure (k, m, or g).
    Eg: 10m or 1g Defaults to -1 (unlimited).
log_opts_max_file
    The maximum number of log files that can be present.
    If rolling the logs creates excess files, the oldest file is removed.
    Only effective when max-size is also set. A positive integer. Defaults to 1.
ipc_mode
    Set the IPC mode for the container.
skip_on_exit_code
    If task exits with this exit code, leave the task
    in ``skipped`` state (default: None). If set to ``None``, any non-zero
    exit code will be treated as a failure.
port_bindings
    Publish a container's port(s) to the host. It is a
    dictionary of value where the key indicates the port to open inside the container
    and value indicates the host port that binds to the container port.
    Incompatible with ``"host"`` in ``network_mode``.
ulimits
    List of ulimit options to set for the container.
    Each item should be a ``docker.types.Ulimit`` instance.


Usage Example
-------------

.. exampleinclude:: /../../providers/tests/system/docker/example_taskflow_api_docker_virtualenv.py
    :language: python
    :start-after: [START transform_docker]
    :end-before: [END transform_docker]
