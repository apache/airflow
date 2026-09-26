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



.. _howto/connection:jupyter_kernel:

Jupyter Kernel Connection
=========================

The Jupyter Kernel connection type enables remote kernel connections.


Default Connection ID
---------------------

  The default Jupyter Kernel connection ID is ``jupyter_kernel_default``.

The connection supports two modes:

* **Raw ZMQ kernel**: connect directly to the ZMQ ports of an already-running kernel
  (the host is a plain hostname/IP).
* **Jupyter server / kernel gateway over HTTP(S)**: start (or attach to) a kernel through the
  REST + WebSocket API of a Jupyter server or kernel gateway — e.g. a JupyterHub user server,
  Jupyter Kernel Gateway, or Enterprise Gateway — authenticated with a token. This mode is
  selected when the host is an ``http://`` or ``https://`` URL, or when the ``use_gateway``
  extra is set to ``true``.

Configuring the Connection (raw ZMQ kernel)
-------------------------------------------

host
    HOSTNAME/IP of the remote Jupyter Kernel

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in kernel connection.
    All parameters are optional.

    * ``session_key``: Session key to initiate a connection to remote kernel [default: ''].
    * ``shell_port``: SHELL port [default: 60316].
    * ``iopub_port``: IOPUB port [default: 60317].
    * ``stdin_port``: STDIN port [default: 60318].
    * ``control_port``: CONTROL port [default: 60319].
    * ``hb_port``: HEARTBEAT port [default: 60320].

Configuring the Connection (Jupyter server / kernel gateway over HTTP(S))
-------------------------------------------------------------------------

host
    Base URL of the Jupyter server or kernel gateway, e.g. ``https://gateway.example.com`` or
    ``https://jupyterhub.example.com/user/alice``. When the URL scheme is omitted and the
    ``use_gateway`` extra is set, ``https://`` is assumed.

port (optional)
    Port of the Jupyter server or kernel gateway. May also be embedded in the host URL.

password (optional)
    Authentication token sent in the authorization header of every request
    (including the kernel channels WebSocket connection).

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in the gateway connection.
    All parameters are optional.

    * ``use_gateway``: Force gateway mode when the host has no URL scheme [default: ``false``].
      Boolean extras accept JSON booleans or the strings ``"true"``/``"false"``.
    * ``token``: Authentication token; alternative to the password field.
    * ``auth_scheme``: Scheme prefix of the authorization header value [default: ``token``].
    * ``auth_header_key``: Name of the authorization header [default: ``Authorization``].
    * ``verify_ssl``: Verify the server's TLS certificate [default: ``true``].
    * ``ca_certs``: Path to a CA certificate bundle used for TLS verification.
    * ``client_cert``: Path to a client TLS certificate.
    * ``client_key``: Path to the key of the client TLS certificate.
    * ``request_timeout``: Timeout (seconds) for HTTP requests to the gateway. Values below
      the kernel launch timeout (~42 seconds) are raised to it by ``jupyter_server``.
    * ``connect_timeout``: Timeout (seconds) for establishing HTTP connections.
    * ``kernel_id``: Attach to this pre-existing kernel instead of starting a new one.
      The kernel is left running after the task completes; the task fails if no kernel
      with this id exists on the gateway. Without it, a new kernel is started (using the
      operator's ``kernel_name``) and shut down after execution.
    * ``headers``: Additional HTTP headers (as json dictionary) sent with every request.

If you are configuring the connection via a URI, ensure that all components of the URI are URL-encoded.

Examples
--------

**Set Remote Kernel Connection as Environment Variable (URI)**
  .. code-block:: bash

     export AIRFLOW_CONN_JUPYTER_KERNEL_DEFAULT='{"host": "remote_host", "extra": {"session_key": "notebooks"}}'

**Set Kernel Gateway Connection as Environment Variable (JSON)**
  .. code-block:: bash

     export AIRFLOW_CONN_JUPYTER_KERNEL_DEFAULT='{"conn_type": "jupyter_kernel", "host": "https://gateway.example.com", "port": 8888, "password": "your-token", "extra": {"verify_ssl": true}}'

**Snippet for create Connection as URI**:
  .. code-block:: python

    from airflow.models.connection import Connection

    conn = Connection(
        conn_id="jupyter_kernel_default",
        conn_type="jupyter_kernel",
        host="remote_host",
        extra={
            # Specify extra parameters here
            "session_key": "notebooks",
        },
    )

    # Generate Environment Variable Name
    env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"

    print(f"{env_key}='{conn.get_uri()}'")
