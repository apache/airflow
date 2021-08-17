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



.. _howto/connection:sftp:

SFTP Connection
===============

The SFTP connection type enables SFTP Integrations.

Authenticating to SFTP
-----------------------

There are two ways to connect to SFTP using Airflow.

1. Use `host key
   <https://pysftp.readthedocs.io/en/release_0.2.9/pysftp.html#pysftp.CnOpts>`_
   i.e. host key entered in extras value ``host_key``.
2. Use a `private key, private key pass, or password
   <https://pysftp.readthedocs.io/en/release_0.2.9/pysftp.html#pysftp.Connection>`_
   i.e. use the ``private_key``, ``private_key_pass``, or ``private_key`` extra values.

Only one authorization method can be used at a time. If you need to manage multiple credentials or keys then you should
configure multiple connections.

Default Connection IDs
----------------------

Hooks, operators, and sensors related to SFTP use ``sftp_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify the sftp username for the remote machine.

Password (optional)
    Specify the sftp password for the remote machine.

Port (optional)
    Specify the SSH port of the remote machine

Host (optional)
    Specify the Hostname or IP of the remote machine

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in sftp connection.
    The following parameters are all optional:

    * ``private_key_pass``: Specify the password to use, if private_key is encrypted.
    * ``no_host_key_check``: Set to false to restrict connecting to hosts with either no entries in ~/.ssh/known_hosts
      (Hosts file) or not present in the host_key extra. This provides maximum protection against trojan horse attacks,
      but can be troublesome when the /etc/ssh/ssh_known_hosts file is poorly maintained or connections to new hosts are
      frequently made. This option forces the user to manually add all new hosts. Default is true, ssh will automatically
      add new host keys to the user known hosts files.
    * ``host_key``: The base64 encoded ssh-rsa public key of the host, as you would find in the known_hosts file.
      Specifying this, along with no_host_key_check=False allows you to only make the connection if the public key of
      the endpoint matches this value.
    * ``private_key`` Specify the path to private key file(str) or paramiko.AgentKey

Example “extras” field:

.. code-block:: bash

    {
       "private_key": "path/to/private_key",
       "no_host_key_check": "false",
       "allow_host_key_change": "false",
       "host_key": "AAAHD...YDWwq=="
    }

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

Example connection string with ``private_key``:

.. code-block:: bash

   export AIRFLOW_CONN_SFTP_DEFAULT='sftp://user:pass@localhost:22?private_key=64bit-encoded-private-key'

Example connection string with ``host_key``:

.. code-block:: bash

    AIRFLOW_CONN_SFTP_DEFAULT='sftp://user:pass@localhost:22?host_key=AAAHD...YDWwq%3D%3D&no_host_key_check=false'
