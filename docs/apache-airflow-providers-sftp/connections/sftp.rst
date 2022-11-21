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

1. Use ``login`` and ``password``.
2. Use ``private_key`` or ``key_file``, along with the optional ``private_key_passphrase``

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

    * ``key_file`` - Full Path of the private SSH Key file that will be used to connect to the remote_host.
    * ``private_key`` - Content of the private key used to connect to the remote_host.
    * ``private_key_passphrase`` - Content of the private key passphrase used to decrypt the private key.
    * ``conn_timeout`` - An optional timeout (in seconds) for the TCP connect. Default is ``10``.
    * ``timeout`` - Deprecated - use conn_timeout instead.
    * ``compress`` - ``true`` to ask the remote client/server to compress traffic; ``false`` to refuse compression. Default is ``true``.
    * ``no_host_key_check`` - Set to ``false`` to restrict connecting to hosts with no entries in ``~/.ssh/known_hosts`` (Hosts file). This provides maximum protection against trojan horse attacks, but can be troublesome when the ``/etc/ssh/ssh_known_hosts`` file is poorly maintained or connections to new hosts are frequently made. This option forces the user to manually add all new hosts. Default is ``true``, ssh will automatically add new host keys to the user known hosts files.
    * ``allow_host_key_change`` - Set to ``true`` if you want to allow connecting to hosts that has host key changed or when you get 'REMOTE HOST IDENTIFICATION HAS CHANGED' error.  This won't protect against Man-In-The-Middle attacks. Other possible solution is to remove the host entry from ``~/.ssh/known_hosts`` file. Default is ``false``.
    * ``look_for_keys`` - Set to ``false`` if you want to disable searching for discoverable private key files in ``~/.ssh/``
    * ``host_key`` - The base64 encoded ssh-rsa public key of the host or "ssh-<key type> <key data>" (as you would find in the ``known_hosts`` file). Specifying this allows making the connection if and only if the public key of the endpoint matches this value.
    * ``disabled_algorithms`` - A dictionary mapping algorithm type to an iterable of algorithm identifiers, which will be disabled for the lifetime of the transport.
    * ``ciphers`` - A list of ciphers to use in order of preference.

Example "extras" field using ``host_key``:

.. code-block:: json

    {
       "no_host_key_check": "false",
       "allow_host_key_change": "false",
       "host_key": "AAAHD...YDWwq=="
    }

Example "extras" field using ``key_file`` or ``private_key``:

.. code-block:: json

    {
       "key_file": "path/to/private_key",
       "no_host_key_check": "true"
    }

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

Example connection string with ``key_file``  (path to key file provided in connection):

.. code-block:: bash

   export AIRFLOW_CONN_SFTP_DEFAULT='sftp://user:pass@localhost:22?key_file=%2Fhome%2Fairflow%2F.ssh%2Fid_rsa'

Example connection string with ``host_key``:

.. code-block:: bash

    AIRFLOW_CONN_SFTP_DEFAULT='sftp://user:pass@localhost:22?host_key=AAAHD...YDWwq%3D%3D&no_host_key_check=false'
