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



.. _howto/connection:ssh:

SSH Connection
==============
The SSH connection type provides connection to use :class:`~airflow.providers.ssh.hooks.ssh.SSHHook` to run
commands on a remote server using :class:`~airflow.providers.ssh.operators.ssh.SSHOperator` or transfer
file from/to the remote server using :class:`~airflow.providers.sftp.operators.sftp.SFTPOperator`.

Configuring the Connection
--------------------------
Host (required)
    The Remote host to connect.

Username (optional)
    The Username to connect to the ``remote_host``.

Password (optional)
    Specify the password of the username to connect to the ``remote_host``.

Port (optional)
    Port of remote host to connect. Default is ``22``.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in ssh
    connection. The following parameters out of the standard python parameters
    are supported:

    * ``key_file`` - Full Path of the private SSH Key file that will be used to connect to the remote_host.
    * ``private_key`` - Content of the private key used to connect to the remote_host.
    * ``private_key_passphrase`` - Content of the private key passphrase used to decrypt the private key.
    * ``conn_timeout`` - An optional timeout (in seconds) for the TCP connect. Default is ``10``.
    * ``timeout`` - Deprecated - use conn_timeout instead.
    * ``cmd_timeout`` - Timeout (in seconds) for executing the command. The default is 10 seconds. `null` value means no timeout.
    * ``compress`` - ``true`` to ask the remote client/server to compress traffic; ``false`` to refuse compression. Default is ``true``.
    * ``no_host_key_check`` - Set to ``false`` to restrict connecting to hosts with no entries in ``~/.ssh/known_hosts`` (Hosts file). This provides maximum protection against trojan horse attacks, but can be troublesome when the ``/etc/ssh/ssh_known_hosts`` file is poorly maintained or connections to new hosts are frequently made. This option forces the user to manually add all new hosts. Default is ``true``, ssh will automatically add new host keys to the user known hosts files.
    * ``allow_host_key_change`` - Set to ``true`` if you want to allow connecting to hosts that has host key changed or when you get 'REMOTE HOST IDENTIFICATION HAS CHANGED' error.  This won't protect against Man-In-The-Middle attacks. Other possible solution is to remove the host entry from ``~/.ssh/known_hosts`` file. Default is ``false``.
    * ``look_for_keys`` - Set to ``false`` if you want to disable searching for discoverable private key files in ``~/.ssh/``
    * ``host_key`` - The base64 encoded ssh-rsa public key of the host or "ssh-<key type> <key data>" (as you would find in the ``known_hosts`` file). Specifying this allows making the connection if and only if the public key of the endpoint matches this value.
    * ``disabled_algorithms`` - A dictionary mapping algorithm type to an iterable of algorithm identifiers, which will be disabled for the lifetime of the transport.
    * ``ciphers`` - A list of ciphers to use in order of preference.

    Example "extras" field:

    .. code-block:: json

       {
          "key_file": "/home/airflow/.ssh/id_rsa",
          "conn_timeout": "10",
          "compress": "false",
          "look_for_keys": "false",
          "allow_host_key_change": "false",
          "host_key": "AAAHD...YDWwq==",
          "disabled_algorithms": {"pubkeys": ["rsa-sha2-256", "rsa-sha2-512"]},
          "ciphers": ["aes128-ctr", "aes192-ctr", "aes256-ctr"]
       }

    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example, to provide a connection string with ``key_file`` (which contains the path to the key file):

    .. code-block:: bash

        export AIRFLOW_CONN_MAIN_SERVER='ssh://user:pass@localhost:22?conn_timeout=10&compress=false&no_host_key_check=false&allow_host_key_change=true&key_file=%2Fhome%2Fairflow%2F.ssh%2Fid_rsa'

    Private keys can be encoded into a one-liner for usage in an environment variable as follows:

    .. code-block:: bash

       python -c 'from urllib.parse import quote_plus, sys; print(quote_plus(sys.stdin.read()))' < /path/to/your/key

    You can then export this as an environment variable:

    .. code-block:: bash

        export AIRFLOW_CONN_SSH_SERVER='ssh://127.0.0.1?private_key=-----BEGIN+RSA+PRIVATE+KEY-----%0D%0AMII.....jBV50%0D%0A-----END+RSA+PRIVATE+KEY-----'

    To configure a private key in the extras in the Airflow UI, you can replace newlines by literal ``\n``:

    .. code-block:: bash

       python -c 'import re, sys; print(re.sub("\r\n", "\\\\n", sys.stdin.read()))' < /path/to/your/key

    You can then provide the result in the extras JSON as:

    .. code-block:: json

        {"private_key": "-----BEGIN RSA PRIVATE KEY-----\nMII.....jBV50\n-----END RSA PRIVATE KEY-----"}
