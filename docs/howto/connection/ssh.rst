..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

SSH Connection
==============
The SSH connection type provides connection to use :class:`~airflow.contrib.hooks.ssh_hook.SSHHook` to run commands on a remote server using :class:`~airflow.contrib.operators.ssh_operator.SSHOperator` or transfer file from/to the remote server using :class:`~airflow.contrib.operators.ssh_operator.SFTPOperator`.

Configuring the Connection
--------------------------
Host (required)
    The Remote host to connect.

Username (optional)
    The Username to connect to the remote_host.

Password (optional)
    Specify the password of the username to connect to the remote_host.

Port (optional)
    Port of remote host to connect. Default is 22.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in ssh
    connection. The following parameters out of the standard python parameters
    are supported:

    * **timeout** - An optional timeout (in seconds) for the TCP connect. Default is ``10``.
    * **compress** - ``true`` to ask the remote client/server to compress traffic; `false` to refuse compression. Default is ``true``.
    * **no_host_key_check** - Set to ``false`` to restrict connecting to hosts with no entries in ``~/.ssh/known_hosts`` (Hosts file). This provides maximum protection against trojan horse attacks, but can be troublesome when the ``/etc/ssh/ssh_known_hosts`` file is poorly maintained or connections to new hosts are frequently made. This option forces the user to manually add all new hosts. Default is ``true``, ssh will automatically add new host keys to the user known hosts files.
    * **allow_host_key_change** - Set to ``true`` if you want to allow connecting to hosts that has host key changed or when you get 'REMOTE HOST IDENTIFICATION HAS CHANGED' error.  This wont protect against Man-In-The-Middle attacks. Other possible solution is to remove the host entry from ``~/.ssh/known_hosts`` file. Default is ``false``.

    Example "extras" field:

    .. code-block:: json

       {
          "timeout": "10",
          "compress": "false",
          "no_host_key_check": "false",
          "allow_host_key_change": "false"
       }

    When specifying the connection as URI (in AIRFLOW_CONN_* variable) you should specify it
    following the standard syntax of connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

        ssh://user:pass@localhost:22?timeout=10&compress=false&no_host_key_check=false&allow_host_key_change=true
