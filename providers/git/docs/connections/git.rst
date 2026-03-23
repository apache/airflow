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



.. _howto/connection:git:

GIT Connection
==============

The GIT connection type enables the GIT Integrations.

Authenticating to GIT
-----------------------

Authenticate to GIT using `GitPython <https://github.com/gitpython-developers/GitPython>`_.
The hook supports both HTTPS (token-based) and SSH (key-based) authentication.

Default Connection IDs
----------------------

Hooks and bundles related to GIT use ``git_default`` by default.

Configuring the Connection
--------------------------

Repository URL
    The URL of the git repository, e.g. ``git@github.com:apache/airflow.git`` for SSH
    or ``https://github.com/apache/airflow.git`` for HTTPS.
    This can also be passed directly to the hook via the ``repo_url`` parameter.

Username or Access Token name (optional)
    The username for HTTPS authentication or the token name. Defaults to ``user`` if not specified.
    When using HTTPS with an access token, this value is used as the username in the
    authenticated URL (e.g. ``https://user:token@github.com/repo.git``).

Access Token (optional)
    The access token for HTTPS authentication. When provided along with the username,
    the hook injects the credentials into the repository URL for HTTPS cloning.

Extra (optional)
    Specify the extra parameters as a JSON dictionary. The following keys are supported:

    **SSH key authentication:**

    * ``key_file``: Path to an SSH private key file to use for authentication.
    * ``private_key``: An inline SSH private key string. When provided, the hook writes it
      to a temporary file and uses it for the SSH connection.
      Mutually exclusive with ``key_file``.
    * ``private_key_passphrase``: Passphrase for the private key (works with both
      ``key_file`` and ``private_key``). Uses ``SSH_ASKPASS`` to provide the passphrase
      non-interactively.

    **SSH connection options:**

    * ``strict_host_key_checking``: Controls SSH strict host key checking. Defaults to ``no``.
      Set to ``yes`` to enable strict checking.
    * ``known_hosts_file``: Path to a custom SSH known-hosts file. When
      ``strict_host_key_checking`` is ``no`` and this is not set, ``/dev/null`` is used.
    * ``ssh_config_file``: Path to a custom SSH config file (passed as ``ssh -F``).
    * ``host_proxy_cmd``: SSH ProxyCommand string for connecting through a bastion or
      jump host (e.g. ``ssh -W %h:%p bastion.example.com``).
    * ``ssh_port``: Non-default SSH port number.

    Example with key file:

    .. code-block:: json

        {
            "key_file": "/path/to/id_rsa",
            "strict_host_key_checking": "no"
        }

    Example with inline private key and passphrase:

    .. code-block:: json

        {
            "private_key": "<content of your PEM-encoded private key>",
            "private_key_passphrase": "my-passphrase"
        }

    Example with bastion host and custom port:

    .. code-block:: json

        {
            "key_file": "/path/to/id_rsa",
            "host_proxy_cmd": "ssh -W %h:%p bastion.example.com",
            "ssh_port": "2222",
            "strict_host_key_checking": "yes",
            "known_hosts_file": "/path/to/known_hosts"
        }
