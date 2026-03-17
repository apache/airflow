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

    * ``key_file``: Path to an SSH private key file to use for authentication.
    * ``private_key``: An inline SSH private key string. When provided, the hook writes it
      to a temporary file and uses it for the SSH connection.
    * ``strict_host_key_checking``: Controls SSH strict host key checking. Defaults to ``no``.
      Set to ``yes`` to enable strict checking.

    Example:

    .. code-block:: json

        {
            "key_file": "/path/to/id_rsa",
            "strict_host_key_checking": "no"
        }

    Or with an inline private key:

    .. code-block:: json

        {
            "private_key": "<content of your PEM-encoded private key>"
        }
