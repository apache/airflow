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



.. _howto/connection:docker:

Docker Connection
=================

The Docker connection type enables connection to the Docker registry.

Authenticating to Docker
------------------------

Authenticate to Docker by using the login information for Docker registry.
More information on `Docker authentication here
<https://docker-py.readthedocs.io/en/1.2.3/api/>`_.

Default Connection IDs
----------------------

Some hooks and operators related to Docker use ``docker_default`` by default.

Configuring the Connection
--------------------------

Login
    Specify the Docker registry username.

Password
    Specify the Docker registry plaintext password.

Host
    Specify the URL to the Docker registry. Ex: ``https://index.docker.io/v1``

Port (optional)
    Specify the port if not specified in host.

Extra
    Specify the extra parameters (as json dictionary) that can be used in Azure connection.
    The following parameters are all optional:

    * ``email``: Specify the email used for the registry account.
    * ``reauth``: Specify whether refresh existing authentication on the Docker server. (bool)

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_DOCKER_DEFAULT='docker://username:password@https%3A%2F%2Findex.docker.io%2Fv1:80?email=myemail%40my.com&reauth=False'
