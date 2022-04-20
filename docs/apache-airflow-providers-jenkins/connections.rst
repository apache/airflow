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



.. _howto/connection:dbt-cloud:

Jenkins Connecting
=======================

Jenkins connection type provides connection to a jenkins server.

Default Connection ID
~~~~~~~~~~~~~~~~~~~~~

All hooks and operators related to Jenkins use ``jenkins_default`` by default.


Configuring the connection
~~~~~~~~~~~~~~~~~~~~~~~~~~

Login
    Specify the login for the Jenkins service you would like to connect to.

Password
    Specify the password for the Jenkins service you would like to connect too.

Host
    Specify host for your Jenkins server. This should ``NOT`` contain the scheme of the address (``http://`` or ``https://``).

    Example: ``jenkins.example.com``

Port
    Specify a port number.

Extras (optional)
    Specify whether you want to use ``http`` or ``https`` scheme by entering ``true`` to use ``https`` or ``false`` for ``http`` in extras.  Default is ``http``.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_JENKINS_DEFAULT='http://username:password@server.com:443'
