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



.. _howto/connection:hive_cli:

Hive CLI Connection
===================

The Hive CLI connection type enables the Hive CLI Integrations.

Authenticating to Hive CLI
--------------------------

There are two ways to connect to Hive using Airflow.

1. Use the `Hive Beeline
   <https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.1.5/bk_dataintegration/content/ch_using-hive-clients-examples.html>`_.
   i.e. make a JDBC connection string with host, port, and schema. Optionally you can connect with a proxy user, and specify a login and password.

2. Use the `Hive CLI
   <https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.4/starting-hive/content/hive_start_a_command_line_query_locally.html>`_.
   i.e. specify Hive CLI params in the extras field.

Only one authorization method can be used at a time. If you need to manage multiple credentials or keys then you should
configure multiple connections.

Default Connection IDs
----------------------

All hooks and operators related to Hive_CLI use ``hive_cli_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify your username for a proxy user or for the Beeline CLI.

Password (optional)
    Specify your Beeline CLI password.

Host (optional)
    Specify your JDBC Hive host that is used for Hive Beeline.

Port (optional)
    Specify your JDBC Hive port that is used for Hive Beeline.

Schema (optional)
    Specify your JDBC Hive database that you want to connect to with Beeline
    or specify a schema for an HQL statement to run with the Hive CLI.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Hive CLI connection.
    The following parameters are all optional:

    * ``use_beeline``
      Specify as ``True`` if using the Beeline CLI. Default is ``False``.
    * ``auth``
      Specify the auth type for use with Hive Beeline CLI.
    * ``proxy_user``
      Specify a proxy user as an ``owner`` or ``login`` or keep blank if using a
      custom proxy user.
    * ``principal``
      Specify the JDBC Hive principal to be used with Hive Beeline.


When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_HIVE_CLI_DEFAULT='hive-cli://beeline-username:beeline-password@jdbc-hive-host:80/hive-database?hive_cli_params=params&use_beeline=True&auth=noSasl&principal=hive%2F_HOST%40EXAMPLE.COM'
