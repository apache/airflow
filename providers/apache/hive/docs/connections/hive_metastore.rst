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



.. _howto/connection:hive_metastore:

Hive Metastore Connection
=========================

The Hive Metastore connection type enables the Hive Metastore Integrations.

Authenticating to Hive Metastore
--------------------------------

Authentication with the Hive Metastore through `Apache Thrift Hive Server
<https://cwiki.apache.org/confluence/display/Hive/HiveServer>`_
and the `hmsclient
<https://pypi.org/project/hmsclient/>`_.


Default Connection IDs
----------------------

All hooks and operators related to the Hive Metastore use ``metastore_default`` by default.

Configuring the Connection
--------------------------

Host (optional)
    The host of your Hive Metastore node. It is possible to specify multiple hosts as a comma-separated list.

Port (optional)
    Your Hive Metastore port number.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Hive Metastore connection.
    The following parameters are all optional:

    * ``auth_mechanism``
      Specify the mechanism for authentication. Default is ``NOSASL``.
    * ``kerberos_service_name``
      Specify the kerberos service name. Default is ``hive``.


When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_METASTORE_DEFAULT='hive-metastore://hive-metastore-node:80?auth_mechanism=NOSASL'
