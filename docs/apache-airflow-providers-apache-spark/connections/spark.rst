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



.. _howto/connection:spark:

Apache Spark Connection
=======================

The Apache Spark connection type enables connection to Apache Spark.

Default Connection IDs
----------------------

Spark Submit and Spark JDBC hooks and operators use ``spark_default`` by default. Spark SQL hooks and operators point to ``spark_sql_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to, it can be ``local``, ``yarn`` or an URL.

Port (optional)
    Specify the port in case of host be an URL.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in spark connection. The following parameters out of the standard python parameters are supported:

    * ``queue`` - The name of the YARN queue to which the application is submitted.
    * ``deploy-mode`` - Whether to deploy your driver on the worker nodes (cluster) or locally as an external client (client).
    * ``spark-binary`` - The command to use for Spark submit. Some distros may use ``spark2-submit``. Default ``spark-submit``. Only ``spark-submit`` and ``spark2-submit`` are allowed as value.
    * ``namespace`` - Kubernetes namespace (``spark.kubernetes.namespace``) to divide cluster resources between multiple users (via resource quota).

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded. The URI and and the mongo
connection string are not the same.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_SPARK_DEFAULT='spark://mysparkcluster.com:80?deploy-mode=cluster&spark_binary=command&namespace=kube+namespace'
