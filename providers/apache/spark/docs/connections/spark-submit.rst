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



.. _howto/connection:spark-submit:

Apache Spark Submit Connection
==============================

The Apache Spark Submit connection type enables connection to Apache Spark via the ``spark-submit`` command.

Default Connection IDs
----------------------

Spark Submit and Spark JDBC hooks and operators use ``spark_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to, it can be ``local``, ``yarn`` or an URL.

Port (optional)
    Specify the port in case of host be an URL.

YARN Queue (optional, only applies to spark on YARN applications)
    The name of the YARN queue to which the application is submitted.

Deploy mode (optional)
    Whether to deploy your driver on the worker nodes (cluster) or locally as an external client (client).

Spark binary (optional)
    The command to use for Spark submit. Some distros may use ``spark2-submit``. Default ``spark-submit``. Only ``spark-submit``, ``spark2-submit`` or ``spark3-submit`` are allowed as value.

Kubernetes namespace (optional, only applies to spark on kubernetes applications)
    Kubernetes namespace (``spark.kubernetes.namespace``) to divide cluster resources between multiple users (via resource quota).

.. note::

  When specifying the connection in environment variable you should specify
  it using URI syntax.
  You can provide a standard Spark master URI directly.
  The master URL will be parsed correctly without needing repeated prefixes such as ``spark://spark://...``
  Ensure all URI components are URL-encoded.

  For example:

  .. code-block:: bash

     export AIRFLOW_CONN_SPARK_DEFAULT='spark://mysparkcluster.com:80?deploy-mode=cluster&spark_binary=command&namespace=kube+namespace'



.. warning::

  Make sure you trust your users with the ability to configure the host settings as it may enable the connection to
  establish communication with external servers. It's crucial to understand that directing the connection towards a
  malicious server can lead to significant security vulnerabilities, including the risk of encountering
  Remote Code Execution (RCE) attacks.
