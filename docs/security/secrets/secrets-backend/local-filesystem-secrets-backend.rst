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

.. _local_filesystem_secrets:

Local Filesystem Secrets Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This backend is especially useful in the following use cases:

* **Development**: It ensures data synchronization between all terminal windows (same as databases),
  and at the same time the values are retained after database restart (same as environment variable)
* **Kubernetes**: It allows you to store secrets in `Kubernetes Secrets <https://kubernetes.io/docs/concepts/configuration/secret/>`__
  or you can synchronize values using the sidecar container and
  `a shared volume <https://kubernetes.io/docs/tasks/access-application-cluster/communicate-containers-same-pod-shared-volume/>`__

To use variable and connection from local file, specify :py:class:`~airflow.secrets.local_filesystem.LocalFilesystemBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Available parameters to ``backend_kwargs``:

* ``variables_file_path``: File location with variables data.
* ``connections_file_path``: File location with connections data.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.secrets.local_filesystem.LocalFilesystemBackend
    backend_kwargs = {"variables_file_path": "/files/var.json", "connections_file_path": "/files/conn.json"}

``JSON``, ``YAML`` and ``.env`` files are supported. All parameters are optional. If the file path is not passed,
the backend returns an empty collection.

Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

If you have set ``connections_file_path`` as ``/files/my_conn.json``, then the backend will read the
file ``/files/my_conn.json`` when it looks for connections.

The file can be defined in ``JSON``, ``YAML`` or ``env`` format. Depending on the format, the data should be saved as a URL or as a connection object.
Any extra json parameters can be provided using keys like ``extra_dejson`` and ``extra``.
The key ``extra_dejson`` can be used to provide parameters as JSON object where as the key ``extra`` can be used in case of a JSON string.
The keys ``extra`` and ``extra_dejson`` are mutually exclusive.

The JSON file must contain an object where the key contains the connection ID and the value contains
the definition of one connection. The connection can be defined as a URI (string) or JSON object.
For a guide about defining a connection as a URI, see:: :ref:`generating_connection_uri`.
For a description of the connection object parameters see :class:`~airflow.models.connection.Connection`.
The following is a sample JSON file.

.. code-block:: json

    {
        "CONN_A": "mysql://host_a",
        "CONN_B": {
            "conn_type": "scheme",
            "host": "host",
            "schema": "schema",
            "login": "Login",
            "password": "None",
            "port": "1234"
        }
    }

The YAML file structure is similar to that of a JSON. The key-value pair of connection ID and the definitions of one or more connections.
In this format, the connection can be defined as a URI (string) or JSON object.

.. code-block:: yaml

    CONN_A: 'mysql://host_a'

    CONN_B:
      - 'mysql://host_a'
      - 'mysql://host_b'

    CONN_C:
      conn_type: scheme
      host: host
      schema: lschema
      login: Login
      password: None
      port: 1234
      extra_dejson:
        a: b
        nestedblock_dict:
          x: y

You can also define connections using a ``.env`` file. Then the key is the connection ID, and
the value should describe the connection using the URI. Connection ID should not be repeated, it will
raise an exception. The following is a sample file.

  .. code-block:: text

    mysql_conn_id=mysql://log:password@13.1.21.1:3306/mysqldbrd
    google_custom_key=google-cloud-platform://?extra__google_cloud_platform__key_path=%2Fkeys%2Fkey.json

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_file_path`` as ``/files/my_var.json``, then the backend will read the
file ``/files/my_var.json`` when it looks for variables.

The file can be defined in ``JSON``, ``YAML`` or ``env`` format.

The JSON file must contain an object where the key contains the variable key and the value contains
the variable value. The following is a sample JSON file.

  .. code-block:: json

    {
        "VAR_A": "some_value",
        "var_b": "different_value"
    }

The YAML file structure is similar to that of JSON, with key containing the variable key and the value containing
the variable value. The following is a sample YAML file.

  .. code-block:: yaml

    VAR_A: some_value
    VAR_B: different_value

You can also define variable using a ``.env`` file. Then the key is the variable key, and variable should
describe the variable value. The following is a sample file.

  .. code-block:: text

    VAR_A=some_value
    var_B=different_value
