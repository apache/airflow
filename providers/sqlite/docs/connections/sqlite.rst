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



SQLite Connection
=================
The SQLite connection type provides connection to a SQLite database.

Configuring the Connection
--------------------------
Host (optional)
    The host to connect to. This can either be a file on disk or an in-memory database. If not set, an in-memory database is being used.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in the sqlite connection.
    See `Recognized Query Parameters <https://www.sqlite.org/uri.html>`_ for all supported parameters.

URI format example
^^^^^^^^^^^^^^^^^^

If serializing with Airflow URI:

.. code-block:: bash

   export AIRFLOW_CONN_SQLITE_DEFAULT='sqlite://relative/path/to/db?mode=ro'

or using an absolute path:

.. code-block:: bash

   export AIRFLOW_CONN_SQLITE_DEFAULT='sqlite:///absolute/path/to/db?mode=ro'

Note the **three** slashes after the connection type.

Or using an in-memory database:

.. code-block:: bash

   export AIRFLOW_CONN_SQLITE_DEFAULT='sqlite://?mode=ro'

When specifying the connection as an environment variable in Airflow versions prior to 2.3.0, you need to specify the connection using the URI format.

Note that all components of the URI should be URL-encoded.

JSON format example
^^^^^^^^^^^^^^^^^^^

If serializing with JSON:

.. code-block:: bash

    export AIRFLOW_CONN_SQLITE_DEFAULT='{
        "conn_type": "sqlite",
        "host": "relative/path/to/db",
        "extra": {
            "mode": "ro"
        }
    }'
