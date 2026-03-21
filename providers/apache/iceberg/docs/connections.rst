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

.. _howto/connection:iceberg:

Connecting to Iceberg
=====================

The Iceberg connection type connects to an Iceberg REST catalog using ``pyiceberg``.
The hook provides catalog introspection (list namespaces, list tables, read schemas,
inspect partitions and snapshots) and OAuth2 token generation for external engines
like Spark, Trino, and Flink.

After installing the Iceberg provider in your Airflow environment, the corresponding
connection type of ``iceberg`` will be available.

Default Connection IDs
----------------------

Iceberg Hook uses the parameter ``iceberg_conn_id`` for Connection IDs and the value
of the parameter as ``iceberg_default`` by default. You can create multiple connections
in case you want to switch between environments.

Configuring the Connection
--------------------------

Catalog URI (Host)
    The URL of the Iceberg REST catalog endpoint.
    Example: ``https://your-catalog.example.com/ws/v1``

Client ID (Login)
    The OAuth2 Client ID for authenticating with the catalog.
    Leave empty for catalogs that don't require OAuth2 credentials (e.g., local catalogs).

Client Secret (Password)
    The OAuth2 Client Secret for authenticating with the catalog.

Extra (Optional)
    A JSON object with additional catalog properties passed to ``pyiceberg.catalog.load_catalog()``.
    Common properties:

    .. code-block:: json

        {
            "warehouse": "s3://my-warehouse/",
            "s3.endpoint": "https://s3.us-east-1.amazonaws.com",
            "s3.region": "us-east-1",
            "s3.access-key-id": "AKIA...",
            "s3.secret-access-key": "..."
        }

    For AWS/GCP/Azure deployments, prefer using IAM roles or environment-based
    credentials and pass only the ``warehouse`` path in extra.

Migration from 1.x
-------------------

In version 2.0.0, ``get_conn()`` now returns a ``pyiceberg.catalog.Catalog`` instance
instead of a token string. If you were using ``get_conn()`` to obtain OAuth2 tokens,
switch to ``get_token()``:

.. code-block:: python

    # Before (1.x)
    token = IcebergHook().get_conn()

    # After (2.0)
    token = IcebergHook().get_token()

The ``get_token_macro()`` method has been updated to use ``get_token()`` automatically,
so Jinja2 templates continue to work without changes.
