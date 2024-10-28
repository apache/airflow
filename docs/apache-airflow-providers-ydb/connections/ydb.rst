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



.. _howto/connection:ydb:

YDB Connection
======================
The YDB connection type provides connection to a YDB database.

Configuring the Connection
--------------------------
Host (required)
    The host without port to connect to. Acceptable schemes: ``grpc/grpcs``, e.g. ``grpc://my_host``, ``ydb.serverless.yandexcloud.net`` or ``lb.etn9txxxx.ydb.mdb.yandexcloud.net``.

Database (required)
    Specify the database to connect to, e.g. ``/local`` or ``/ru-central1/b1gtl2kg13him37quoo6/etndqstq7ne4v68n6c9b``.

Port (optional)
    The port or the YDB cluster to connect to. Default is 2135.

Login (optional)
    Specify the user name to connect.

Password (optional)
    Specify the password to connect.

Service account auth JSON (optional)
    Service account auth JSON, e.g. {"id": "...", "service_account_id": "...", "private_key": "..."}.

Service account auth JSON file path (optional)
    Service account auth JSON file path. File content looks like: {"id": "...", "service_account_id": "...", "private_key": "..."}.

Access Token (optional)
    User account IAM token.

Use VM metadata (optional)
    Whether to use VM metadata to retrieve access token

    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded). The connection could be specified as JSON string as well.

    For example:

    .. code-block:: bash

        AIRFLOW_CONN_YDB_DEFAULT1='ydb://grpcs://my_name:my_password@example.com:2135/?database=%2Flocal'
        AIRFLOW_CONN_YDB_DEFAULT2='{"conn_type": "ydb", "host": "grpcs://example.com", "login": "my_name", "password": "my_password", "port": 2135, "extra": {"database": "/local"}}'
