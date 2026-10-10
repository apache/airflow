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

.. _howto/connection:search:

Elasticsearch / OpenSearch connection
=====================================

The ``search`` connection type points
:class:`~airflow.providers.search.hooks.search.SearchHook` at an Elasticsearch or OpenSearch
cluster. The hook asks the cluster which engine it is, so the same connection type serves both.

Default Connection ID
---------------------

``search_default``

Configuring the Connection
--------------------------

Host or URL
    A full URL such as ``https://search.example.com:9200``, or a host name. A host name is
    combined with the ``scheme`` option (default ``http``) and the port.

Port
    Used when the host has no port. Defaults to 9200 for a bare host name.

Login, Password
    HTTP basic auth.

Options (JSON)
    * ``api_key``: Elasticsearch API key, instead of login and password.
    * ``cloud_id``: Elastic Cloud deployment id, instead of host.
    * ``scheme``: ``http`` or ``https``, when the host is not a URL.
    * ``url_prefix``: path the cluster is served under, behind a proxy.
    * ``verify_certs`` (default ``true``), ``ca_certs``, ``client_cert``, ``client_key``: TLS settings.
    * ``request_timeout`` (default 10 seconds), ``max_retries`` (default 3), ``http_compress``.
    * ``headers``: extra headers sent with every request.
    * ``fetch_size`` (default 1000): rows per page for SQL queries.

Examples
--------

.. code-block:: bash

    export AIRFLOW_CONN_SEARCH_DEFAULT='{
        "conn_type": "search",
        "host": "https://search.example.com:9200",
        "login": "airflow",
        "password": "secret"
    }'
