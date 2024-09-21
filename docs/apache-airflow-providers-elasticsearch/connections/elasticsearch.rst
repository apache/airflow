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



.. _howto/connection:elasticsearch:

ElasticSearch Connection
========================

The ElasticSearch connection that enables ElasticSearch integrations.

Authenticating to ElasticSearch
-------------------------------

Authenticate with the `ElasticSearch DBAPI
<https://pypi.org/project/elasticsearch-dbapi/>`_

Default Connection IDs
----------------------

Some hooks and operators related to ElasticSearch use elasticsearch_default by default.

Configuring the Connection
--------------------------

User
    Specify the login used for the initial connection

Password
    Specify the Elasticsearch API key used for the `initial connection
    <https://www.elastic.co/guide/en/cloud/current/ec-api-authentication.html#ec-api-authentication>`_

Host
    Specify the Elasticsearch host used for the initial connection

Port
    Specify the Elasticsearch port for the initial connection

Scheme
    Specify the schema for the Elasticsearch API. `http` by default

Extra (Optional)
    Specify the extra parameters (as json dictionary) that can be used in Elasticsearch connection.
    The following parameters are all optional:

    * ``http_compress``: specify whether or not to use ``http_compress``. False by default.
    * ``timeout``: specify the time frame of the ``timeout``. False by default.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

    export AIRFLOW_CONN_ELASTICSEARCH_DEFAULT='elasticsearch://elasticsearchlogin:elasticsearchpassword@elastic.co:80/http'

.. exampleinclude:: /../../tests/system/providers/elasticsearch/example_elasticsearch_query.py
    :language: python
    :dedent: 4
    :start-after: [START howto_elasticsearch_query]
    :end-before: [END howto_elasticsearch_query]
