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

.. _howto/connection:vespa:

Vespa Connection
================

The `Vespa <https://vespa.ai/>`__ connection type enables access to Vespa clusters.

Default Connection IDs
----------------------

The Vespa hook uses the ``vespa_default`` connection ID by default.

Configuring the Connection
--------------------------

Endpoint
    Hostname or full URL of the Vespa endpoint. If a scheme is already included, it takes precedence over the
    ``protocol`` extra field.

Document Type
    Set in the standard Airflow ``schema`` field. This becomes the Vespa document type used by the hook and operator.

    .. note::

       The example DAG assumes the Vespa document type has a ``body`` field because the
       verification task queries on it.

Port (optional)
    Optional port appended to the endpoint when it is not already present in the host value.

Namespace (optional)
    Vespa document namespace. Defaults to ``default``.

Protocol (optional)
    Protocol used when the host does not already include one. Supported values are ``http`` and ``https``.
    Defaults to ``http``.

Vespa Cloud Secret Token (optional)
    Token-based authentication for Vespa Cloud.

Client Certificate Path (optional)
    File path to the client certificate used for mTLS authentication.

Client Key Path (optional)
    File path to the client key used for mTLS authentication.

Max Feed Queue Size (optional)
    Passed through to ``feed_async_iterable`` to control the Vespa feed queue size.

Max Feed Workers (optional)
    Passed through to ``feed_async_iterable`` to control the number of feed workers.

Max Feed Connections (optional)
    Passed through to ``feed_async_iterable`` to control the maximum number of concurrent feed connections.
