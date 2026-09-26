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

.. _howto/connection:islo:

Islo Connection
===============

The ``islo`` connection type holds the API key and endpoints for
`islo.dev <https://islo.dev>`__, the hosted microVM sandbox service behind
:class:`~airflow.providers.common.ai.sandbox.IsloSandboxBackend`. It backs
:class:`~airflow.providers.common.ai.hooks.islo.IsloHook` (see
:doc:`../hooks/islo` for hook usage and installation instructions).

Default Connection IDs
----------------------

``IsloHook`` and ``IsloSandboxBackend`` use ``islo_default`` by default.

Configuring the Connection
---------------------------

API Key (Password field)
    An Islo API key, created with ``islo api-key create`` or in the Islo
    dashboard. Required.

Compute URL (Host field)
    Optional. The regional compute API the microVMs run on, passed to the SDK
    as ``compute_url=``. The SDK default is ``https://ca.compute.islo.dev``.

API URL (Extra field)
    Optional. The control-plane URL, passed as ``base_url=``. The SDK default
    is ``https://api.islo.dev``. Stored in ``extra["base_url"]``.

Request Timeout (Extra field)
    Optional. Default HTTP timeout in seconds for the SDK client the hook
    returns. Stored in ``extra["timeout"]``. It only affects direct use of the
    client: ``IsloSandboxBackend`` sets a timeout on every call itself, bounded by
    the command deadline, and 120 seconds for file transfers.

The ``schema``, ``port`` and ``login`` fields are hidden in the connection
form; they are not used by this connection type.

Environment variables instead of a connection
---------------------------------------------

Pass ``islo_conn_id=None`` to ``IsloSandboxBackend`` to let the SDK read
``ISLO_API_KEY``, ``ISLO_BASE_URL`` and ``ISLO_COMPUTE_URL`` from the worker
environment instead. That is convenient for a local trial, but it keeps the key
outside your secrets backend, so prefer a connection in a deployment.

Examples
--------

**API key only**

.. code-block:: json

    {
        "conn_type": "islo",
        "password": "<api-key>"
    }

**Regional compute API and a longer request timeout**

.. code-block:: json

    {
        "conn_type": "islo",
        "password": "<api-key>",
        "host": "https://<region>.compute.islo.dev",
        "extra": "{\"timeout\": 60}"
    }
