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

The Islo hook uses the ``islo_default`` connection by default.

Access Key
    Store the Islo access key in the connection's Password field.

Control API URL
    Store a custom control-plane URL in Host. The default is
    ``https://api.islo.dev``.

Extra
    Optional JSON fields are ``compute_url`` (default
    ``https://ca.compute.islo.dev``), ``request_timeout`` in seconds, and
    ``max_retries`` for idempotent requests.

For example:

.. code-block:: json

    {
      "compute_url": "https://ca.compute.islo.dev",
      "request_timeout": 30,
      "max_retries": 3
    }

Store this connection in an Airflow secrets backend in production. The access
key stays in the scheduler; it is never injected into task sandboxes.
