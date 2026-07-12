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

API Key
    Store the Islo API key in the connection's Password field. The hook sends
    it directly to the compute API as a Bearer credential.

Compute API URL
    Store the Islo compute API base URL in Host. The default is
    ``https://ca.compute.islo.dev``.

Extra
    Optional JSON fields are ``request_timeout`` in seconds and
    ``max_retries`` for idempotent reads and deletion.
    ``max_response_bytes`` bounds each decompressed API response buffered by
    the scheduler and defaults to 4194304 bytes. Sandbox creation and command
    submission are never retried because a timed-out response may still have
    created the resource.

For example:

.. code-block:: json

    {
      "request_timeout": 30,
      "max_retries": 3,
      "max_response_bytes": 4194304
    }

Store this connection in an Airflow secrets backend in production. The API key
stays in the scheduler; it is never injected into task sandboxes.
