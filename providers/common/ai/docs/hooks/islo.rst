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

.. _howto/hook:islo:

``IsloHook``
============

Use :class:`~airflow.providers.common.ai.hooks.islo.IsloHook` to turn an
``islo`` connection into an authenticated `islo.dev <https://islo.dev>`__ SDK
client. :class:`~airflow.providers.common.ai.sandbox.IsloSandboxBackend`
resolves its credentials through this hook; call it directly when a task needs
the client for something the backend does not cover, such as listing or
reclaiming sandboxes.

.. code-block:: python

    from airflow.providers.common.ai.hooks.islo import IsloHook

    client = IsloHook(islo_conn_id="islo_default").get_conn()
    print(client.sandboxes.list_sandboxes(name_prefix="airflow-sandbox-", limit=20))

The hook owns its own ``islo`` connection type so the connection form is
labeled for the service it configures.

Connection Configuration
------------------------

The hook reads the Airflow connection of type ``islo``
(see :doc:`../connections/islo`):

- **password** -- the Islo API key. Required.
- **host** -- optional compute URL, passed as ``compute_url``.
- **extra** JSON -- ``{"base_url": "https://api.islo.dev", "timeout": 30}``,
  both optional.

``test_connection`` lists one sandbox with the configured credentials and
creates nothing.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - Parameter
     - Default
     - Description
   * - ``islo_conn_id``
     - ``islo_default``
     - Airflow connection ID of type ``islo``.

Dependencies
------------

Install the ``islo`` extra to use this hook::

    pip install "apache-airflow-providers-common-ai[islo]"
