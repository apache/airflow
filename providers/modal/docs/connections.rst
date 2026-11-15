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

.. _howto/connection:modal:

Modal Connection
================

The `Modal <https://modal.com/>`__ connection type holds a Modal API token and, optionally,
the Modal environment to work in. :class:`~airflow.providers.modal.hooks.modal.ModalHook`
turns it into a ``modal.Client`` that other Modal SDK calls accept through their ``client=``
argument.

Default Connection IDs
----------------------

The Modal hook points to the ``modal_default`` connection by default.

Configuring the Connection
--------------------------

Token ID (login)
    The Modal token id (``ak-...``). Create one in the Modal dashboard under *Settings > API
    Tokens*, or with ``modal token new``.

Token Secret (password)
    The Modal token secret (``as-...``) that pairs with the token id.

Extra (optional)
    A JSON dictionary. One key is recognized:

    * ``environment``: the Modal `environment <https://modal.com/docs/guide/environments>`__
      (for example ``main`` or ``dev``) that ``ModalHook.lookup_app`` scopes app lookups to.
      Sandboxes inherit the environment from their app. When unset, the Modal SDK resolves it
      from ``MODAL_ENVIRONMENT`` or the active profile in ``~/.modal.toml``.

    The Modal *workspace* is not a connection field: Modal derives it from the token.

Credential precedence
---------------------

The hook resolves credentials in this order:

1. Token id and secret on the connection. The client is built from exactly those, so a
   worker that also has ``MODAL_TOKEN_ID`` in its environment still uses the connection.
2. A connection with neither token field set, or no ``modal_default`` connection at all.
   The Modal SDK's own resolution applies: ``MODAL_TOKEN_ID`` / ``MODAL_TOKEN_SECRET`` from
   the environment, then the active profile in ``~/.modal.toml``. This keeps a worker that
   already ran ``modal token new`` working without any Airflow configuration.
3. Exactly one of the two token fields set. The hook raises: a half-filled connection is a
   misconfiguration, and falling back to ambient credentials would hide it.

A connection id other than ``modal_default`` that does not exist is always an error, since
the author named it on purpose.

Examples
--------

Environment variable, JSON form:

.. code-block:: bash

    export AIRFLOW_CONN_MODAL_DEFAULT='{"conn_type": "modal", "login": "ak-...", "password": "as-...", "extra": {"environment": "main"}}'

URI form:

.. code-block:: bash

    export AIRFLOW_CONN_MODAL_DEFAULT='modal://ak-...:as-...@?environment=main'

For invoking deployed functions and running sandboxes through the hook, see :doc:`usage`.
