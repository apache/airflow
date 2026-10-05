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

Secrets Backend
---------------

.. versionadded:: 1.10.10

In addition to retrieving connections & variables from environment variables or the metastore database, you
can also enable alternative secrets backend to retrieve Airflow connections or Airflow variables via
:ref:`Apache Airflow Community provided backends <community_secret_backends>` in
:doc:`apache-airflow-providers:core-extensions/secrets-backends`.

.. note::

    The Airflow UI only shows connections and variables stored in the Metadata DB and not via any other method.
    If you use an alternative secrets backend, check inside your backend to view the values of your variables and connections.

You can also get Airflow configurations with sensitive data from the Secrets Backend.
See :doc:`/howto/set-config` for more details.

Search path
^^^^^^^^^^^
When looking up a connection/variable, by default Airflow will search environment variables first and metastore
database second.

If you enable an alternative secrets backend, it will be searched first, followed by environment variables,
then metastore.  This search ordering is not configurable. Though, in some alternative secrets backend you might have
the option to filter which connection/variable/config is searched in the secret backend. Please look at the
documentation of the secret backend you are using to see if such option is available.

On the other hand, if a workers secrets backend is defined, the order of lookup has higher priority for the workers secrets
backend and then the secrets backend.

.. warning::

    When using environment variables or an alternative secrets backend to store secrets or variables, it is possible to create key collisions.
    In the event of a duplicated key between backends, all write operations will update the value in the metastore, but all read operations will
    return the first match for the requested key starting with the custom backend, then the environment variables and finally the metastore.

.. _secrets_backend_configuration:

Configuration
^^^^^^^^^^^^^

The ``[secrets]`` section has the following options:

.. code-block:: ini

    [secrets]
    backend =
    backend_kwargs =

Set ``backend`` to the fully qualified class name of the backend you want to enable.

You can provide ``backend_kwargs`` with json and it will be passed as kwargs to the ``__init__`` method of
your secrets backend.

If you want to check which secret backend is currently set, you can use ``airflow config get-value secrets backend`` command as in
the example below.

.. code-block:: bash

    $ airflow config get-value secrets backend
    airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend

Setting individual backend kwargs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Instead of encoding all kwargs as a JSON blob, you can set each one as a separate environment
variable using the ``AIRFLOW__SECRETS__BACKEND_KWARG__<KEY>`` prefix:

.. code-block:: bash

    # These two are equivalent:
    export AIRFLOW__SECRETS__BACKEND_KWARGS='{"role_id": "abc", "secret_id": "xyz"}'

    # or individually (useful for K8s Secrets):
    export AIRFLOW__SECRETS__BACKEND_KWARG__ROLE_ID=abc
    export AIRFLOW__SECRETS__BACKEND_KWARG__SECRET_ID=xyz

Per-key variables override the same key from ``BACKEND_KWARGS``. Values are raw strings
(not JSON-parsed). For workers, use the
``AIRFLOW__WORKERS__SECRETS_BACKEND_KWARG__<KEY>`` prefix.

.. note::
   These environment variables are masked in logs at startup, the same way
   ``BACKEND_KWARGS`` is masked.

Worker Specific Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The above section covers a general configuration option for all Airflow components. But with Airflow 3, if you want to
configure separate secrets backend for workers, you can do that using:

.. code-block:: ini

    [workers]
    secrets_backend =
    secrets_backend_kwargs =


Set ``secrets_backend`` to the fully qualified class name of the backend you want to enable.

You can provide ``secrets_backend_kwargs`` with json and it will be passed as kwargs to the ``__init__`` method of
your secrets backend for the workers.

If you want to check which secret backend is currently set, you can use ``airflow config get-value workers secrets_backend`` command as in
the example below.

.. code-block:: bash

    $ airflow config get-value workers secrets_backend
    airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend

Supported core backends
^^^^^^^^^^^^^^^^^^^^^^^

.. toctree::
    :maxdepth: 1
    :glob:

    *

.. _community_secret_backends:

Apache Airflow Community provided secret backends
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Apache Airflow Community also releases community developed providers (:doc:`apache-airflow-providers:index`)
and some of them also provide handlers that extend secret backends
capability of Apache Airflow. You can see all those providers in
:doc:`apache-airflow-providers:core-extensions/secrets-backends`.


.. _roll_your_own_secrets_backend:

Roll your own secrets backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A secrets backend is a subclass of :py:class:`airflow.secrets.base_secrets.BaseSecretsBackend` and must implement either
:py:meth:`~airflow.secrets.base_secrets.BaseSecretsBackend.get_connection` or :py:meth:`~airflow.secrets.base_secrets.BaseSecretsBackend.get_conn_value` for retrieving connections, :py:meth:`~airflow.secrets.base_secrets.BaseSecretsBackend.get_variable` for retrieving variables and :py:meth:`~airflow.secrets.base_secrets.BaseSecretsBackend.get_config` for retrieving Airflow configurations.

After writing your backend class, provide the fully qualified class name in the ``backend`` key in the ``[secrets]``
section of ``airflow.cfg``.

Additional arguments to your SecretsBackend can be configured in ``airflow.cfg`` by supplying a JSON string to ``backend_kwargs``, which will be passed to the ``__init__`` of your SecretsBackend.
See :ref:`Configuration <secrets_backend_configuration>` for more details, and :ref:`SSM Parameter Store <ssm_parameter_store_secrets>` for an example.

Async server-side reads
^^^^^^^^^^^^^^^^^^^^^^^

When the Execution API serves a Variable or Connection, it resolves the value with
``airflow.secrets.resolver.resolve_variable`` or ``airflow.secrets.resolver.resolve_connection``.
Both functions search the configured backends in the same order as synchronous reads. There is no
separate async configuration.

A backend can implement either or both of these optional methods:

.. code-block:: python

    async def aget_variable(self, key: str, team_name: str | None = None) -> str | None: ...


    async def aget_connection(self, conn_id: str, team_name: str | None = None) -> Connection | None: ...

They take the same arguments and return the same values as ``get_variable`` and ``get_connection``.
``aget_connection`` returns the core :py:class:`airflow.models.connection.Connection` or ``None``.

The resolver calls these methods as follows:

* **Precedence:** the resolver uses the async method when it is defined in the same class as its
  synchronous counterpart, or in a subclass of that class. If a subclass overrides only the
  synchronous method, the resolver uses that override. Existing custom backends therefore keep their
  behavior when a parent class gains an async method.
* **Fallback:** a backend without an async method needs no change. The resolver runs its synchronous
  method in a worker thread.
* **Errors:** the resolver doesn't retry a failing async method synchronously. It continues with the
  next backend, as it does for synchronous methods. ``AirflowSecretsBackendAccessDenied`` ends the
  search.
* **Event loop:** async methods run on the event loop that serves the request. Create, use, and close
  async clients on that loop. Don't create a loop-bound client in ``__init__``, and don't share it
  with the synchronous method.
* **Database session:** the metastore backend's async methods use the request's async database
  session when the Execution API supplies one. Otherwise, they open and close their own. The resolver
  never passes that session to another backend, and it doesn't commit or close it.
* **Cancellation:** a cancelled request stops the backend search and the cache update. The resolver
  can't interrupt a synchronous method that is already running in a worker thread. That method
  finishes, and the resolver discards its result.

The resolver runs these steps in worker threads because they can block:

* Loading the backend list, which can import and construct a configured backend.
* Calling synchronous backend methods.
* Reading or writing an initialized secrets cache (``[secrets] use_cache``), which a separate manager
  process holds.
* Initializing the Fernet key, which can run ``fernet_key_cmd`` or read ``fernet_key_secret``.
  Successful initialization is cached; failed loads can be retried.

Masking, connection parsing, and response validation run on the event loop. Code in an async method
must not block the event loop.

Provider backends don't implement these methods, so they use the worker-thread fallback. Task
processes read secrets through the Execution API.


Adapt to non-Airflow compatible secret formats for connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default implementation of Secret backend requires use of an Airflow-specific format of storing
secrets for connections. Currently most community provided implementations require the connections to
be stored as JSON or the Airflow Connection URI format (see
:doc:`apache-airflow-providers:core-extensions/secrets-backends`). However, some organizations may need to store the credentials (passwords/tokens etc) in some other way. For example, if the same credentials store needs to be used for multiple data platforms, or if you are using a service with a built-in mechanism of rotating the credentials that does not work with the Airflow-specific format.
In this case you will need to roll your own secret backend as described in the previous chapter,
possibly extending an existing secrets backend and adapting it to the scheme used by your organization.
