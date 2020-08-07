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


Secrets backend
---------------

.. versionadded:: 1.10.10

In addition to retrieving connections & variables from environment variables or the metastore database, you can enable
an alternative secrets backend to retrieve Airflow connections or Airflow variables,
such as :ref:`Google Cloud Secret Manager<google_cloud_secret_manager_backend>`,
:ref:`Hashicorp Vault Secrets<hashicorp_vault_secrets>` or you can :ref:`roll your own <roll_your_own_secrets_backend>`.

.. note::

    The Airflow UI only shows connections and variables stored in the Metadata DB and not via any other method.
    If you use an alternative secrets backend, check inside your backend to view the values of your variables and connections.

Search path
^^^^^^^^^^^
When looking up a connection/variable, by default Airflow will search environment variables first and metastore
database second.

If you enable an alternative secrets backend, it will be searched first, followed by environment variables,
then metastore.  This search ordering is not configurable.

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

Supported backends
^^^^^^^^^^^^^^^^^^

.. toctree::
    :maxdepth: 1
    :glob:

    *

.. _roll_your_own_secrets_backend:

Roll your own secrets backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A secrets backend is a subclass of :py:class:`airflow.secrets.BaseSecretsBackend` and must implement either
:py:meth:`~airflow.secrets.BaseSecretsBackend.get_connections` or :py:meth:`~airflow.secrets.BaseSecretsBackend.get_conn_uri`.

After writing your backend class, provide the fully qualified class name in the ``backend`` key in the ``[secrets]``
section of ``airflow.cfg``.

Additional arguments to your SecretsBackend can be configured in ``airflow.cfg`` by supplying a JSON string to ``backend_kwargs``, which will be passed to the ``__init__`` of your SecretsBackend.
See :ref:`Configuration <secrets_backend_configuration>` for more details, and :ref:`SSM Parameter Store <ssm_parameter_store_secrets>` for an example.

.. note::

    If you are rolling your own secrets backend, you don't strictly need to use airflow's URI format. But
    doing so makes it easier to switch between environment variables, the metastore, and your secrets backend.
