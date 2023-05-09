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

A secrets backend is a subclass of :py:class:`airflow.secrets.BaseSecretsBackend` and must implement either
:py:meth:`~airflow.secrets.BaseSecretsBackend.get_connection` or :py:meth:`~airflow.secrets.BaseSecretsBackend.get_conn_value`.

After writing your backend class, provide the fully qualified class name in the ``backend`` key in the ``[secrets]``
section of ``airflow.cfg``.

Additional arguments to your SecretsBackend can be configured in ``airflow.cfg`` by supplying a JSON string to ``backend_kwargs``, which will be passed to the ``__init__`` of your SecretsBackend.
See :ref:`Configuration <secrets_backend_configuration>` for more details, and :ref:`SSM Parameter Store <ssm_parameter_store_secrets>` for an example.


Adapt to non-Airflow compatible secret formats for connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default implementation of Secret backend requires use of an Airflow-specific format of storing
secrets for connections. Currently most community provided implementations require the connections to
be stored as JSON or the Airflow Connection URI format (see
:doc:`apache-airflow-providers:core-extensions/secrets-backends`). However, some organizations may need to store the credentials (passwords/tokens etc) in some other way. For example, if the same credentials store needs to be used for multiple data platforms, or if you are using a service with a built-in mechanism of rotating the credentials that does not work with the Airflow-specific format.
In this case you will need to roll your own secret backend as described in the previous chapter,
possibly extending an existing secrets backend and adapting it to the scheme used by your organization.
