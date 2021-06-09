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

Managing Connections
====================

Airflow needs to know how to connect to your environment. Information
such as hostname, port, login and passwords to other systems and services is
handled in the ``Admin->Connections`` section of the UI. The pipeline code you
will author will reference the 'conn_id' of the Connection objects.

.. image:: ../img/connections.png

Connections can be created and managed using either the UI or environment
variables.

See the :doc:`Connections Concepts </concepts/connections>` documentation for
more information.

.. _creating_connection_ui:

Creating a Connection with the UI
---------------------------------

Open the ``Admin->Connections`` section of the UI. Click the ``Create`` link
to create a new connection.

.. image:: ../img/connection_create.png

1. Fill in the ``Conn Id`` field with the desired connection ID. It is
   recommended that you use lower-case characters and separate words with
   underscores.
2. Choose the connection type with the ``Conn Type`` field.
3. Fill in the remaining fields. See
   :ref:`manage-connections-connection-types` for a description of the fields
   belonging to the different connection types.
4. Click the ``Save`` button to create the connection.

.. _editing_connection_ui:

Editing a Connection with the UI
--------------------------------

Open the ``Admin->Connections`` section of the UI. Click the pencil icon next
to the connection you wish to edit in the connection list.

.. image:: ../img/connection_edit.png

Modify the connection properties and click the ``Save`` button to save your
changes.

.. _connection/cli:

Creating a Connection from the CLI
----------------------------------

You may add a connection to the database from the CLI.

Obtain the URI for your connection (see :ref:`Generating a Connection URI <generating_connection_uri>`).

Then add connection like so:

.. code-block:: bash

    airflow connections add 'my_prod_db' \
        --conn-uri 'my-conn-type://login:password@host:port/schema?param1=val1&param2=val2'

Alternatively you may specify each parameter individually:

.. code-block:: bash

    airflow connections add 'my_prod_db' \
        --conn-type 'my-conn-type' \
        --conn-login 'login' \
        --conn-password 'password' \
        --conn-host 'host' \
        --conn-port 'port' \
        --conn-schema 'schema' \
        ...

.. _connection/export:

Exporting Connections from the CLI
----------------------------------

You may export connections from the database using the CLI. The supported formats are ``json``, ``yaml`` and ``env``.

You may mention the target file as the parameter:

.. code-block:: bash

    airflow connections export connections.json

Alternatively you may specify ``format`` parameter for overriding the format:

.. code-block:: bash

    airflow connections export /tmp/connections --format yaml

You may also specify ``-`` for STDOUT:

.. code-block:: bash

    airflow connections export -

The JSON format contains an object where the key contains the connection ID and the value contains the definition of the connection. In this format, the connection is defined as a JSON object. The following is a sample JSON file.

.. code-block:: json

    {
      "airflow_db": {
        "conn_type": "mysql",
        "host": "mysql",
        "login": "root",
        "password": "plainpassword",
        "schema": "airflow",
        "port": null,
        "extra": null
      },
      "druid_broker_default": {
        "conn_type": "druid",
        "host": "druid-broker",
        "login": null,
        "password": null,
        "schema": null,
        "port": 8082,
        "extra": "{\"endpoint\": \"druid/v2/sql\"}"
      }
    }

The YAML file structure is similar to that of a JSON. The key-value pair of connection ID and the definitions of one or more connections. In this format, the connection is defined as a YAML object. The following is a sample YAML file.

.. code-block:: yaml

    airflow_db:
      conn_type: mysql
      extra: null
      host: mysql
      login: root
      password: plainpassword
      port: null
      schema: airflow
    druid_broker_default:
      conn_type: druid
      extra: '{"endpoint": "druid/v2/sql"}'
      host: druid-broker
      login: null
      password: null
      port: 8082
      schema: null

You may also export connections in ``.env`` format. The key is the connection ID, and the value describes the connection using the URI. The following is a sample ENV file.

.. code-block:: text

    airflow_db=mysql://root:plainpassword@mysql/airflow
    druid_broker_default=druid://druid-broker:8082?endpoint=druid%2Fv2%2Fsql

.. _environment_variables_secrets_backend:

Storing a Connection in Environment Variables
---------------------------------------------

The environment variable naming convention is :envvar:`AIRFLOW_CONN_{CONN_ID}`, all uppercase.

So if your connection id is ``my_prod_db`` then the variable name should be ``AIRFLOW_CONN_MY_PROD_DB``.

.. note::

    Single underscores surround ``CONN``.  This is in contrast with the way ``airflow.cfg``
    parameters are stored, where double underscores surround the config section name.
    Connections set using Environment Variables would not appear in the Airflow UI but you will
    be able to use them in your DAG file.

The value of this environment variable must use airflow's URI format for connections.  See the section
:ref:`Generating a Connection URI <generating_connection_uri>` for more details.

Using .bashrc (or similar)
^^^^^^^^^^^^^^^^^^^^^^^^^^

If storing the environment variable in something like ``~/.bashrc``, add as follows:

.. code-block:: bash

    export AIRFLOW_CONN_MY_PROD_DATABASE='my-conn-type://login:password@host:port/schema?param1=val1&param2=val2'

Using docker .env
^^^^^^^^^^^^^^^^^

If using with a docker ``.env`` file, you may need to remove the single quotes.

.. code-block::

    AIRFLOW_CONN_MY_PROD_DATABASE=my-conn-type://login:password@host:port/schema?param1=val1&param2=val2

Connection URI format
---------------------

In general, Airflow's URI format is like so:

.. code-block::

    my-conn-type://my-login:my-password@my-host:5432/my-schema?param1=val1&param2=val2

The above URI would produce a ``Connection`` object equivalent to the following:

.. code-block:: python

    Connection(
        conn_id="",
        conn_type="my_conn_type",
        description=None,
        login="my-login",
        password="my-password",
        host="my-host",
        port=5432,
        schema="my-schema",
        extra=json.dumps(dict(param1="val1", param2="val2")),
    )


.. _generating_connection_uri:

Generating a connection URI
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To make connection URI generation easier, the :py:class:`~airflow.models.connection.Connection` class has a
convenience method :py:meth:`~airflow.models.connection.Connection.get_uri`.  It can be used like so:

.. code-block:: pycon

    >>> import json
    >>> from airflow.models.connection import Connection
    >>> c = Connection(
    ...     conn_id="some_conn",
    ...     conn_type="mysql",
    ...     description="connection description",
    ...     host="myhost.com",
    ...     login="myname",
    ...     password="mypassword",
    ...     extra=json.dumps(dict(this_param="some val", that_param="other val*")),
    ... )
    >>> print(f"AIRFLOW_CONN_{c.conn_id.upper()}='{c.get_uri()}'")
    AIRFLOW_CONN_SOME_CONN='mysql://myname:mypassword@myhost.com?this_param=some+val&that_param=other+val%2A'

Additionally, if you have created a connection, you can use ``airflow connections get`` command.

.. code-block:: console

    $ airflow connections get sqlite_default
    Id: 40
    Conn Id: sqlite_default
    Conn Type: sqlite
    Host: /tmp/sqlite_default.db
    Schema: null
    Login: null
    Password: null
    Port: null
    Is Encrypted: false
    Is Extra Encrypted: false
    Extra: {}
    URI: sqlite://%2Ftmp%2Fsqlite_default.db

.. _manage-connections-connection-types:

Encoding arbitrary JSON
^^^^^^^^^^^^^^^^^^^^^^^

Some JSON structures cannot be urlencoded without loss.  For such JSON, ``get_uri``
will store the entire string under the url query param ``__extra__``.

For example:

.. code-block:: pycon

    >>> extra_dict = {"my_val": ["list", "of", "values"], "extra": {"nested": {"json": "val"}}}
    >>> c = Connection(
    ...     conn_type="scheme",
    ...     host="host/location",
    ...     schema="schema",
    ...     login="user",
    ...     password="password",
    ...     port=1234,
    ...     extra=json.dumps(extra_dict),
    ... )
    >>> uri = c.get_uri()
    >>> uri
    'scheme://user:password@host%2Flocation:1234/schema?__extra__=%7B%22my_val%22%3A+%5B%22list%22%2C+%22of%22%2C+%22values%22%5D%2C+%22extra%22%3A+%7B%22nested%22%3A+%7B%22json%22%3A+%22val%22%7D%7D%7D'


And we can verify that it returns the same dictionary:

.. code-block:: pycon

    >>> new_c = Connection(uri=uri)
    >>> new_c.extra_dejson == extra_dict
    True


But for the most common case of storing only key-value pairs, plain url encoding is used.

You can verify a URI is parsed correctly like so:

.. code-block:: pycon

    >>> from airflow.models.connection import Connection

    >>> c = Connection(
    ...     uri="my-conn-type://my-login:my-password@my-host:5432/my-schema?param1=val1&param2=val2"
    ... )
    >>> print(c.login)
    my-login
    >>> print(c.password)
    my-password


Handling of special characters in connection params
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    Use the convenience method ``Connection.get_uri`` when generating a connection
    as described in section :ref:`Generating a Connection URI <generating_connection_uri>`.
    This section for informational purposes only.

Special handling is required for certain characters when building a URI manually.

For example if your password has a ``/``, this fails:

.. code-block:: pycon

    >>> c = Connection(
    ...     uri="my-conn-type://my-login:my-pa/ssword@my-host:5432/my-schema?param1=val1&param2=val2"
    ... )
    ValueError: invalid literal for int() with base 10: 'my-pa'

To fix this, you can encode with :func:`~urllib.parse.quote_plus`:

.. code-block:: pycon

    >>> c = Connection(
    ...     uri="my-conn-type://my-login:my-pa%2Fssword@my-host:5432/my-schema?param1=val1&param2=val2"
    ... )
    >>> print(c.password)
    my-pa/ssword

Securing Connections
--------------------

Airflow uses `Fernet <https://github.com/fernet/spec/>`__ to encrypt passwords in the connection
configurations stored the metastore database. It guarantees that without the encryption password, Connection
Passwords cannot be manipulated or read without the key. For information on configuring Fernet, look at :ref:`security/fernet`.

In addition to retrieving connections from environment variables or the metastore database, you can enable
an secrets backend to retrieve connections. For more details see :doc:`/security/secrets/secrets-backend/index`.


Test Connections
----------------

Airflow Web UI & API allows to test connections. The test connection feature can be used from
:ref:`create <creating_connection_ui>` or :ref:`edit <editing_connection_ui>` connection page, or through calling
:doc:`Connections REST API </stable-rest-api-ref/>`.

To test a connection Airflow calls out the ``test_connection`` method from the associated hook class and reports the
results of it. It may happen that the connection type does not have any associated hook or the hook doesn't have the
``test_connection`` method implementation, in either case the error message will throw the proper error message.

One important point to note is that the connections will be tested from the webserver only, so this feature is
subject to network egress rules setup for your webserver. Also, if webserver & worker machines have different libs or
provider packages installed then the test results might differ.

Last caveat is that this feature won't be available for the connections coming out of the secrets backends.


Custom connection types
-----------------------

Airflow allows the definition of custom connection types - including modifications of the add/edit form
for the connections. Custom connection types are defined in community maintained providers, but you can
can also add a custom provider that adds custom connection types. See :doc:`apache-airflow-providers:index`
for description on how to add custom providers.

The custom connection types are defined via Hooks delivered by the providers. The Hooks can implement
methods defined in the protocol class :class:`~airflow.hooks.base_hook.DiscoverableHook`. Note that your
custom Hook should not derive from this class, this class is a dummy example to document expectations
regarding about class fields and methods that your Hook might define. Another good example is
:py:class:`~airflow.providers.jdbc.hooks.jdbc.JdbcHook`.

By implementing those methods in your hooks and exposing them via ``hook-class-names`` array in
the provider meta-data you can customize Airflow by:

* Adding custom connection types
* Adding automated Hook creation from the connection type
* Adding custom form widget to display and edit custom "extra" parameters in your connection URL
* Hiding fields that are not used for your connection
* Adding placeholders showing examples of how fields should be formatted

You can read more about details how to add custom provider packages in the :doc:`apache-airflow-providers:index`
