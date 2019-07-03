..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
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

.. image:: ../../img/connections.png

Connections can be created and managed using either the UI or environment
variables.

See the :ref:`Connections Concepts <concepts-connections>` documentation for
more information.

Creating a Connection with the UI
---------------------------------

Open the ``Admin->Connections`` section of the UI. Click the ``Create`` link
to create a new connection.

.. image:: ../../img/connection_create.png

1. Fill in the ``Conn Id`` field with the desired connection ID. It is
   recommended that you use lower-case characters and separate words with
   underscores.
2. Choose the connection type with the ``Conn Type`` field.
3. Fill in the remaining fields. See
   :ref:`manage-connections-connection-types` for a description of the fields
   belonging to the different connection types.
4. Click the ``Save`` button to create the connection.

Editing a Connection with the UI
--------------------------------

Open the ``Admin->Connections`` section of the UI. Click the pencil icon next
to the connection you wish to edit in the connection list.

.. image:: ../../img/connection_edit.png

Modify the connection properties and click the ``Save`` button to save your
changes.

Creating a Connection with Environment Variables
------------------------------------------------

Connections in Airflow pipelines can be created using environment variables.
The environment variable needs to have a prefix of ``AIRFLOW_CONN_`` for
Airflow with the value in a URI format to use the connection properly.

When referencing the connection in the Airflow pipeline, the ``conn_id``
should be the name of the variable without the prefix. For example, if the
``conn_id`` is named ``postgres_master`` the environment variable should be
named ``AIRFLOW_CONN_POSTGRES_MASTER`` (note that the environment variable
must be all uppercase).

Airflow assumes the value returned from the environment variable to be in a URI
format (e.g. ``postgres://user:password@localhost:5432/master`` or
``s3://accesskey:secretkey@S3``). The underscore character is not allowed
in the scheme part of URI, so it must be changed to a hyphen character
(e.g. `google-compute-platform` if `conn_type` is `google_compute_platform`).
Query parameters are parsed to one-dimensional dict and then used to fill extra.


.. _manage-connections-connection-types:

Connection Types
----------------

.. toctree::
    :maxdepth: 1
    :glob:

    *
