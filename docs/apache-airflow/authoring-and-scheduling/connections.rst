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

Connections & Hooks
===================

Airflow is often used to pull and push data into other systems, and so it has a first-class *Connection* concept for storing credentials that are used to talk to external systems.

A Connection is essentially set of parameters - such as username, password and hostname - along with the type of system that it connects to, and a unique name, called the ``conn_id``.

They can be managed via the UI or via the CLI; see :doc:`/howto/connection` for more information on creating, editing and managing connections. There are customizable connection storage and backend options.

You can use Connections directly from your own code, you can use them via Hooks or use them from :ref:`templates <concepts:jinja-templating>`::


    echo {{ conn.<conn_id>.host }}

.. _concepts_hooks:

Hooks
-----

A Hook is a high-level interface to an external platform that lets you quickly and easily talk to them without having to write low-level code that hits their API or uses special libraries. They're also often the building blocks that Operators are built out of.

They integrate with Connections to gather credentials, and many have a default ``conn_id``; for example, the :class:`~airflow.providers.postgres.hooks.postgres.PostgresHook` automatically looks for the Connection with a ``conn_id`` of ``postgres_default`` if you don't pass one in.

You can view a :ref:`full list of airflow hooks <pythonapi:hooks>` in our API documentation.

Custom connections
------------------

Airflow allows to define custom connection types. This is what is described in detail in
:doc:`apache-airflow-providers:index` - providers give you the capability of defining your own connections.
The connection customization can be done by any provider, but also
many of the providers managed by the community define custom connection types.
The full list of all providers delivered by ``Apache Airflow community managed providers`` can be found in
:doc:`apache-airflow-providers:core-extensions/connections`.
