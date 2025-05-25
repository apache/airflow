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

.. _howto/connection:leveldb:

LevelDB Connection
==================

The **LevelDB** connection type enables tasks to interact with a local (or mounted) `LevelDB
<https://github.com/google/leveldb>`_ database by means of
:class:`airflow.providers.google.leveldb.hooks.leveldb.LevelDBHook`
(which is a thin wrapper around the `Plyvel <https://plyvel.readthedocs.io/>`_ client).

Default Connection IDs
----------------------

Hooks related to LevelDB use ``leveldb_default`` by default.

Configuring the Connection
--------------------------

LevelDB runs as an *embedded* key‑value store—there is no server to connect to.
Consequently, the Airflow connection only needs enough information to locate (and,
optionally, create) the on‑disk database.

Host
    **Path** to the database directory (for example ``/tmp/testdb`` or
    ``/opt/airflow/data/leveldb``).
    If the directory does not yet exist you may allow the hook to create it via the
    ``create_if_missing`` extra parameter (see below).

Login / Password / Port / Schema
    Not used by :class:`LevelDBHook`.  Leave these fields blank.

Extra (optional, JSON)
    Additional options accepted by :py:meth:`plyvel.DB`:

    ``create_if_missing`` *(bool)*
        When *true*, a new database directory will be created if it is absent.
        Defaults to *false* (raises an error if the path does not exist).

    ``error_if_exists`` *(bool)*
        Raise an exception if the database already exists (defaults to *false*).

    Any other keyword argument supported by ``plyvel.DB`` will be forwarded
    unchanged.

Example *Extra* field value::

    {
        "create_if_missing": true,
        "error_if_exists": false
    }
