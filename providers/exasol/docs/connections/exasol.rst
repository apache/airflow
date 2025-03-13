.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements. See the NOTICE file
   distributed with this work for additional details regarding 
   copyright ownership. The ASF licenses this file under the 
   Apache License, Version 2.0 (the "License"); you may not use 
   this file except in compliance with the License. 

   You may obtain a copy of the License at:

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, 
   software distributed under the License is provided on an 
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
   either express or implied. See the License for the specific 
   language governing permissions and limitations under the License.

.. _howto/connection:exasol:

Exasol Connection
=================

The Exasol connection type allows Apache Airflow to connect to an Exasol database 
for querying and data extraction.

.. note::
   This connection type is designed for use with ``ExasolHook``.

Authenticating to Exasol
------------------------

Airflow supports authentication to Exasol via the standard connection mechanism.

Default Connection IDs
----------------------

The default connection ID is ``exasol_default``.

Configuring the Connection
--------------------------

Host
  The hostname or IP address of the Exasol database.

Port (optional)
  The port to connect to Exasol (default: ``8563``).

Schema (optional)
  The Exasol schema to be used.

Login (optional)
  The username for authentication.

Password (optional)
  The password for authentication.

Extra
  Specify additional parameters (as JSON dictionary) to be used in the Exasol connection.

  * ``encryption``: Whether to use encryption (default: true).
  * ``compression``: Whether to enable compression (default: true).

For a full setup guide, see: :ref:`howto/connection:exasol_ce_setup`