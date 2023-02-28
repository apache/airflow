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

.. _howto/connection:impala:

Apache Impala Connection
========================

The Apache Impala connection type configures a connection to Apache Impala via the ``impyla`` Python package.

Default Connection IDs
----------------------

Impala hooks and operators use ``impala_default`` by default.

Configuring the Connection
--------------------------
Host (optional)
     The hostname for HS2. For Impala, this can be any of the ``impalad`` service.

Port (optional)
     The port number for HS2. The Impala default is ``21050``. The Hive port is
     likely different.

Login (optional)
    LDAP user, if applicable.

Password (optional)
    LDAP password, if applicable.

Schema (optional)
    The default database. If ``None``, the result is
    implementation-dependent.

Extra (optional)
     A JSON dictionary specifying the extra parameters that can be used in ``impyla`` connection.
