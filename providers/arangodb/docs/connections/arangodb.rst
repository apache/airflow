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

.. _howto/connection:arangodb:

ArangoDB Connection
====================
The ArangoDB connection provides credentials for accessing the ArangoDB.

Configuring the Connection
--------------------------
ArangoDB Host (required)
    Specify ArangoDB Host URL or comma separated list of URLs (coordinators in a cluster),
    e.g. ``http://127.0.0.1:8529`` or ``http://127.0.0.1:8529,http://127.0.0.1:8530``.
ArangoDB Database/Schema (required)
    Specify **Database/Schema** for the ArangoDB. eg. ``_system``.
ArangoDB Username (required)
    Specify **username** for the ArangoDB, e.g. ``root``.
ArangoDB Password (required)
    Specify **password** for the ArangoDB
