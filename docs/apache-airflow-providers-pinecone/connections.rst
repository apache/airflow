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

.. _howto/connection:pinecone:

Pinecone Connection
===================

The `Pinecone <https://www.pinecone.io/>`__ connection type enables access to Pinecone APIs.

Default Connection IDs
----------------------

Pinecone hook points to ``pinecone_default`` connection by default.

Configuring the Connection
--------------------------

Host (optional)
    Host URL to connect to a specific Pinecone index.

Pinecone Environment (optional)
    Specify your Pinecone environment for pod based indexes.

Pinecone API key (required)
    Specify your Pinecone API Key to connect.

Project ID (optional)
    Project ID corresponding to your API Key.

Pinecone Region (optional)
    Specify the region for Serverless Indexes in Pinecone.

PINECONE_DEBUG_CURL (optional)
    Set to ``true`` to enable curl debug output.
