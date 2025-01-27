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

.. _howto/connection:qdrant:

Qdrant Connection
===================

The `Qdrant <https://qdrant.tech/>`__ connection type enables access to Qdrant clusters.

Default Connection IDs
----------------------

The Qdrant hook use the ``qdrant_default`` connection ID by default.

Configuring the Connection
--------------------------

Host (optional)
    Host of the Qdrant instance to connect to.

API key (optional)
    Qdrant API Key for authentication.

Port (optional)
    REST port of the Qdrant instance to connect to. Defaults to ``6333``.

URL (optional)
    URL of the Qdrant instance to connect to. If specified, it overrides the ``host`` and ``port`` parameters.

GRPC Port (optional)
    GRPC port of the Qdrant instance to connect to. Defaults to ``6334``.

Prefer GRPC (optional)
    Whether to use GRPC for custom methods. Defaults to ``False``.

HTTPS (optional)
    Whether to use HTTPS for requests. Defaults to ``True`` if an API key is provided. ``False`` otherwise.

Prefix (optional)
    Prefix to add to the REST URL endpoints. Defaults to ``None``.
