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



.. _howto/connection:openfaas:

OpenFaaS Connection
===================

The OpenFaaS connection type provides connection to an OpenFaaS gateway.

Configuring the Connection
--------------------------

Host (required)
    The OpenFaaS gateway URL (e.g., ``http://gateway.openfaas:8080``).

Login (optional)
    Username for basic authentication if your OpenFaaS gateway requires authentication.

Password (optional)
    Password for basic authentication if your OpenFaaS gateway requires authentication.

Extra (optional)
    Specify the extra parameters (as JSON dictionary) that can be used in the OpenFaaS
    connection.
