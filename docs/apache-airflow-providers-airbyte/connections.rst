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



Airbyte Connection
==================
The Airbyte connection type use the Airbyte API Python SDK to authenticate to the server.

Host(required)
    The host to connect to the Airbyte server.
    If you are using Airbyte Cloud: `api.airbyte.com`.

Scheme (optional)
    The HTTP scheme of your server.
    Default value is `https`.

Port (optional)
    The port for the Airbyte server.
    If you are using Airbyte Cloud you don't need to provide the port.

Client ID (required)
    The Client ID to connect to the Airbyte server.
    You can find this information in the Settings / Applications page in Airbyte UI.

Client Secret (required)
    The Client Secret to connect to the Airbyte server.
    You can find this information in the Settings / Applications page in Airbyte UI.
