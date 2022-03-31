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

Flyte Connection
================

The Flyte connection enables connecting to Flyte through FlyteRemote.

Configuring the Connection
--------------------------

Host(optional)
    The FlyteAdmin host. Defaults to localhost.

Port (optional)
    The FlyteAdmin port. Defaults to 30081.

Login (optional)
    ``client_id``

Password (optional)
    ``client_credentials_secret``

Extra (optional)
    Specify the ``extra`` parameter as JSON dictionary to provide additional parameters.
    * ``project``: The default project to connect to.
    * ``domain``: The default domain to connect to.
    * ``insecure``: Whether to use SSL or not.
    * ``command``: The command to execute to return a token using an external process.
    * ``scopes``: List of scopes to request.
    * ``auth_mode``: The OAuth mode to use. Defaults to pkce flow.
