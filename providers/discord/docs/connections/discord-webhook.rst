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



.. _howto/connection:http:

Discord Webhook Connection
==========================

The Discord connection enables connections to Discord webhooks.
The Discord connection uses the HTTP connection to perform the webhook request.

Authenticating with HTTP
------------------------

Login and Password authentication can be used along with any authentication method using headers.
Headers can be given in json format in the Extras field.

Default Connection IDs
----------------------

The Discord webhook operators and hooks use ``discord_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify the login for the discord server you would like to connect too.

Password (optional)
    Specify the password of the user for the discord server you would like to connect too.

Host (optional)
    Specify the entire url or the base of the url for the discord server.

Port (optional)
    Specify a port number if applicable.

Schema (optional)
    Specify the service type etc: http/https.

Webhook Endpoint
    The endpoint that will be used to perform the HTTP request.

Extra (optional)
    Specify headers in json format.
