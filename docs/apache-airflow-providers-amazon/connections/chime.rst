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

.. _howto/connection:chime:

Amazon Chime Connection
==========================

The Chime connection works with calling Chime webhooks to send messages to a chime room.

Authenticating to Amazon Chime
---------------------------------
When a webhook is created in a Chime room a token will be included in the url for authentication.


Default Connection IDs
----------------------

The default connection ID is ``chime_default``.

Configuring the Connection
--------------------------

Login (optional)
    Chime does not require a login for a webhook, this field can be left blank.

Password (optional)
    The token for authentication should be included in extras. No passwords are used for Chime webhooks.

Host (optional)
    Specify the entire url or the base of the url for the service.

Port (optional)
    Specify a port number if applicable.

Schema (optional)
    Specify the service type etc: http/https.

Extras (optional)
    Specify webhook_endpoint here which will start with ``incomingwebhooks/``
If you are configuring the connection via a URI, ensure that all components of the URI are URL-encoded.

Examples
--------

**Connection**
* **Login:
* **Password:
* **Host: hooks.chime.aws
* **Port:
* ** Schema: https

* **Extras**:

.. code-block:: json

{
"webhook_endpoint": "incomingwebhooks/abceasd-3423-a1237-ffff-000cccccccc?token=somechimetoken"
}
