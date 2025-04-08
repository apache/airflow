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

`Amazon Chime <https://aws.amazon.com/chime/>`__ connection works with
`Chime Incoming Webhooks <https://docs.aws.amazon.com/chime/latest/ag/webhooks.html>`__ to send messages to a
`Chime Chat Room <https://docs.aws.amazon.com/chime/latest/ug/chime-chat-room.html>`__.

Authenticating to Amazon Chime
---------------------------------
When a webhook is created in a Chime room a token will be included in the url for authentication.


Default Connection IDs
----------------------

The default connection ID is ``chime_default``.

Configuring the Connection
--------------------------
Chime Webhook Endpoint
    Specify the entire url or the base of the url for the service.

Chime Webhook token
    The token for authentication including the webhook ID.

Schema
    Whether or not the endpoint should be http or https


Examples
--------

.. list-table:: Amazon Chime Connection Sample
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - **Chime Webhook Endpoint**
     - ``hooks.chime.aws``
   * - **Chime Webhook Token**
     - ``abceasd-3423-a1237-ffff-000cccccccc?token=somechimetoken``
   * - **Schema**
     - ``https``
