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

.. _howto/connection:google_ads:

Google Ads Connection
=====================

The **Google Ads** connection type enables integrations with the Google Ads API.

Default Connection IDs
----------------------

Hooks related to Google Ads use ``google_ads_default`` by default.

Configuring the Connection
--------------------------

Developer token
    Your Google Ads *developer token*.

    See the official guide on how to obtain a developer token:

    https://developers.google.com/google-ads/api/docs/first-call/dev-token

OAuth2 client ID
    The *client ID* from your Google Cloud OAuth 2.0 credentials.

OAuth2 client secret
    The *client secret* paired with the client ID above.

OAuth2 refresh token
    The *refresh token* generated for the client ID / secret pair.

Authentication method
    Either ``service_account`` (default) or ``developer_token``.

    Use ``service_account`` when authenticating with a Google service‑account key file

    specified via ``GOOGLE_APPLICATION_CREDENTIALS`` or a GCP connection.

Extra (optional, JSON)
    Additional client configuration, provided as a JSON dictionary.
    Common keys include:

    ``login_customer_id``
        Customer ID to impersonate when using a manager account.

    ``linked_customer_id``
        Linked customer ID for certain account‑specific requests.
