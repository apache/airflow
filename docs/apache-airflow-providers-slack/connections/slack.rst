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



.. _howto/connection:slack:

Slack API Connection
====================

The Slack connection type enables Slack API Integrations.

Authenticating to Slack
-----------------------

Authenticate to Slack using a `Slack API token
<https://slack.com/help/articles/215770388-Create-and-regenerate-API-tokens>`_.

Default Connection IDs
----------------------

.. warning::

  The SlackHook and community provided operators not intend to use any Slack API Connection by default right now.
  It might change in the future to ``slack_api_default``.

Configuring the Connection
--------------------------

Password
    Specify the Slack API token.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in slack_sdk.WebClient.
    All parameters are optional.

    * ``timeout``: The maximum number of seconds the client will wait to connect and receive a response from Slack API.
    * ``base_url``: A string representing the Slack API base URL.
    * ``proxy``: Proxy to make the Slack Incoming Webhook call.
    * ``retry_handlers``: Comma separated list of import paths to zero-argument callable which returns retry handler
      for Slack WebClient.

If you are configuring the connection via a URI, ensure that all components of the URI are URL-encoded.

Examples
--------

**Set Slack API Connection as Environment Variable (URI)**
  .. code-block:: bash

     export AIRFLOW_CONN_SLACK_API_DEFAULT='slack://:xoxb-1234567890123-09876543210987-AbCdEfGhIjKlMnOpQrStUvWx@/?timeout=42'

**Snippet for create Connection as URI**:
  .. code-block:: python

    from airflow.models.connection import Connection

    conn = Connection(
        conn_id="slack_api_default",
        conn_type="slack",
        password="xoxb-1234567890123-09876543210987-AbCdEfGhIjKlMnOpQrStUvWx",
        extra={
            # Specify extra parameters here
            "timeout": "42",
        },
    )

    # Generate Environment Variable Name
    env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"

    print(f"{env_key}='{conn.get_uri()}'")
    # AIRFLOW_CONN_SLACK_API_DEFAULT='slack://:xoxb-1234567890123-09876543210987-AbCdEfGhIjKlMnOpQrStUvWx@/?timeout=42'
