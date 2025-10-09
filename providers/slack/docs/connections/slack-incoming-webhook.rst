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


.. _howto/connection:slack-incoming-webhook:

Slack Incoming Webhook Connection
=================================

The Slack Incoming Webhook connection type enables
`Slack Incoming Webhooks <https://api.slack.com/messaging/webhooks>`_ Integrations.

Authenticating to Slack
-----------------------

Authenticate to Slack using a `Incoming Webhook URL
<https://api.slack.com/messaging/webhooks#getting_started>`_.

Default Connection IDs
----------------------

.. warning::

  The :class:`airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook` and community provided operators
  not intend to use any Slack Incoming Webhook Connection by default right now.
  It might change in the future to ``slack_default``.

Configuring the Connection
--------------------------

Schema
    Optional. Http schema, if not specified than **https** is used.

Slack Webhook Endpoint (Host)
    Optional. Reference to slack webhook endpoint, if not specified than **hooks.slack.com/services** is used.
    In case if endpoint contain schema, than value from field ``Schema`` ignores.

Webhook Token (Password)
    Specify the Slack Incoming Webhook URL. It might specified as full url like
    **https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX** in this case values
    from ``Slack Webhook Endpoint (Host)`` and ``Schema`` fields ignores.
    Or it might specified as URL path like **T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX** in this case
    Slack Incoming Webhook URL will build from this field, ``Schema`` and ``Slack Webhook Endpoint (Host)``.

Extra
    Specify the extra parameters (as json dictionary) that can be used in
    `slack_sdk.WebhookClient <https://slack.dev/python-slack-sdk/webhook/index.html>`_.
    All parameters are optional.

    * ``timeout``: The maximum number of seconds the client will wait to connect
      and receive a response from Slack Incoming Webhook.
    * ``proxy``: Proxy to make the Slack Incoming Webhook call.

If you are configuring the connection via a URI, ensure that all components of the URI are URL-encoded.

Examples
--------

**Snippet for create Connection as URI**:
  .. code-block:: python

    from airflow.models.connection import Connection

    conn = Connection(
        conn_id="slack_default",
        conn_type="slackwebhook",
        password="T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
        extra={
            # Specify extra parameters here
            "timeout": "42",
        },
    )

    # Generate Environment Variable Name
    env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
    print(f"{env_key}='{conn.get_uri()}'")

**Set Slack API Connection as Environment Variable (URI)**
  .. code-block:: bash

     export AIRFLOW_CONN_SLACK_DEFAULT='slackwebhook://:T00000000%2FB00000000%2FXXXXXXXXXXXXXXXXXXXXXXXX@/?timeout=42'
