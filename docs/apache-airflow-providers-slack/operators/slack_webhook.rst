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

Slack Incoming Webhook Operators
================================


SlackWebhookOperator
--------------------

Use the :class:`~airflow.providers.slack.operators.slack_webhook.SlackWebhookOperator` to post messages
to predefined Slack channel through `Incoming Webhook <https://api.slack.com/messaging/webhooks>`__

Using the Operator
^^^^^^^^^^^^^^^^^^

You could send simple text message

.. exampleinclude:: /../../tests/system/providers/slack/example_slack_webhook.py
    :language: python
    :dedent: 4
    :start-after: [START slack_webhook_operator_text_howto_guide]
    :end-before: [END slack_webhook_operator_text_howto_guide]


Or you could use `Block Kit <https://api.slack.com/reference/block-kit>`_ for create app layouts

.. exampleinclude:: /../../tests/system/providers/slack/example_slack_webhook.py
    :language: python
    :dedent: 4
    :start-after: [START slack_webhook_operator_blocks_howto_guide]
    :end-before: [END slack_webhook_operator_blocks_howto_guide]
