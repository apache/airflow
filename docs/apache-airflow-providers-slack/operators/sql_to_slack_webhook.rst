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

.. _howto/operator:SqlToSlackWebhookOperator:

SqlToSlackWebhookOperator
=========================

Use the :class:`~airflow.providers.slack.transfers.sql_to_slack_webhook.SqlToSlackWebhookOperator` to post query result
as message to predefined Slack channel through `Incoming Webhook <https://api.slack.com/messaging/webhooks>`__.

Using the Operator
^^^^^^^^^^^^^^^^^^

This operator will execute a custom query in the provided SQL connection and publish a Slack message that can be formatted
and contain the resulting dataset (e.g. ASCII formatted dataframe).

An example usage of the SqlToSlackWebhookOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/slack/example_sql_to_slack_webhook.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_to_slack_webhook]
    :end-before: [END howto_operator_sql_to_slack_webhook]
