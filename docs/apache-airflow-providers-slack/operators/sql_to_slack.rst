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

.. _howto/operator:SqlToSlackApiFileOperator:

SqlToSlackApiFileOperator
=========================

Use the :class:`~airflow.providers.slack.transfers.sql_to_slack.SqlToSlackApiFileOperator` to post query result as a file
to Slack channel(s) through `Slack API <https://api.slack.com/>`__.

Using the Operator
^^^^^^^^^^^^^^^^^^

.. note::
    Operator supports two methods for upload files, which controlled by ``slack_method_version``,
    by default it use Slack SDK method ``upload_files`` however this might impact a performance and cause random API errors.
    It is recommended to switch to Slack SDK method ``upload_files_v2`` by set ``v2`` to ``slack_method_version``,
    however this action required to add additional scope to your application:

    * **files:write** - for write files.
    * **files:read** - for read files (not required if you use Slack SDK >= 3.23.0).
    * **channels:read** - get list of public channels, for convert Channel Name to Channel ID.
    * **groups:read** - get list of private channels, for convert Channel Name to Channel ID
    * **mpim:read** - additional permission for API method **conversations.list**
    * **im:read** - additional permission for API method **conversations.list**

    .. seealso::
        - `Slack SDK 3.19.0 Release Notes <https://github.com/slackapi/python-slack-sdk/releases/tag/v3.19.0>`_
        - `conversations.list API <https://api.slack.com/methods/conversations.list>`_


This operator will execute a custom query in the provided SQL connection and publish a file to Slack channel(s).

An example usage of the SqlToSlackApiFileOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/slack/example_sql_to_slack.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_to_slack_api_file]
    :end-before: [END howto_operator_sql_to_slack_api_file]
