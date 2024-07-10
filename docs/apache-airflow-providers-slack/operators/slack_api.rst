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

Slack API Operators
===================

Introduction
------------

`Slack API <https://api.slack.com/>`__ operators can post text messages or send files to specified Slack channel(s).

SlackAPIPostOperator
--------------------

Use the :class:`~airflow.providers.slack.operators.slack.SlackAPIPostOperator` to post messages to a Slack channel.


Using the Operator
^^^^^^^^^^^^^^^^^^

You could send simple text message

.. exampleinclude:: /../../tests/system/providers/slack/example_slack.py
    :language: python
    :dedent: 4
    :start-after: [START slack_api_post_operator_text_howto_guide]
    :end-before: [END slack_api_post_operator_text_howto_guide]


Or you could use `Block Kit <https://api.slack.com/reference/block-kit>`_ for create app layouts

.. exampleinclude:: /../../tests/system/providers/slack/example_slack.py
    :language: python
    :dedent: 4
    :start-after: [START slack_api_post_operator_blocks_howto_guide]
    :end-before: [END slack_api_post_operator_blocks_howto_guide]


SlackAPIFileOperator
--------------------

Use the :class:`~airflow.providers.slack.operators.slack.SlackAPIFileOperator` to send files to a Slack channel(s).

Using the Operator
^^^^^^^^^^^^^^^^^^

.. note::
    Operator supports two methods for upload files, which controlled by ``method_version``,
    by default it use Slack SDK method ``upload_files_v2`` it is possible to use legacy ``upload_files``
    method by set ``v1`` to ``method_version`` however this not recommended because it
    might impact a performance, cause random API errors and is being sunset on March 11, 2025 in addition
    beginning May 8, 2024, newly-created apps will be unable to use this API method.

    If you previously use ``v1`` you should check that your application has appropriate scopes:

    * **files:write** - for write files.
    * **files:read** - for read files (not required if you use Slack SDK >= 3.23.0).
    * **channels:read** - get list of public channels, for convert Channel Name to Channel ID.
    * **groups:read** - get list of private channels, for convert Channel Name to Channel ID
    * **mpim:read** - additional permission for API method **conversations.list**
    * **im:read** - additional permission for API method **conversations.list**

    .. seealso::
        - `Slack SDK 3.19.0 Release Notes <https://github.com/slackapi/python-slack-sdk/releases/tag/v3.19.0>`_
        - `conversations.list API <https://api.slack.com/methods/conversations.list>`_
        - `files.upload retires in March 2025 <https://api.slack.com/changelog/2024-04-a-better-way-to-upload-files-is-here-to-stay>`_

You could send file attachment by specifying file path

.. exampleinclude:: /../../tests/system/providers/slack/example_slack.py
    :language: python
    :start-after: [START slack_api_file_operator_howto_guide]
    :end-before: [END slack_api_file_operator_howto_guide]


Or by directly providing file contents

.. exampleinclude:: /../../tests/system/providers/slack/example_slack.py
    :language: python
    :start-after: [START slack_api_file_operator_content_howto_guide]
    :end-before: [END slack_api_file_operator_content_howto_guide]
