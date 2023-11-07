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
