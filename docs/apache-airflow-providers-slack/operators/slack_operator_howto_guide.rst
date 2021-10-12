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

How-to Guide for Slack Operators
================================

Introduction
------------

Slack operators can send text messages (:class:`~airflow.providers.slack.operators.slack.SlackAPIFileOperator`)
or files (:class:`~airflow.providers.slack.operators.slack.SlackAPIPostOperator`) to specified Slack channels.
Provide either ``slack_conn_id`` or ``token`` for the connection, and specify ``channel`` (name or ID).

Example Codes for Sending Files
-------------------------------

Sending files by specifying file names

.. exampleinclude:: /../../airflow/providers/slack/example_dags/example_slack.py
    :language: python
    :start-after: [START slack_operator_howto_guide_send_file]
    :end-before: [END slack_operator_howto_guide_send_file]


Sending files by directly providing file contents

.. exampleinclude:: /../../airflow/providers/slack/example_dags/example_slack.py
    :language: python
    :start-after: [START slack_operator_howto_guide_send_file_content]
    :end-before: [END slack_operator_howto_guide_send_file_content]
