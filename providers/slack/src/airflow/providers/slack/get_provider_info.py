# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE OVERWRITTEN!
#
# IF YOU WANT TO MODIFY THIS FILE, YOU SHOULD MODIFY THE TEMPLATE
# `get_provider_info_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze/templates` DIRECTORY


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-slack",
        "name": "Slack",
        "description": "`Slack <https://slack.com/>`__ services integration including:\n\n  - `Slack API <https://api.slack.com/>`__\n  - `Slack Incoming Webhook <https://api.slack.com/messaging/webhooks>`__\n",
        "state": "ready",
        "source-date-epoch": 1741509641,
        "versions": [
            "9.0.2",
            "9.0.1",
            "9.0.0",
            "8.9.2",
            "8.9.1",
            "8.9.0",
            "8.8.0",
            "8.7.1",
            "8.7.0",
            "8.6.2",
            "8.6.1",
            "8.6.0",
            "8.5.1",
            "8.5.0",
            "8.4.0",
            "8.3.0",
            "8.2.0",
            "8.1.0",
            "8.0.0",
            "7.3.2",
            "7.3.1",
            "7.3.0",
            "7.2.0",
            "7.1.1",
            "7.1.0",
            "7.0.0",
            "6.0.0",
            "5.1.0",
            "5.0.0",
            "4.2.3",
            "4.2.2",
            "4.2.1",
            "4.2.0",
            "4.1.0",
            "4.0.1",
            "4.0.0",
            "3.0.0",
            "2.0.0",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Slack",
                "external-doc-url": "https://slack.com/",
                "logo": "/docs/integration-logos/Slack.png",
                "tags": ["service"],
            },
            {
                "integration-name": "Slack API",
                "external-doc-url": "https://api.slack.com/",
                "how-to-guide": ["/docs/apache-airflow-providers-slack/operators/slack_api.rst"],
                "tags": ["service"],
            },
            {
                "integration-name": "Slack Incoming Webhook",
                "external-doc-url": "https://api.slack.com/messaging/webhooks",
                "how-to-guide": ["/docs/apache-airflow-providers-slack/operators/slack_webhook.rst"],
                "tags": ["service"],
            },
        ],
        "operators": [
            {"integration-name": "Slack API", "python-modules": ["airflow.providers.slack.operators.slack"]},
            {
                "integration-name": "Slack Incoming Webhook",
                "python-modules": ["airflow.providers.slack.operators.slack_webhook"],
            },
        ],
        "hooks": [
            {"integration-name": "Slack API", "python-modules": ["airflow.providers.slack.hooks.slack"]},
            {
                "integration-name": "Slack Incoming Webhook",
                "python-modules": ["airflow.providers.slack.hooks.slack_webhook"],
            },
        ],
        "transfers": [
            {
                "source-integration-name": "Common SQL",
                "target-integration-name": "Slack",
                "python-module": "airflow.providers.slack.transfers.base_sql_to_slack",
            },
            {
                "source-integration-name": "Common SQL",
                "target-integration-name": "Slack API",
                "python-module": "airflow.providers.slack.transfers.sql_to_slack",
                "how-to-guide": "/docs/apache-airflow-providers-slack/operators/sql_to_slack.rst",
            },
            {
                "source-integration-name": "Common SQL",
                "target-integration-name": "Slack Incoming Webhook",
                "python-module": "airflow.providers.slack.transfers.sql_to_slack_webhook",
                "how-to-guide": "/docs/apache-airflow-providers-slack/operators/sql_to_slack_webhook.rst",
            },
        ],
        "connection-types": [
            {"hook-class-name": "airflow.providers.slack.hooks.slack.SlackHook", "connection-type": "slack"},
            {
                "hook-class-name": "airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook",
                "connection-type": "slackwebhook",
            },
        ],
        "notifications": [
            "airflow.providers.slack.notifications.slack.SlackNotifier",
            "airflow.providers.slack.notifications.slack_webhook.SlackWebhookNotifier",
        ],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "apache-airflow-providers-common-sql>=1.20.0",
            "slack_sdk>=3.19.0",
        ],
        "devel-dependencies": [],
    }
