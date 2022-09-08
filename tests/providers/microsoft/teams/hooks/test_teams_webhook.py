#
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
#
import json

from airflow.providers.microsoft.teams.hooks.teams_webhook import TeamsWebhookHook


class TestTeamsWebhookHook:

    _config = {
        'http_conn_id': 'teams-webhook-default',
        'webhook_token': 'manual_token',
        'message': 'Awesome teams message',
        'subtitle': 'your subtitle here',
        'facts': [{'name': 'Issuer', 'value': 'Airflow'}],
        'action_button_name': 'Learn More',
        'action_button_url': 'https://airflow.apache.org',
        'theme_color': 'FF0000',
        'icon_url': 'https://airflow.apache.org/_images/pin_large.png',
    }
    expected_message_dict = {
        "themeColor": "FF0000",
        "summary": "Awesome teams message",
        "sections": [
            {
                "activityTitle": "Awesome teams message",
                "activitySubtitle": "your subtitle here",
                "activityImage": "https://airflow.apache.org/_images/pin_large.png",
                "facts": [{"name": "Issuer", "value": "Airflow"}],
                "potentialAction": [
                    {
                        "@context": "http://schema.org",
                        "@type": "OpenUri",
                        "name": "Learn More",
                        "targets": [{"os": "default", "uri": "https://airflow.apache.org"}],
                    }
                ],
            }
        ],
    }

    def test_get_token_manual_token(self):
        manual_token = 'manual_token_here'
        hook = TeamsWebhookHook(webhook_token=manual_token)

        webhook_token = hook._get_token(manual_token, None)

        assert webhook_token == manual_token

    def test_build_teams_message(self):
        hook = TeamsWebhookHook(**self._config)
        message = hook._build_teams_message()
        assert self.expected_message_dict == json.loads(message)
