# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import json
from airflow.exceptions import AirflowException
from airflow.operators.slack_operator import SlackAPIPostOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class SlackAPIPostOperatorTestCase(unittest.TestCase):
    def setUp(self):
        self.test_username = 'test_username'
        self.test_channel = '#test_slack_channel'
        self.test_text = 'test_text'
        self.test_icon_url = 'test_icon_url'
        self.test_attachments = [
            {
                "fallback": "Required plain-text summary of the attachment.",
                "color": "#36a64f",
                "pretext": "Optional text that appears above the attachment block",
                "author_name": "Bobby Tables",
                "author_link": "http://flickr.com/bobby/",
                "author_icon": "http://flickr.com/icons/bobby.jpg",
                "title": "Slack API Documentation",
                "title_link": "https://api.slack.com/",
                "text": "Optional text that appears within the attachment",
                "fields": [
                    {
                        "title": "Priority",
                        "value": "High",
                        "short": 'false'
                    }
                ],
                "image_url": "http://my-website.com/path/to/image.jpg",
                "thumb_url": "http://example.com/path/to/thumb.png",
                "footer": "Slack API",
                "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png",
                "ts": 123456789
            }
        ]
        self.test_attachments_in_json = json.dumps(self.test_attachments)
        self.test_kwarg = 'test_kwarg'

        self.expected_method = 'chat.postMessage'
        self.expected_api_params = {
            'channel': self.test_channel,
            'username': self.test_username,
            'text': self.test_text,
            'icon_url': self.test_icon_url,
            'attachments': self.test_attachments_in_json,
        }

    def __construct_operator(self, test_token, test_slack_conn_id):
        return SlackAPIPostOperator(
            task_id='slack',
            username=self.test_username,
            token=test_token,
            slack_conn_id=test_slack_conn_id,
            channel=self.test_channel,
            text=self.test_text,
            icon_url=self.test_icon_url,
            attachments=self.test_attachments,
            kwarg=self.test_kwarg
        )

    @mock.patch('airflow.operators.slack_operator.SlackHook')
    def test_execute_with_token_only(self, slack_hook_class_mock):
        slack_hook_mock = mock.Mock()
        slack_hook_class_mock.return_value = slack_hook_mock

        test_token = 'test_token'
        slack_api_post_operator = self.__construct_operator(test_token, None)

        slack_api_post_operator.execute()

        slack_hook_class_mock.assert_called_with(token=test_token, slack_conn_id=None)

        slack_hook_mock.call.assert_called_with(self.expected_method, self.expected_api_params)

    @mock.patch('airflow.operators.slack_operator.SlackHook')
    def test_execute_with_slack_conn_id_only(self, slack_hook_class_mock):
        slack_hook_mock = mock.Mock()
        slack_hook_class_mock.return_value = slack_hook_mock

        test_slack_conn_id = 'test_slack_conn_id'
        slack_api_post_operator = self.__construct_operator(None, test_slack_conn_id)

        slack_api_post_operator.execute()

        slack_hook_class_mock.assert_called_with(token=None, slack_conn_id=test_slack_conn_id)

        slack_hook_mock.call.assert_called_with(self.expected_method, self.expected_api_params)

    def test_init_with_invalid_params(self):
        test_token = 'test_token'
        test_slack_conn_id = 'test_slack_conn_id'
        self.assertRaises(AirflowException, self.__construct_operator, test_token, test_slack_conn_id)

        self.assertRaises(AirflowException, self.__construct_operator, None, None)

    def test_init_with_valid_params(self):
        test_token = 'test_token'
        test_slack_conn_id = 'test_slack_conn_id'

        slack_api_post_operator = self.__construct_operator(test_token, None)
        self.assertEqual(slack_api_post_operator.token, test_token)
        self.assertEqual(slack_api_post_operator.slack_conn_id, None)
        self.assertEqual(slack_api_post_operator.method, self.expected_method)
        self.assertEqual(slack_api_post_operator.text, self.test_text)
        self.assertEqual(slack_api_post_operator.channel, self.test_channel)
        self.assertEqual(slack_api_post_operator.api_params, self.expected_api_params)
        self.assertEqual(slack_api_post_operator.username, self.test_username)
        self.assertEqual(slack_api_post_operator.icon_url, self.test_icon_url)
        self.assertEqual(slack_api_post_operator.attachments, self.test_attachments)

        slack_api_post_operator = self.__construct_operator(None, test_slack_conn_id)
        self.assertEqual(slack_api_post_operator.token, None)
        self.assertEqual(slack_api_post_operator.slack_conn_id, test_slack_conn_id)


if __name__ == "__main__":
    unittest.main()
