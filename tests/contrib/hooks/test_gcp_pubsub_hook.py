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
#

from base64 import b64encode as b64e
import unittest

from apiclient.errors import HttpError

from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
PUBSUB_STRING = 'airflow.contrib.hooks.gcp_pubsub_hook.{}'

TEST_PROJECT = 'test-project'
TEST_TOPIC = 'test-topic'
TEST_MESSAGES = [
    {
        'data': b64e('Hello, World!'),
        'attributes': {'type': 'greeting'}
    },
    {'data': b64e('Knock, knock')},
    {'attributes': {'foo': ''}}]

EXPANDED_TOPIC = 'projects/%s/topics/%s' % (TEST_PROJECT, TEST_TOPIC)


def mock_init(self, gcp_conn_id, delegate_to=None):
    pass


class PubSubHookTest(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.pubsub_hook = PubSubHook(gcp_conn_id='test')

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_create_nonexistent_topic(self, mock_service):
        self.pubsub_hook.create_topic(TEST_PROJECT, TEST_TOPIC)

        create_method = (mock_service.return_value.projects.return_value.topics
                         .return_value.create)
        create_method.assert_called_with(body={}, name=EXPANDED_TOPIC)
        create_method.return_value.execute.assert_called_with()

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_create_preexisting_topic_failifexists(self, mock_service):
        (mock_service.return_value.projects.return_value.topics.return_value
         .create.return_value.execute.side_effect) = HttpError(
            resp={'status': '409'}, content='')

        try:
            self.pubsub_hook.create_topic(TEST_PROJECT, TEST_TOPIC,
                                          fail_if_exists=True)
        except Exception:
            pass  # Expected.
        else:
            self.fail('Topic creation should fail for existing topic when '
                      'fail_if_exists=True')

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_create_preexisting_topic_nofailifexists(self, mock_service):
        (mock_service.return_value.projects.return_value.topics.return_value
         .get.return_value.execute.side_effect) = HttpError(
            resp={'status': '409'}, content='')

        try:
            self.pubsub_hook.create_topic(TEST_PROJECT, TEST_TOPIC,
                                          fail_if_exists=False)
        except Exception:
            self.fail('Topic creation should not fail for existing topic when '
                      'fail_if_exists=False')

    @mock.patch(PUBSUB_STRING.format('PubSubHook.get_conn'))
    def test_publish(self, mock_service):
        self.pubsub_hook.publish(TEST_PROJECT, TEST_TOPIC, TEST_MESSAGES)

        publish_method = (mock_service.return_value.projects.return_value
                          .topics.return_value.publish)
        publish_method.assert_called_with(
            topic=EXPANDED_TOPIC, body={'messages': TEST_MESSAGES})
