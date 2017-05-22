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

import mock
import unittest

from airflow.operators.slack_operator import SlackPostOperator

class TestSlackPostOperator(unittest.TestCase):
    @mock.patch('airflow.hooks.slack_webhook_hook.SlackWebHookHook.post')
    def test_execute(self, hook_post):
        operator = SlackPostOperator(task_id='test_task_id', text='something',
                                     icon_url='icon_url')
        operator.execute({})

        hook_post.assert_called_once_with({'text': 'something',
                                           'channel': '#general',
                                           'username': 'Airflow',
                                           'icon': 'icon_url',
                                           'attachments': None})



