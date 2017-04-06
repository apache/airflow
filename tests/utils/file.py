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

from mock import patch

from airflow.utils.file import use_virtualenv


class VirtualEnvTest(unittest.TestCase):

    @patch('airflow.utils.file.sys', executable='/usr/bin/python')
    def test_not_using_virtualenv(self, mock_sys):
        if hasattr(mock_sys, 'real_prefix'):
            del mock_sys.real_prefix
        self.assertEqual('gunicorn', use_virtualenv('gunicorn'))

    @patch('airflow.utils.file.sys', real_prefix='/usr',
           executable='/path/to/venv/bin/python')
    def test_using_virtualenv(self, mock_sys):
        self.assertEqual('/path/to/venv/bin/gunicorn',
                         use_virtualenv('gunicorn'))
