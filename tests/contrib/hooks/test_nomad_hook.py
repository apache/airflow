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

import unittest

from mock import Mock
from mock import patch

from airflow import configuration
from airflow.contrib.hooks.nomad_hook import NomadHook

nomad_client_mock = Mock(
        name="nomad_client"
)


class TestNomadHook(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    @patch("airflow.contrib.hooks.nomad_hook.NOMAD", autospec=True,
           return_value=nomad_client_mock)
    def test_nomad_client_connection(self, nomad_mock):
        nomad_hook = NomadHook(ip='127.0.0.1', port='4646')

        self.assertTrue(nomad_mock.called)
        self.assertIsInstance(nomad_hook.client, Mock)


if __name__ == '__main__':
    unittest.main()
