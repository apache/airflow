# -*- coding: utf-8 -*-
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

from apiclient.errors import HttpError
import unittest
from airflow.contrib.hooks.gcp_spanner_hook import SpannerHook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = 'test-project-id'
DATASET_ID = 'test-dataset-id'
BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
SPANNER_STRING = 'airflow.contrib.hooks.gcp_spanner_hook.{}'


def mock_init(self, gcp_conn_id, delegate_to=None):
    pass


class SpannerHookTest(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.spanner_hook = SpannerHook()

    @mock.patch(SPANNER_STRING.format('SpannerHook.get_conn'))
    def test_list_instance_configs(self, mock_service):
        self.spanner_hook.list_instance_configs(PROJECT_ID)

        list_method = (mock_service.return_value.projects.return_value.instanceConfigs
                         .return_value.list)
        list_method.assert_called_with("projects/{}".format(PROJECT_ID))
        list_method.return_value.execute.assert_called_with()
