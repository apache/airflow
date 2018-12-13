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

import unittest

from airflow.contrib.hooks.gcp_memorystore_hook import MemorystoreHook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = "test-project-id"
INSTANCE_ID = "test-instance_id"
LOCATION_ID = "us-east1"
BASE_STRING = "airflow.contrib.hooks.gcp_api_base_hook.{}"
MEMORYSTORE_STRING = "airflow.contrib.hooks.gcp_memorystore_hook.{}"
MEMORYSTORE_HOOK_CONN_STRING = MEMORYSTORE_STRING.format("MemorystoreHook.get_conn")


def mock_init(self, gcp_conn_id, delegate_to=None, version=None):
    pass


class MemorystoreHookTest(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            BASE_STRING.format("GoogleCloudBaseHook.__init__"), new=mock_init
        ):
            self.memorystore_hook = MemorystoreHook()

    @mock.patch(MEMORYSTORE_HOOK_CONN_STRING)
    def test_list_instances(self, mock_service):
        self.memorystore_hook.list_instances(PROJECT_ID, LOCATION_ID)

        method = (
            mock_service.return_value.projects.return_value.locations.return_value
            .instances.return_value.list
        )
        method.assert_called_with(
            parent="projects/{}/locations/{}".format(PROJECT_ID, LOCATION_ID)
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(MEMORYSTORE_HOOK_CONN_STRING)
    def test_create_instance(self, mock_service):
        instance_body = {
            "name": "projects/{}/locations/{}/instances/{}".format(
                PROJECT_ID, LOCATION_ID, INSTANCE_ID
            ),
            "tier": "BASIC",
            "memorySizeGb": 1,
        }
        self.memorystore_hook.create_instance(PROJECT_ID, LOCATION_ID, INSTANCE_ID)

        method = (
            mock_service.return_value.projects.return_value.locations.return_value
            .instances.return_value.create
        )
        method.assert_called_with(
            parent="projects/{}/locations/{}".format(PROJECT_ID, LOCATION_ID),
            instanceId=INSTANCE_ID,
            body=instance_body,
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(MEMORYSTORE_HOOK_CONN_STRING)
    def test_delete_instance(self, mock_service):
        self.memorystore_hook.delete_instance(PROJECT_ID, LOCATION_ID, INSTANCE_ID)

        method = (
            mock_service.return_value.projects.return_value.locations.return_value
            .instances.return_value.delete
        )
        method.assert_called_with(
            name="projects/{}/locations/{}/instances/{}".format(
                PROJECT_ID, LOCATION_ID, INSTANCE_ID
            )
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(MEMORYSTORE_HOOK_CONN_STRING)
    def test_get_instance(self, mock_service):
        self.memorystore_hook.get_instance(PROJECT_ID, LOCATION_ID, INSTANCE_ID)

        method = (
            mock_service.return_value.projects.return_value.locations.return_value
            .instances.return_value.get
        )
        method.assert_called_with(
            name="projects/{}/locations/{}/instances/{}".format(
                PROJECT_ID, LOCATION_ID, INSTANCE_ID
            )
        )
        method.return_value.execute.assert_called_with()
