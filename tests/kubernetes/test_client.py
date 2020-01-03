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

import unittest

import mock

from airflow.kubernetes.kube_client import RefreshConfiguration, get_kube_client


class TestClient(unittest.TestCase):

    @mock.patch('airflow.kubernetes.kube_client.config')
    def test_load_cluster_config(self, _):
        client = get_kube_client(in_cluster=True)
        assert not isinstance(client.api_client.configuration, RefreshConfiguration)

    @mock.patch('airflow.kubernetes.kube_client.config')
    @mock.patch('airflow.kubernetes.refresh_config._get_kube_config_loader_for_yaml_file')
    def test_load_file_config(self, _, _2):
        client = get_kube_client(in_cluster=False)
        assert isinstance(client.api_client.configuration, RefreshConfiguration)
