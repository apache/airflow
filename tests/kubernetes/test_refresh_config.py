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

import os
from unittest import TestCase, mock

import pytest
from kubernetes.config.kube_config import ConfigNode
from pendulum.parsing import ParserError

from airflow.kubernetes.refresh_config import (
    RefreshConfiguration,
    RefreshKubeConfigLoader,
    _get_kube_config_loader_for_yaml_file,
    _parse_timestamp,
)


class TestRefreshKubeConfigLoader(TestCase):
    ROOT_PROJECT_DIR = os.path.abspath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
    )

    KUBE_CONFIG_PATH = os.path.join(ROOT_PROJECT_DIR, "tests", "kubernetes", "kube_config")

    def test_parse_timestamp_should_convert_z_timezone_to_unix_timestamp(self):
        ts = _parse_timestamp("2020-01-13T13:42:20Z")
        assert 1578922940 == ts

    def test_parse_timestamp_should_convert_regular_timezone_to_unix_timestamp(self):
        ts = _parse_timestamp("2020-01-13T13:42:20+0600")
        assert 1578922940 == ts

    def test_parse_timestamp_should_throw_exception(self):
        with pytest.raises(ParserError):
            _parse_timestamp("foobar")

    def test_get_kube_config_loader_for_yaml_file(self):
        refresh_kube_config_loader = _get_kube_config_loader_for_yaml_file(self.KUBE_CONFIG_PATH)

        assert refresh_kube_config_loader is not None

        assert refresh_kube_config_loader.current_context['name'] == 'federal-context'

        context = refresh_kube_config_loader.current_context['context']
        assert context is not None
        assert context['cluster'] == 'horse-cluster'
        assert context['namespace'] == 'chisel-ns'
        assert context['user'] == 'green-user'

    def test_get_api_key_with_prefix(self):

        refresh_config = RefreshConfiguration()
        refresh_config.api_key['key'] = '1234'
        assert refresh_config is not None

        api_key = refresh_config.get_api_key_with_prefix("key")

        assert api_key == '1234'

    @mock.patch('kubernetes.config.exec_provider.ExecProvider.__init__', return_value=None)
    @mock.patch('kubernetes.config.exec_provider.ExecProvider.run', return_value={'token': '1234'})
    def test_refresh_kube_config_loader(self, exec_provider_run, exec_provider_init):
        current_context = _get_kube_config_loader_for_yaml_file(self.KUBE_CONFIG_PATH).current_context

        config_dict = {}
        config_dict['current-context'] = 'federal-context'
        config_dict['contexts'] = []
        config_dict['contexts'].append(current_context)

        config_dict['clusters'] = []

        cluster_config = {}
        cluster_config['api-version'] = 'v1'
        cluster_config['server'] = 'http://cow.org:8080'
        cluster_config['name'] = 'horse-cluster'
        cluster_root_config = {}
        cluster_root_config['cluster'] = cluster_config
        cluster_root_config['name'] = 'horse-cluster'
        config_dict['clusters'].append(cluster_root_config)

        refresh_kube_config_loader = RefreshKubeConfigLoader(config_dict=config_dict)
        refresh_kube_config_loader._user = {}

        config_node = ConfigNode('command', 'test')
        config_node.__dict__['apiVersion'] = '2.0'
        config_node.__dict__['command'] = 'test'

        refresh_kube_config_loader._user['exec'] = config_node

        result = refresh_kube_config_loader._load_from_exec_plugin()
        assert result is not None
