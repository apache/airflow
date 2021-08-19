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
import logging
import re
import unittest
from unittest.mock import patch

import pytest

from airflow.providers_manager import ProviderInfo, ProvidersManager


class TestProviderManager(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def test_providers_are_loaded(self):
        provider_manager = ProvidersManager()
        provider_list = list(provider_manager.providers.keys())
        # No need to sort the list - it should be sorted alphabetically !
        for provider in provider_list:
            package_name = provider_manager.providers[provider][1]['package-name']
            version = provider_manager.providers[provider][0]
            assert re.search(r'[0-9]*\.[0-9]*\.[0-9]*.*', version)
            assert package_name == provider
        # just a sanity check - no exact number as otherwise we would have to update
        # several tests if we add new connections/provider which is not ideal
        assert len(provider_list) > 65

    def test_hooks_deprecation_warnings_generated(self):
        with pytest.warns(expected_warning=DeprecationWarning, match='hook-class-names') as warning_records:
            providers_manager = ProvidersManager()
            providers_manager._provider_dict['test-package'] = ProviderInfo(
                version='0.0.1',
                provider_info={'hook-class-names': ['airflow.providers.sftp.hooks.sftp.SFTPHook']},
            )
            providers_manager._discover_hooks()
        assert warning_records

    def test_hooks_deprecation_warnings_not_generated(self):
        with pytest.warns(expected_warning=None) as warning_records:
            providers_manager = ProvidersManager()
            providers_manager._provider_dict['apache-airflow-providers-sftp'] = ProviderInfo(
                version='0.0.1',
                provider_info={
                    'hook-class-names': ['airflow.providers.sftp.hooks.sftp.SFTPHook'],
                    'connection-types': [
                        {
                            'hook-class-name': 'airflow.providers.sftp.hooks.sftp.SFTPHook',
                            'connection-type': 'sftp',
                        }
                    ],
                },
            )
            providers_manager._discover_hooks()
        assert [] == [w.message for w in warning_records.list if "hook-class-names" in str(w.message)]

    def test_warning_logs_generated(self):
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManager()
            providers_manager._provider_dict['apache-airflow-providers-sftp'] = ProviderInfo(
                version='0.0.1',
                provider_info={
                    'hook-class-names': ['airflow.providers.sftp.hooks.sftp.SFTPHook'],
                    'connection-types': [
                        {
                            'hook-class-name': 'airflow.providers.sftp.hooks.sftp.SFTPHook',
                            'connection-type': 'wrong-connection-type',
                        }
                    ],
                },
            )
            providers_manager._discover_hooks()
            _ = providers_manager._hooks_lazy_dict['wrong-connection-type']
        assert len(self._caplog.records) == 1
        assert "Inconsistency!" in self._caplog.records[0].message
        assert "sftp" not in providers_manager.hooks

    def test_warning_logs_not_generated(self):
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManager()
            providers_manager._provider_dict['apache-airflow-providers-sftp'] = ProviderInfo(
                version='0.0.1',
                provider_info={
                    'hook-class-names': ['airflow.providers.sftp.hooks.sftp.SFTPHook'],
                    'connection-types': [
                        {
                            'hook-class-name': 'airflow.providers.sftp.hooks.sftp.SFTPHook',
                            'connection-type': 'sftp',
                        }
                    ],
                },
            )
            providers_manager._discover_hooks()
            _ = providers_manager._hooks_lazy_dict['sftp']
        assert not self._caplog.records
        assert "sftp" in providers_manager.hooks

    @patch('airflow.providers_manager.importlib.import_module')
    def test_hooks(self, mock_import_module):
        with pytest.warns(expected_warning=None) as warning_records:
            with self._caplog.at_level(logging.WARNING):
                provider_manager = ProvidersManager()
                connections_list = list(provider_manager.hooks.keys())
                assert len(connections_list) > 60
        assert [] == [w.message for w in warning_records.list if "hook-class-names" in str(w.message)]
        assert len(self._caplog.records) == 0
        mock_import_module.assert_not_called()

    def test_hook_values(self):
        with pytest.warns(expected_warning=None) as warning_records:
            with self._caplog.at_level(logging.WARNING):
                provider_manager = ProvidersManager()
                connections_list = list(provider_manager.hooks.values())
                assert len(connections_list) > 60
        assert [] == [w.message for w in warning_records.list if "hook-class-names" in str(w.message)]
        assert len(self._caplog.records) == 0

    def test_connection_form_widgets(self):
        provider_manager = ProvidersManager()
        connections_form_widgets = list(provider_manager.connection_form_widgets.keys())
        assert len(connections_form_widgets) > 29

    def test_field_behaviours(self):
        provider_manager = ProvidersManager()
        connections_with_field_behaviours = list(provider_manager.field_behaviours.keys())
        assert len(connections_with_field_behaviours) > 16

    def test_extra_links(self):
        provider_manager = ProvidersManager()
        extra_link_class_names = list(provider_manager.extra_links_class_names)
        assert len(extra_link_class_names) > 5

    def test_logging(self):
        provider_manager = ProvidersManager()
        logging_class_names = list(provider_manager.logging_class_names)
        assert len(logging_class_names) > 5

    def test_secrets_backends(self):
        provider_manager = ProvidersManager()
        secrets_backends_class_names = list(provider_manager.secrets_backend_class_names)
        assert len(secrets_backends_class_names) > 4

    def test_auth_backends(self):
        provider_manager = ProvidersManager()
        auth_backend_module_names = list(provider_manager.auth_backend_module_names)
        assert len(auth_backend_module_names) > 0
