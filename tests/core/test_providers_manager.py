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
import re
import unittest

from airflow.providers_manager import ProvidersManager


class TestProviderManager(unittest.TestCase):
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

    def test_hooks(self):
        provider_manager = ProvidersManager()
        connections_list = list(provider_manager.hooks.keys())
        assert len(connections_list) > 60

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
