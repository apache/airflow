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
"""Test HBase provider metadata."""

from __future__ import annotations


class TestHBaseProvider:
    """Test HBase provider registration."""

    def test_get_provider_info(self):
        """Test that get_provider_info returns valid metadata."""
        from airflow.providers.apache.hbase.get_provider_info import get_provider_info

        provider_info = get_provider_info()

        assert provider_info is not None
        assert isinstance(provider_info, dict)

        # Check required fields
        assert "package-name" in provider_info
        assert provider_info["package-name"] == "apache-airflow-providers-apache-hbase"

        assert "name" in provider_info
        assert provider_info["name"] == "Apache HBase"

        # Check hooks
        assert "hooks" in provider_info
        assert isinstance(provider_info["hooks"], list)
        assert len(provider_info["hooks"]) > 0

        # Check operators
        assert "operators" in provider_info
        assert isinstance(provider_info["operators"], list)
        assert len(provider_info["operators"]) > 0

        # Check connection types
        assert "connection-types" in provider_info
        assert isinstance(provider_info["connection-types"], list)
        assert len(provider_info["connection-types"]) == 2  # HBaseThriftHook and HBaseCLIHook

        # Verify connection types
        hook_classes = [ct["hook-class-name"] for ct in provider_info["connection-types"]]
        assert "airflow.providers.apache.hbase.hooks.hbase.HBaseThriftHook" in hook_classes
        assert "airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook" in hook_classes

    def test_provider_version(self):
        """Test that provider version is defined."""
        from airflow.providers.apache.hbase import __version__

        assert __version__ is not None
        assert isinstance(__version__, str)
        assert len(__version__) > 0
