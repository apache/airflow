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
from __future__ import annotations

from airflow.models.base import naming_convention
from airflow.providers.edge3.models.edge_base import Base, edge_metadata


class TestEdgeBase:
    def test_edge_metadata_is_isolated_from_core(self):
        """edge_metadata must not be the same object as Airflow core's Base.metadata."""
        from airflow.models.base import Base as CoreBase

        assert edge_metadata is not CoreBase.metadata

    def test_edge_tables_not_in_core_metadata(self):
        """Edge3 tables must never appear in Airflow core's Base.metadata."""
        from airflow.models.base import Base as CoreBase

        edge_table_names = {"edge_worker", "edge_job", "edge_logs"}
        core_table_names = set(CoreBase.metadata.tables.keys())
        assert not edge_table_names & core_table_names

    def test_edge_metadata_contains_edge_tables(self):
        """edge_metadata must contain all three Edge3 tables."""
        # Import models to ensure they are registered
        import airflow.providers.edge3.models.edge_job
        import airflow.providers.edge3.models.edge_logs
        import airflow.providers.edge3.models.edge_worker  # noqa: F401

        assert "edge_worker" in edge_metadata.tables
        assert "edge_job" in edge_metadata.tables
        assert "edge_logs" in edge_metadata.tables

    def test_edge_metadata_uses_same_naming_convention_as_core(self):
        """edge_metadata should use the same naming convention as Airflow core."""
        assert edge_metadata.naming_convention == naming_convention

    def test_base_allow_unmapped(self):
        """Base must have __allow_unmapped__ set to match core Base workaround."""
        assert Base.__allow_unmapped__ is True
