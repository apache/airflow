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
from __future__ import annotations

from airflow.providers.google.cloud.links.bigtable import (
    BigtableClusterLink,
    BigtableInstanceLink,
    BigtableTablesLink,
)

EXPECTED_BIGTABLE_INSTANCE_LINK_NAME = "Bigtable Instance"
EXPECTED_BIGTABLE_INSTANCE_LINK_KEY = "instance_key"
EXPECTED_BIGTABLE_INSTANCE_LINK_FORMAT_STR = "/bigtable/instances/{instance_id}/overview?project={project_id}"
EXPECTED_BIGTABLE_CLUSTER_LINK_NAME = "Bigtable Cluster"
EXPECTED_BIGTABLE_CLUSTER_LINK_KEY = "cluster_key"
EXPECTED_BIGTABLE_CLUSTER_LINK_FORMAT_STR = (
    "/bigtable/instances/{instance_id}/clusters/{cluster_id}?project={project_id}"
)
EXPECTED_BIGTABLE_TABLES_LINK_NAME = "Bigtable Tables"
EXPECTED_BIGTABLE_TABLES_LINK_KEY = "tables_key"
EXPECTED_BIGTABLE_TABLES_LINK_FORMAT_STR = "/bigtable/instances/{instance_id}/tables?project={project_id}"


class TestBigtableInstanceLink:
    def test_class_attributes(self):
        assert BigtableInstanceLink.name == EXPECTED_BIGTABLE_INSTANCE_LINK_NAME
        assert BigtableInstanceLink.key == EXPECTED_BIGTABLE_INSTANCE_LINK_KEY
        assert BigtableInstanceLink.format_str == EXPECTED_BIGTABLE_INSTANCE_LINK_FORMAT_STR


class TestBigtableClusterLink:
    def test_class_attributes(self):
        assert BigtableClusterLink.name == EXPECTED_BIGTABLE_CLUSTER_LINK_NAME
        assert BigtableClusterLink.key == EXPECTED_BIGTABLE_CLUSTER_LINK_KEY
        assert BigtableClusterLink.format_str == EXPECTED_BIGTABLE_CLUSTER_LINK_FORMAT_STR


class TestBigtableTablesLink:
    def test_class_attributes(self):
        assert BigtableTablesLink.name == EXPECTED_BIGTABLE_TABLES_LINK_NAME
        assert BigtableTablesLink.key == EXPECTED_BIGTABLE_TABLES_LINK_KEY
        assert BigtableTablesLink.format_str == EXPECTED_BIGTABLE_TABLES_LINK_FORMAT_STR
