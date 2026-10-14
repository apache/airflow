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

from airflow.providers.google.cloud.links.cloud_sql import (
    CloudSQLInstanceDatabaseLink,
    CloudSQLInstanceLink,
)

EXPECTED_CLOUD_SQL_INSTANCE_LINK_NAME = "Cloud SQL Instance"
EXPECTED_CLOUD_SQL_INSTANCE_LINK_KEY = "cloud_sql_instance"
EXPECTED_CLOUD_SQL_INSTANCE_LINK_FORMAT_STR = "/sql/instances/{instance}/overview?project={project_id}"
EXPECTED_CLOUD_SQL_INSTANCE_DATABASE_LINK_NAME = "Cloud SQL Instance Database"
EXPECTED_CLOUD_SQL_INSTANCE_DATABASE_LINK_KEY = "cloud_sql_instance_database"
EXPECTED_CLOUD_SQL_INSTANCE_DATABASE_LINK_FORMAT_STR = (
    "/sql/instances/{instance}/databases?project={project_id}"
)


class TestCloudSQLInstanceLink:
    def test_class_attributes(self):
        assert CloudSQLInstanceLink.name == EXPECTED_CLOUD_SQL_INSTANCE_LINK_NAME
        assert CloudSQLInstanceLink.key == EXPECTED_CLOUD_SQL_INSTANCE_LINK_KEY
        assert CloudSQLInstanceLink.format_str == EXPECTED_CLOUD_SQL_INSTANCE_LINK_FORMAT_STR


class TestCloudSQLInstanceDatabaseLink:
    def test_class_attributes(self):
        assert CloudSQLInstanceDatabaseLink.name == EXPECTED_CLOUD_SQL_INSTANCE_DATABASE_LINK_NAME
        assert CloudSQLInstanceDatabaseLink.key == EXPECTED_CLOUD_SQL_INSTANCE_DATABASE_LINK_KEY
        assert CloudSQLInstanceDatabaseLink.format_str == EXPECTED_CLOUD_SQL_INSTANCE_DATABASE_LINK_FORMAT_STR
