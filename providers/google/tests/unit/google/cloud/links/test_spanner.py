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

from airflow.providers.google.cloud.links.spanner import (
    SpannerDatabaseLink,
    SpannerInstanceLink,
)

EXPECTED_SPANNER_INSTANCE_LINK_NAME = "Spanner Instance"
EXPECTED_SPANNER_INSTANCE_LINK_KEY = "spanner_instance"
EXPECTED_SPANNER_INSTANCE_LINK_FORMAT_STR = (
    "/spanner/instances/{instance_id}/details/databases?project={project_id}"
)
EXPECTED_SPANNER_DATABASE_LINK_NAME = "Spanner Database"
EXPECTED_SPANNER_DATABASE_LINK_KEY = "spanner_database"
EXPECTED_SPANNER_DATABASE_LINK_FORMAT_STR = (
    "/spanner/instances/{instance_id}/databases/{database_id}/details/tables?project={project_id}"
)


class TestSpannerInstanceLink:
    def test_class_attributes(self):
        assert SpannerInstanceLink.name == EXPECTED_SPANNER_INSTANCE_LINK_NAME
        assert SpannerInstanceLink.key == EXPECTED_SPANNER_INSTANCE_LINK_KEY
        assert SpannerInstanceLink.format_str == EXPECTED_SPANNER_INSTANCE_LINK_FORMAT_STR


class TestSpannerDatabaseLink:
    def test_class_attributes(self):
        assert SpannerDatabaseLink.name == EXPECTED_SPANNER_DATABASE_LINK_NAME
        assert SpannerDatabaseLink.key == EXPECTED_SPANNER_DATABASE_LINK_KEY
        assert SpannerDatabaseLink.format_str == EXPECTED_SPANNER_DATABASE_LINK_FORMAT_STR
