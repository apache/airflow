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

from airflow.providers.celery.cleanup import get_db_cleanup_table_configs

from tests_common.test_utils.config import conf_vars


class TestCeleryDbCleanupTableConfigs:
    def test_default_uses_bare_table_names(self):
        configs = get_db_cleanup_table_configs()
        assert [c["table_name"] for c in configs] == ["celery_taskmeta", "celery_tasksetmeta"]
        assert all(c["recency_column_name"] == "date_done" for c in configs)

    def test_result_backend_schema_qualifies_table_names(self):
        with conf_vars({("celery", "result_backend_schema"): "celery"}):
            configs = get_db_cleanup_table_configs()
        assert [c["table_name"] for c in configs] == [
            "celery.celery_taskmeta",
            "celery.celery_tasksetmeta",
        ]
