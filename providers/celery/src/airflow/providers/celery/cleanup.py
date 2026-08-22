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
"""``airflow db clean`` table contributions for the Celery result backend."""

from __future__ import annotations

from typing import Any

from airflow.providers.common.compat.sdk import conf


def get_db_cleanup_table_configs() -> list[dict[str, Any]]:
    """
    Specify Celery result backend table configs for ``airflow db clean`` to clean.

    Registered via the ``db-cleanup-tables`` provider extension point.

    This needs to be defined in the provider since the returned table names are
    schema-qualified when ``[celery] result_backend_schema`` is set.
    """
    schema = conf.get("celery", "result_backend_schema", fallback=None)

    def qualified(table_name: str) -> str:
        return f"{schema}.{table_name}" if schema else table_name

    return [
        {"table_name": qualified("celery_taskmeta"), "recency_column_name": "date_done"},
        {"table_name": qualified("celery_tasksetmeta"), "recency_column_name": "date_done"},
    ]
