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

from airflow.providers.common.dataquality.backends.object_storage import ObjectStorageResultsBackend

__all__ = ["ObjectStorageResultsBackend", "get_backend_from_config"]


def get_backend_from_config() -> ObjectStorageResultsBackend | None:
    """Build the results backend configured under ``[common.dataquality]``, or ``None`` when not configured."""
    from airflow.providers.common.compat.sdk import conf

    results_path = conf.get("common.dataquality", "results_path", fallback=None)
    if not results_path:
        return None
    conn_id = conf.get("common.dataquality", "results_conn_id", fallback=None)
    return ObjectStorageResultsBackend(results_path=results_path, conn_id=conn_id or None)
