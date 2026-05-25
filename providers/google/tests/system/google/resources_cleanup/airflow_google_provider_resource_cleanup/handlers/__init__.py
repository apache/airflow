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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler


def get_delete_handlers() -> dict[str, type[BaseDeleteHandler]]:
    from airflow_google_provider_resource_cleanup.handlers.ai import AIPlatformDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.bq import BigQueryDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.composer import ComposerDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.dataform import DataformDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.dataplex import DataplexDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.dataproc import DataprocDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.dlp import DLPDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.logging import LoggingDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.ray import RayClusterOnVertexAIDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.sqladmin import CloudSQLDeleteHandler
    from airflow_google_provider_resource_cleanup.handlers.storage import StorageDeleteHandler

    return {
        "ai": AIPlatformDeleteHandler,
        "bq": BigQueryDeleteHandler,
        "bqtransfer": BigQueryDeleteHandler,
        "composer": ComposerDeleteHandler,
        "sqladmin": CloudSQLDeleteHandler,
        "dataform": DataformDeleteHandler,
        "dataplex": DataplexDeleteHandler,
        "dataproc": DataprocDeleteHandler,
        "dlp": DLPDeleteHandler,
        "logging": LoggingDeleteHandler,
        "storage": StorageDeleteHandler,
        "storagetransfer": StorageDeleteHandler,
        "vertex_ai_raycluster": RayClusterOnVertexAIDeleteHandler,
    }
