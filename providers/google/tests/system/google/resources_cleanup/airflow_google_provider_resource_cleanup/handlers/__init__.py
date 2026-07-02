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
    from typing import Type

    from ._base import BaseDeleteHandler


def get_delete_handlers() -> dict[str, Type[BaseDeleteHandler]]:
    from .ai import AIPlatformDeleteHandler
    from .bq import BigQueryDeleteHandler
    from .composer import ComposerDeleteHandler
    from .dataform import DataformDeleteHandler
    from .dataplex import DataplexDeleteHandler
    from .dataproc import DataprocDeleteHandler
    from .dlp import DLPDeleteHandler
    from .logging import LoggingDeleteHandler
    from .ray import RayClusterOnVertexAIDeleteHandler
    from .sqladmin import CloudSQLDeleteHandler
    from .storage import StorageDeleteHandler

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
