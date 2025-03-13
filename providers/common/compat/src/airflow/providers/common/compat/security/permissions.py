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
    from airflow.security.permissions import RESOURCE_ASSET, RESOURCE_ASSET_ALIAS, RESOURCE_BACKFILL
else:
    try:
        from airflow.security.permissions import RESOURCE_ASSET, RESOURCE_ASSET_ALIAS, RESOURCE_BACKFILL
    except ImportError:
        from airflow.security.permissions import RESOURCE_DATASET as RESOURCE_ASSET


__all__ = ["RESOURCE_ASSET", "RESOURCE_ASSET_ALIAS", "RESOURCE_BACKFILL"]
