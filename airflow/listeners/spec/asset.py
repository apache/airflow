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

from pluggy import HookspecMarker

if TYPE_CHECKING:
    from airflow.assets import Asset, AssetAlias

hookspec = HookspecMarker("airflow")


@hookspec
def on_asset_created(asset: Asset):
    """Execute when a new asset is created."""


@hookspec
def on_asset_alias_created(dataset_alias: AssetAlias):
    """Execute when a new dataset alias is created."""


@hookspec
def on_asset_changed(asset: Asset):
    """Execute when asset change is registered."""
