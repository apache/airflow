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

from typing import TYPE_CHECKING, Any

import attrs

from airflow.datasets import DatasetAlias, extract_event_key

if TYPE_CHECKING:
    from airflow.datasets import Dataset


@attrs.define(init=False)
class Metadata:
    """Metadata to attach to a DatasetEvent."""

    uri: str
    extra: dict[str, Any]
    alias_name: str | None = None

    def __init__(
        self, target: str | Dataset, extra: dict[str, Any], alias: DatasetAlias | str | None = None
    ) -> None:
        self.uri = extract_event_key(target)
        self.extra = extra
        if isinstance(alias, DatasetAlias):
            self.alias_name = alias.name
        else:
            self.alias_name = alias
