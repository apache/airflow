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

from dataclasses import dataclass


@dataclass
class DataSourceConfig:
    """config for input datasource."""

    conn_id: str
    uri: str
    format: str | None = None
    table_name: str | None = None
    schema: dict[str, str] | None = None
    db_name: str | None = None

    def __post_init__(self):

        if self.schema is not None and not isinstance(self.schema, dict):
            raise ValueError("Schema must be a dictionary of column names and types")
