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

from sqlalchemy import MetaData
from sqlalchemy.orm import registry

from airflow.models.base import _get_schema, naming_convention
from airflow.utils.sqlalchemy import is_sqlalchemy_v1

edge_metadata = MetaData(schema=_get_schema(), naming_convention=naming_convention)
edge_mapper_registry = registry(metadata=edge_metadata)

if TYPE_CHECKING:
    EdgeBase = Any
else:
    EdgeBase = edge_mapper_registry.generate_base()
    if not is_sqlalchemy_v1():
        EdgeBase.__allow_unmapped__ = True
