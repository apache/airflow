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

from sqlalchemy import MetaData
from sqlalchemy.orm import registry

from airflow.models.base import _get_schema, naming_convention

# Isolated metadata for Edge3 provider tables.
# By using a dedicated MetaData + registry + Base, Edge3 tables are never
# registered in Airflow core's Base.metadata, avoiding validation conflicts
# without needing the post-hoc Base.metadata.remove() hack.
edge_metadata = MetaData(schema=_get_schema(), naming_convention=naming_convention)
_edge_mapper_registry = registry(metadata=edge_metadata)
Base = _edge_mapper_registry.generate_base()
Base.__allow_unmapped__ = True  # match core Base workaround for unmapped v1.4 models
