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

from airflow.sdk.definitions.partition_mappers.base import PartitionMapper


class ProductMapper(PartitionMapper):
    """Partition mapper that combines multiple mappers into a multi-dimensional key."""

    def __init__(self, mappers: list[PartitionMapper]) -> None:
        if len(mappers) < 2:
            raise ValueError("ProductMapper requires at least 2 child mappers")
        self.mappers = mappers
