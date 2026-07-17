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

from airflow.partition_mappers.base import PartitionMapper

if TYPE_CHECKING:
    from datetime import datetime


class IdentityMapper(PartitionMapper):
    """Partition mapper that does not change the key."""

    def to_downstream(self, key: str) -> str:
        return key

    def carry_partition_date(self, source_partition_date: datetime | None) -> datetime | None:
        # Identity passthrough: the consumer's key equals the producer's, so the
        # producer's partition_date is the consumer's. to_partition_date cannot
        # recover it (the key carries no temporal meaning), so it is carried here.
        return source_partition_date
