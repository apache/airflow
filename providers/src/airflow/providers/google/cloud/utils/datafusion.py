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

from enum import Enum


class DataFusionPipelineType(Enum):
    """Enum for Data Fusion pipeline types."""

    BATCH = "batch"
    STREAM = "stream"

    @staticmethod
    def from_str(value: str) -> DataFusionPipelineType:
        value_to_item = {item.value: item for item in DataFusionPipelineType}
        if value in value_to_item:
            return value_to_item[value]
        raise ValueError(
            f"Invalid value '{value}'. Valid values are: {[i for i in value_to_item.keys()]}"
        )
