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

from typing import Any, Mapping, Sequence

from airflow.api_connexion.exceptions import BadRequest


def extract_update_mask_data(
    update_mask: Sequence[str], non_update_fields: list[str], data: Mapping[str, Any]
) -> Mapping[str, Any]:
    extracted_data = {}
    for field in update_mask:
        field = field.strip()
        if field in data and field not in non_update_fields:
            extracted_data[field] = data[field]
        else:
            raise BadRequest(detail=f"'{field}' is unknown or cannot be updated.")
    return extracted_data
