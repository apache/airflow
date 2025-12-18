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

from typing import Any


def normalize_asset_metadata(metadata_dict: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize Asset metadata to ensure JSON serializability.

    This function should be called before creating AssetMetadata objects.

    Location to integrate: airflow/assets.py or wherever AssetMetadata is created
    """
    try:
        import numpy as np
    except ImportError:
        np = None

    def _normalize_value(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _normalize_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_normalize_value(item) for item in value]
        if np:
            if isinstance(value, np.integer):
                return int(value)
            if isinstance(value, np.floating):
                return float(value)
            if isinstance(value, np.bool_):
                return bool(value)
            if isinstance(value, np.ndarray):
                return value.tolist()
            if isinstance(value, np.complexfloating):
                return {"real": float(value.real), "imag": float(value.imag)}
        return value

    return _normalize_value(metadata_dict)
