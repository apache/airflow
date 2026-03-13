#!/usr/bin/env python3
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
"""Generate the OpenAPI spec for the registry API from shared Pydantic contracts.

Usage:
    python dev/registry/export_registry_schemas.py
"""

from __future__ import annotations

import json
from pathlib import Path

from registry_contract_models import build_openapi_document

AIRFLOW_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = AIRFLOW_ROOT / "registry" / "schemas" / "openapi.json"


def main() -> int:
    content = json.dumps(build_openapi_document(), indent=2, sort_keys=True) + "\n"
    OPENAPI_PATH.parent.mkdir(parents=True, exist_ok=True)
    OPENAPI_PATH.write_text(content)
    print(f"  wrote {OPENAPI_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
