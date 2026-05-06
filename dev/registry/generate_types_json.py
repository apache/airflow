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
"""Generate types.json for the registry frontend from registry_tools.types."""

from __future__ import annotations

import json
from pathlib import Path

from registry_tools.types import MODULE_TYPES

REGISTRY_DATA_DIR = Path(__file__).parent.parent.parent / "registry" / "src" / "_data"


def main() -> None:
    types_list = [
        {"id": type_id, "label": info["label"], "icon": info["icon"]}
        for type_id, info in MODULE_TYPES.items()
    ]
    output_path = REGISTRY_DATA_DIR / "types.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(types_list, f, indent=2)
        f.write("\n")
    print(f"Wrote {len(types_list)} types to {output_path}")


if __name__ == "__main__":
    main()
