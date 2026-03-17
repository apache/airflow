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
from __future__ import annotations

import os
from pathlib import Path


def detect_context() -> str:
    """
    Return "breeze" when running inside a Breeze container, otherwise "host".

    This is intentionally a small heuristic for the PoC and can be refined later.
    """
    if os.environ.get("AIRFLOW_BREEZE_CONTAINER"):
        return "breeze"
    if Path("/opt/airflow").exists() and Path("/.dockerenv").exists():
        return "breeze"
    return "host"


def main() -> None:
    print(detect_context())


if __name__ == "__main__":
    main()
