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

import json
import os

from in_container_utils import console

if __name__ == "__main__":
    excluded_providers = json.loads(os.environ.get("EXCLUDED_PROVIDERS", "{}"))
    extra = os.environ["EXTRA"]
    console.print(
        f"[bright_blue]Check if provider {extra} is excluded in {excluded_providers}"
    )
    python_version = os.environ["PYTHON_MAJOR_MINOR_VERSION"]
    for provider in excluded_providers.get(python_version, []):
        provider_extra = "[" + provider.replace(".", "-") + "]"
        console.print(f"[bright_blue]Checking {provider_extra}.")
        if provider_extra == extra:
            console.print(
                f"[yellow]Provider {provider} is excluded for Python {python_version}"
            )
            exit(1)
    console.print(f"[green]Provider {extra} is not excluded.")
