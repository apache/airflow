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

from pathlib import Path

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )


AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()


def stable_sort(x):
    return x.casefold(), x


def sort_uniq(sequence):
    return sorted(set(sequence), key=stable_sort)


def sort_file(path: Path):
    content = path.read_text().splitlines(keepends=True)
    sorted_content = sort_uniq(content)
    path.write_text("".join(sorted_content))


if __name__ == "__main__":
    prod_image_installed_providers_path = (
        AIRFLOW_SOURCES / "prod_image_installed_providers.txt"
    )
    sort_file(prod_image_installed_providers_path)
