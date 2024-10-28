#!/usr/bin/env python
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
import sys
from pathlib import Path

from packaging.utils import (
    InvalidSdistFilename,
    InvalidWheelFilename,
    parse_sdist_filename,
    parse_wheel_filename,
)


def print_package_specs(extras: str = "") -> None:
    for package_path in sys.argv[1:]:
        try:
            package, _, _, _ = parse_wheel_filename(Path(package_path).name)
        except InvalidWheelFilename:
            try:
                package, _ = parse_sdist_filename(Path(package_path).name)
            except InvalidSdistFilename:
                print(
                    f"Could not parse package name from {package_path}", file=sys.stderr
                )
                continue
        print(f"{package}{extras} @ file://{package_path}")


if __name__ == "__main__":
    print_package_specs(extras=os.environ.get("EXTRAS", ""))
