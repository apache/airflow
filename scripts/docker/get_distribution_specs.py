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
import zipfile
from email.parser import HeaderParser
from pathlib import Path

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import (
    InvalidSdistFilename,
    InvalidWheelFilename,
    parse_sdist_filename,
    parse_wheel_filename,
)

_CURRENT_PYTHON = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"


def _compatible_with_current_python(wheel_path: str) -> bool:
    """Return False if the wheel's Requires-Python excludes the running interpreter."""
    try:
        with zipfile.ZipFile(wheel_path) as zf:
            for name in zf.namelist():
                if name.endswith(".dist-info/METADATA"):
                    requires = HeaderParser().parsestr(zf.read(name).decode("utf-8")).get("Requires-Python")
                    if requires:
                        return _CURRENT_PYTHON in SpecifierSet(requires)
                    return True
    except (zipfile.BadZipFile, InvalidSpecifier, KeyError) as exc:
        print(f"Warning: could not check Requires-Python for {wheel_path}: {exc}", file=sys.stderr)
    return True


def print_package_specs(extras: str = "") -> None:
    for package_path in sys.argv[1:]:
        try:
            package, _, _, _ = parse_wheel_filename(Path(package_path).name)
        except InvalidWheelFilename:
            try:
                package, _ = parse_sdist_filename(Path(package_path).name)
            except InvalidSdistFilename:
                print(f"Could not parse package name from {package_path}", file=sys.stderr)
                continue
        if package_path.endswith(".whl") and not _compatible_with_current_python(package_path):
            print(
                f"Skipping {package} (Requires-Python not satisfied by {_CURRENT_PYTHON})",
                file=sys.stderr,
            )
            continue
        print(f"{package}{extras} @ file://{package_path}")


if __name__ == "__main__":
    print_package_specs(extras=os.environ.get("EXTRAS", ""))
