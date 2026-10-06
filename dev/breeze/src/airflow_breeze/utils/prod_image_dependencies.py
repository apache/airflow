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
"""Prepare deterministic wheel metadata for the production dependency layer."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import shutil
import zipfile
from pathlib import Path

from packaging.utils import canonicalize_name, parse_wheel_filename


def write_metadata_wheel(source: Path, destination: Path) -> None:
    """Retain dependency and compatibility metadata without installing application files."""
    with zipfile.ZipFile(source) as wheel:
        metadata_paths = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise ValueError(f"Expected one METADATA file in {source.name}")
        metadata_path = metadata_paths[0]
        dist_info = metadata_path.rsplit("/", 1)[0]
        files = {
            metadata_path: wheel.read(metadata_path),
            f"{dist_info}/WHEEL": wheel.read(f"{dist_info}/WHEEL"),
        }
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for name, data in sorted(files.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        writer.writerow((name, f"sha256={digest}", len(data)))
    writer.writerow((f"{dist_info}/RECORD", "", ""))
    files[f"{dist_info}/RECORD"] = record.getvalue().encode()
    with zipfile.ZipFile(destination, "w") as wheel:
        for name, data in sorted(files.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.external_attr = 0o644 << 16
            wheel.writestr(info, data)


def prepare_dependency_context(context: Path, python_version: str) -> Path | None:
    """Copy context inputs, replacing local Airflow wheels with metadata-only wheels.

    Return ``None`` for source distributions: their dependency metadata may require running
    the build backend, so they must use the regular installation path.
    """
    if any(context.glob("*.tar.gz")) or any(context.glob("*.zip")):
        return None
    wheels = sorted(context.glob("*.whl"))
    local_wheels = []
    for wheel in wheels:
        name = canonicalize_name(parse_wheel_filename(wheel.name)[0])
        if name in {
            "apache-airflow",
            "apache-airflow-core",
            "apache-airflow-task-sdk",
            "apache-airflow-ctl",
        } or name.startswith("apache-airflow-providers-"):
            local_wheels.append(wheel)
    if not local_wheels:
        return None
    destination = context / ".dependency-cache" / python_version
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(context, destination, ignore=shutil.ignore_patterns(".dependency-cache", "*.whl"))
    # CI regenerates a timestamp comment even when the resolved requirements are unchanged.
    for constraints in destination.glob("constraints-*/constraints-*.txt"):
        constraints.write_text(
            "\n".join(
                line for line in constraints.read_text().splitlines() if not line.lstrip().startswith("#")
            )
            + "\n"
        )
    local_names = []
    for wheel in wheels:
        if wheel in local_wheels:
            write_metadata_wheel(wheel, destination / wheel.name)
            local_names.append(str(parse_wheel_filename(wheel.name)[0]))
        else:
            shutil.copyfile(wheel, destination / wheel.name)
    (destination / "metadata-distributions.txt").write_text("\n".join(sorted(set(local_names))) + "\n")
    return destination
