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

# Copyright 2013 The Servo Project Developers.
# Copyright 2017 zerolib Developers.
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

# This command is a largely vendored-in script from
# https://github.com/MuxZeroNet/reproducible/blob/master/reproducible.py
from __future__ import annotations

import contextlib
import gzip
import itertools
import locale
import os
import shutil
import tarfile
from argparse import ArgumentParser
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess

from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    OUT_DIR,
    REPRODUCIBLE_DIR,
)
from airflow_breeze.utils.python_versions import check_python_version
from airflow_breeze.utils.run_utils import run_command


def get_source_date_epoch(path: Path):
    import yaml

    reproducible_build_yaml = path / "reproducible_build.yaml"
    reproducible_build_dict = yaml.safe_load(reproducible_build_yaml.read_text())
    source_date_epoch: int = reproducible_build_dict["source-date-epoch"]
    return source_date_epoch


@contextlib.contextmanager
def cd(new_path: Path):
    """Context manager for changing the current working directory"""
    previous_path = os.getcwd()
    try:
        os.chdir(new_path.as_posix())
        yield
    finally:
        os.chdir(previous_path)


@contextlib.contextmanager
def setlocale(name: str):
    """Context manager for changing the current locale"""
    saved_locale = locale.setlocale(locale.LC_ALL)
    try:
        yield locale.setlocale(locale.LC_ALL, name)
    finally:
        locale.setlocale(locale.LC_ALL, saved_locale)


def repack_deterministically(
    source_archive: Path, dest_archive: Path, prepend_path=None, timestamp=0
) -> CompletedProcess | CalledProcessError:
    """Repack a .tar.gz archive in a deterministic (reproducible) manner.

    See https://reproducible-builds.org/docs/archives/ for more details."""

    def reset(tarinfo):
        """Helper to reset owner/group and modification time for tar entries"""
        tarinfo.uid = tarinfo.gid = 0
        tarinfo.uname = tarinfo.gname = "root"
        tarinfo.mtime = timestamp
        return tarinfo

    check_python_version()
    OUT_DIR.mkdir(exist_ok=True)
    shutil.rmtree(REPRODUCIBLE_DIR, ignore_errors=True)
    REPRODUCIBLE_DIR.mkdir(exist_ok=True)

    result = run_command(
        [
            "tar",
            "-xf",
            source_archive.as_posix(),
            "-C",
            REPRODUCIBLE_DIR.as_posix(),
        ],
        check=False,
    )
    if result.returncode != 0:
        return result
    dest_archive.unlink(missing_ok=True)
    result = run_command(
        [
            "chmod",
            "-R",
            "go=",
            REPRODUCIBLE_DIR.as_posix(),
        ],
        check=False,
    )
    with cd(REPRODUCIBLE_DIR):
        current_dir = "."
        file_list = [current_dir]
        for root, dirs, files in os.walk(current_dir):
            for name in itertools.chain(dirs, files):
                file_list.append(os.path.join(root, name))

        # Sort file entries with the fixed locale
        with setlocale("C"):
            file_list.sort(key=locale.strxfrm)

        # Use a temporary file and atomic rename to avoid partially-formed
        # packaging (in case of exceptional situations like running out of disk space).
        temp_file = f"{dest_archive}.temp~"
        with os.fdopen(
            os.open(temp_file, os.O_WRONLY | os.O_CREAT, 0o644), "wb"
        ) as out_file:
            with gzip.GzipFile(fileobj=out_file, mtime=0, mode="wb") as gzip_file:
                with tarfile.open(fileobj=gzip_file, mode="w:") as tar_file:  # type: ignore
                    for entry in file_list:
                        arcname = entry
                        if prepend_path is not None:
                            arcname = os.path.normpath(
                                os.path.join(prepend_path, arcname)
                            )
                        if arcname == ".":
                            continue
                        if arcname.startswith("./"):
                            arcname = arcname[2:]
                        tar_file.add(
                            entry, filter=reset, recursive=False, arcname=arcname
                        )
        os.rename(temp_file, dest_archive)
    return result


def main():
    check_python_version()
    parser = ArgumentParser()
    parser.add_argument("-a", "--archive", help="archive to repack")
    parser.add_argument("-o", "--out", help="archive destination")
    parser.add_argument("-p", "--prepend", help="prepend path in the archive")
    parser.add_argument(
        "-t",
        "--timestamp",
        help="timestamp of files",
        type=int,
        default=get_source_date_epoch(AIRFLOW_SOURCES_ROOT / "airflow"),
    )

    args = parser.parse_args()

    if not args.archive or not args.out:
        error = (
            "You should provide an archive to repack, and the target "
            f"archive file name, not {repr((args.archoive, args.out))}"
        )
        raise ValueError(error)

    repack_deterministically(
        source_archive=Path(args.archive),
        dest_archive=Path(args.out),
        prepend_path=args.prepend,
        timestamp=args.timestamp,
    )


if __name__ == "__main__":
    main()
