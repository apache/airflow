#!/usr/bin/env python3
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
"""
Module to update db migration information in Airflow
"""
import os
from pathlib import Path

from checksumdir import dirhash
from rich.console import Console

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[2].resolve()
SVG_FILE = AIRFLOW_SOURCES_ROOT / "docs" / "apache-airflow" / 'img' / 'airflow_erd.svg'
HASH_FILE = SVG_FILE.with_suffix(".sha256")

MIGRATIONS_DIR = AIRFLOW_SOURCES_ROOT / "airflow" / "migrations"
if __name__ == '__main__':
    from eralchemy2 import render_er

    console = Console(width=400)

    sha256hash = dirhash(
        MIGRATIONS_DIR, 'sha256', excluded_extensions=['pyc'], ignore_hidden=True, include_paths=True
    )
    old_hash = HASH_FILE.read_text() if HASH_FILE.exists() else ""
    if sha256hash != old_hash:
        console.print(
            f"[bright_blue]Generating diagram in {SVG_FILE} as some files "
            f"changed in {MIGRATIONS_DIR} since last generation."
        )
        render_er(
            os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"),
            os.fspath(SVG_FILE),
            exclude_tables=['sqlite_sequence'],
        )
        HASH_FILE.write_text(sha256hash)
        host_os = os.environ.get('HOST_OS')
        if host_os and host_os.lower() == 'linux':
            try:
                host_uid = int(os.environ['HOST_USER_ID'])
                host_gid = int(os.environ['HOST_GROUP_ID'])
                os.chown(path=HASH_FILE, uid=host_uid, gid=host_gid)
                os.chown(path=SVG_FILE, uid=host_uid, gid=host_gid)
            except Exception as e:
                console.print("[yellow]Exception while fixing ownership. Skipping fixing it:", e)
        console.print(f"[bright_blue]Hash file saved in {HASH_FILE}")
        console.print(f"[green]The diagram has been generated in {SVG_FILE}. Please commit the changes!")
    else:
        console.print("[green]Skip file generation as no files changes since last generation")
