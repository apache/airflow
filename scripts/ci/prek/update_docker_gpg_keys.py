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
"""Fetch GPG public keys used during Docker image builds and store them in scripts/docker/keys/."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile

from common_prek_utils import AIRFLOW_ROOT_PATH

KEYS_DIR = AIRFLOW_ROOT_PATH / "scripts" / "docker" / "keys"

KEYSERVERS = [
    "hkps://keyserver.ubuntu.com",
    "hkps://pgp.surf.nl",
    "hkps://keys.openpgp.org",
]

KEYS: dict[str, str] = {
    # MariaDB APT repository signing key
    "mariadb": "0xF1656F24C74CD1D8",
    # PostgreSQL PGDG APT repository signing key
    "postgres": "7FCC7D46ACCC4CF8",
    # Microsoft APT repository signing key (MSSQL ODBC)
    "microsoft": "EB3E94ADBE1229CF",
    # Python 3.10 release manager (Pablo Galindo Salgado)
    "python-3.10": "A035C8C19219BA821ECEA86B64E628F8D684696D",
}


def _recv_key(key_id: str, gpg_env: dict[str, str]) -> bool:
    """Try to receive a GPG key from keyservers, trying each in order."""
    for keyserver in KEYSERVERS:
        result = subprocess.run(
            ["gpg", "--batch", "--keyserver", keyserver, "--recv-keys", key_id],
            env=gpg_env,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return True
        print(f"  Warning: {keyserver} failed for {key_id}: {result.stderr.strip()}", file=sys.stderr)
    return False


def fetch_and_export_keys() -> bool:
    """Fetch all keys from keyservers and export as ASCII-armored files. Returns True if any changed."""
    changed = False
    KEYS_DIR.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as gnupghome:
        gpg_env = {**os.environ, "GNUPGHOME": gnupghome}

        for name, key_id in KEYS.items():
            if not _recv_key(key_id, gpg_env):
                print(f"ERROR: All keyservers failed for {key_id} ({name})", file=sys.stderr)
                return False

            result = subprocess.run(
                ["gpg", "--export", "--armor", key_id],
                env=gpg_env,
                capture_output=True,
                check=False,
            )
            if result.returncode != 0 or not result.stdout:
                print(f"ERROR: Failed to export key {key_id} ({name})", file=sys.stderr)
                return False

            key_file = KEYS_DIR / f"{name}.asc"
            new_content = result.stdout
            if key_file.exists() and key_file.read_bytes() == new_content:
                continue

            key_file.write_bytes(new_content)
            print(f"Updated {key_file.relative_to(AIRFLOW_ROOT_PATH)}")
            changed = True

    return changed


if __name__ == "__main__":
    changed = fetch_and_export_keys()
    if changed:
        print("\nGPG key files updated. Please review and commit the changes.")
        sys.exit(1)
