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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "diagrams>=0.23.4",
#   "rich>=13.6.0",
# ]
# ///

from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import console


def _get_file_hash(file_to_check: Path) -> str:
    hash_md5 = hashlib.md5()
    hash_md5.update(Path(file_to_check).resolve().read_bytes())
    return hash_md5.hexdigest()


def main():
    # get all files as arguments
    for arg in sys.argv[1:]:
        source_file = Path(arg).resolve()
        checksum = _get_file_hash(source_file)
        hash_file = source_file.with_suffix(".md5sum")
        if not hash_file.exists() or not hash_file.read_text().strip() == str(checksum).strip():
            console.print(f"[bright_blue]Changes in {source_file}. Regenerating the image.")
            process = subprocess.run(
                [sys.executable, source_file.resolve().as_posix()], check=False, cwd=source_file.parent
            )
            if process.returncode != 0:
                if sys.platform == "darwin":
                    console.print(
                        "[red]Likely you have no graphviz installed[/]"
                        "Please install eralchemy package to run this script. "
                        "This will require to install graphviz, "
                        "and installing graphviz might be difficult for MacOS. Please follow: "
                        "https://pygraphviz.github.io/documentation/stable/install.html#macos ."
                    )
                sys.exit(process.returncode)
            hash_file.write_text(str(checksum) + "\n")
        else:
            console.print(f"[bright_blue]No changes in {source_file}. Not regenerating the image.")


if __name__ == "__main__":
    main()
