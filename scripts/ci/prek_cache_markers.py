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

"""Snapshot prek installation metadata used to detect cache repairs."""

from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

MARKERS = (("hooks", ".prek-hook.json"), ("repos", ".prek-repo.json"))


def snapshot_markers(cache_dir: Path) -> dict[str, str]:
    """Return shallow marker paths and their byte-for-byte contents."""
    snapshot = {}
    for directory, marker_name in MARKERS:
        parent = cache_dir / directory
        try:
            entries = sorted(parent.iterdir())
        except FileNotFoundError:
            continue
        for entry in entries:
            marker = entry / marker_name
            try:
                contents = marker.read_bytes()
            except FileNotFoundError:
                continue
            relative_path = marker.relative_to(cache_dir).as_posix()
            snapshot[relative_path] = base64.b64encode(contents).decode()
    return snapshot


def main() -> None:
    cache_dir, output_file = map(Path, sys.argv[1:])
    snapshot = snapshot_markers(cache_dir)
    # Marker names are prek internals; an unrecognised layout must not look unchanged.
    if not snapshot and cache_dir.is_dir() and any(cache_dir.iterdir()):
        sys.exit("Populated prek cache contains no recognised installation markers")
    Path(output_file).write_text(json.dumps(snapshot, sort_keys=True))


if __name__ == "__main__":
    main()
