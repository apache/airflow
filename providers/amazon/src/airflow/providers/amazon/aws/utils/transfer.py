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
"""Utilities shared by Amazon transfer operators."""

from __future__ import annotations


def strip_overlapping_folder_markers(keys: list[str]) -> tuple[list[str], list[str]]:
    """
    Drop trailing-slash keys that are strict prefixes of other listed keys.

    Treated as directory markers. A lone trailing-slash key with no overlap
    (e.g. ``lonely/``) is preserved, and a non-slash key that happens to be a
    strict prefix of another (e.g. ``abc`` of ``abcdef``) is also preserved.
    Returns ``(kept, dropped)``.
    """
    if not keys:
        return [], []
    ordered = sorted(set(keys))
    kept: list[str] = []
    dropped: list[str] = []
    for current, nxt in zip(ordered, ordered[1:]):
        if current.endswith("/") and nxt.startswith(current):
            dropped.append(current)
        else:
            kept.append(current)
    kept.append(ordered[-1])
    return kept, dropped
