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
Cadwyn `VersionBundle` for the YAML DAG format, keyed by `$schema` version (a dated URL).

The `cadwyn` import is deferred so importing this package does not pull in
FastAPI/Starlette. Versions are newest-first.
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cadwyn import VersionBundle


@functools.cache
def get_bundle() -> VersionBundle:
    """Build the format's `VersionBundle` lazily (newest-to-oldest)."""
    from cadwyn import HeadVersion, Version, VersionBundle

    return VersionBundle(HeadVersion(), Version("2026-10-30"))
