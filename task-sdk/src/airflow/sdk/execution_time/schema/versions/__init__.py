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

import functools
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cadwyn import VersionBundle


@functools.cache
def get_bundle() -> VersionBundle:
    """
    Build the supervisor schema ``VersionBundle`` lazily.

    The ``cadwyn`` import is deferred to here so importing this module -- and the parent ``schema``
    package, which the Task SDK supervisor pulls in on every worker -- does not import ``cadwyn``,
    which drags in FastAPI/Starlette/Jinja2. The bundle (and its cadwyn machinery) is only needed on
    the foreign-language-SDK migration path; a pure-Python worker never builds it. Cached so the
    bundle is constructed once per process.
    """
    from cadwyn import HeadVersion, Version, VersionBundle

    from airflow.sdk.execution_time.schema.versions.v2026_07_30 import AddStubArgsToTIRunContext

    return VersionBundle(
        HeadVersion(),
        Version("2026-07-30", AddStubArgsToTIRunContext),
        Version("2026-06-16"),
    )


def __getattr__(name: str) -> Any:
    # Keep ``from ...versions import bundle`` working for existing callers without forcing the
    # cadwyn import at module load (only resolves the bundle when ``bundle`` is actually accessed).
    if name == "bundle":
        return get_bundle()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
