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
Mock Cadwyn version bundle used by the supervisor-schemas integration tests.

The bundle and its body classes live in their own module so that any
helper or fixture that needs them imports a single canonical
definition. The integration test installs them via a ``monkeypatch``
fixture for the duration of one test and tears down automatically.

Two body classes mirror the production split between channels:

- :class:`_LangSdkRequest` -- lang-SDK -> supervisor. Three fields with
  three dated breaking changes; each entry pairs a
  ``schema(...).didnt_exist`` instruction with a
  ``convert_request_to_next_version_for`` backfill so a wire payload
  from an older runtime reaches the head Pydantic class with every
  field present.
- :class:`_SupervisorResponse` -- supervisor -> lang-SDK. Three fields
  with three dated breaking changes; each entry carries only
  ``schema(...).didnt_exist`` (responses never flow upstream, so no
  upgrade transformer is needed).
"""

from __future__ import annotations

from typing import Literal

from cadwyn import (
    HeadVersion,
    Version,
    VersionBundle,
    VersionChange,
    convert_request_to_next_version_for,
    schema,
)
from pydantic import BaseModel


class _LangSdkRequest(BaseModel):
    """
    lang-SDK -> supervisor request body.

    Three fields appear here; an older runtime omits later fields and
    the upgrade walk backfills them so the supervisor's head decoder
    always validates.
    """

    type: Literal["_LangSdkRequest"] = "_LangSdkRequest"
    ti_id: str
    field_a: int | None = None
    field_b: int | None = None
    field_c: int | None = None


class _SupervisorResponse(BaseModel):
    """
    supervisor -> lang-SDK response body.

    Three fields appear here; the downgrade walk trims any field
    introduced after the runtime's pinned version.
    """

    type: Literal["_SupervisorResponse"] = "_SupervisorResponse"
    ti_id: str
    response_x: str | None = None
    response_y: str | None = None
    response_z: str | None = None


# Request-body breaking changes -- each adds a field and a request-side
# backfill so an older lang-SDK payload reaches the head shape intact.


class _AddRequestFieldA(VersionChange):
    """3026-02-15: introduce ``_LangSdkRequest.field_a``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_LangSdkRequest).field("field_a").didnt_exist,)

    @convert_request_to_next_version_for(_LangSdkRequest)  # type: ignore[arg-type]
    def _backfill(request):
        request.body.setdefault("field_a", 0)


class _AddRequestFieldB(VersionChange):
    """3026-05-10: introduce ``_LangSdkRequest.field_b``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_LangSdkRequest).field("field_b").didnt_exist,)

    @convert_request_to_next_version_for(_LangSdkRequest)  # type: ignore[arg-type]
    def _backfill(request):
        request.body.setdefault("field_b", 0)


class _AddRequestFieldC(VersionChange):
    """3026-08-22: introduce ``_LangSdkRequest.field_c``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_LangSdkRequest).field("field_c").didnt_exist,)

    @convert_request_to_next_version_for(_LangSdkRequest)  # type: ignore[arg-type]
    def _backfill(request):
        request.body.setdefault("field_c", 0)


# Response-body breaking changes -- downgrade-only direction, no upgrade
# transformer because responses are never sent lang-SDK -> supervisor.


class _AddResponseFieldX(VersionChange):
    """3026-03-01: introduce ``_SupervisorResponse.response_x``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(_SupervisorResponse).field("response_x").didnt_exist,
    )


class _AddResponseFieldY(VersionChange):
    """3026-06-15: introduce ``_SupervisorResponse.response_y``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(_SupervisorResponse).field("response_y").didnt_exist,
    )


class _AddResponseFieldZ(VersionChange):
    """3026-09-30: introduce ``_SupervisorResponse.response_z``."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(_SupervisorResponse).field("response_z").didnt_exist,
    )


MOCK_VERSION_BUNDLE = VersionBundle(
    HeadVersion(),
    Version("3026-09-30", _AddResponseFieldZ),
    Version("3026-08-22", _AddRequestFieldC),
    Version("3026-06-15", _AddResponseFieldY),
    Version("3026-05-10", _AddRequestFieldB),
    Version("3026-03-01", _AddResponseFieldX),
    Version("3026-02-15", _AddRequestFieldA),
    Version("3025-12-01"),
)


ALL_VERSIONS: tuple[str, ...] = (
    "3025-12-01",
    "3026-02-15",
    "3026-03-01",
    "3026-05-10",
    "3026-06-15",
    "3026-08-22",
    "3026-09-30",
)


MOCK_REGISTRY: dict[str, type] = {
    "_LangSdkRequest": _LangSdkRequest,
    "_SupervisorResponse": _SupervisorResponse,
}
"""
Wire-discriminator -> head class map for the mock bundle.

Production lookups in ``resolve_body_class`` go through
``schema.registered_models_by_name``. The
``mock_version_migrator`` fixture in :mod:`test_integration` swaps that
lookup for this dict so the upgrade path can resolve
``_LangSdkRequest`` / ``_SupervisorResponse`` discriminators without
touching the real registry.
"""
