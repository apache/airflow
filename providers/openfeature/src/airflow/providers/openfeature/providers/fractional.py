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
"""A dependency-free, in-process OpenFeature provider with deterministic percentage rollouts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from openfeature.flag_evaluation import FlagResolutionDetails, Reason
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata

_ANONYMOUS = "__anonymous__"


def _bucket(entity: str, flag_key: str) -> int:
    """Return a deterministic bucket in ``0..99`` for ``(entity, flag_key)``.

    Stable across processes and machines (uses ``hashlib`` rather than the salted builtin ``hash``),
    so the same entity always lands in the same cohort for a given flag.
    """
    digest = hashlib.sha256(f"{flag_key}:{entity}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % 100


def _entity_of(evaluation_context) -> str:
    if evaluation_context is None:
        return _ANONYMOUS
    attributes = evaluation_context.attributes or {}
    return evaluation_context.targeting_key or attributes.get("entity") or attributes.get("id") or _ANONYMOUS


@dataclass
class BoolFlag:
    """A boolean flag enabled for ``rollout_pct`` percent of entities (0..100)."""

    rollout_pct: int


@dataclass
class VariantFlag:
    """A multi-variant string flag. ``variants`` is ``[(value, weight)]`` with weights summing to 100."""

    variants: list[tuple[str, int]]


class FractionalProvider(AbstractProvider):
    """In-process OpenFeature provider doing deterministic percentage rollouts.

    Useful for zero-dependency canary/percentage rollouts and for testing without a running flag
    daemon. For richer targeting or a shared source of truth, point OpenFeature at a backend such as
    flagd, Unleash, or GrowthBook instead; the evaluation call sites do not change.

    :param bool_flags: mapping of flag key to :class:`BoolFlag` (percentage rollout).
    :param variant_flags: mapping of flag key to :class:`VariantFlag` (weighted variants).
    """

    def __init__(
        self,
        bool_flags: dict[str, BoolFlag] | None = None,
        variant_flags: dict[str, VariantFlag] | None = None,
    ) -> None:
        self._bool_flags = dict(bool_flags or {})
        self._variant_flags = dict(variant_flags or {})

    def get_metadata(self) -> Metadata:
        return Metadata(name="FractionalProvider")

    def get_provider_hooks(self) -> list:
        return []

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        flag = self._bool_flags.get(flag_key)
        if flag is None:
            return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)
        enabled = _bucket(_entity_of(evaluation_context), flag_key) < flag.rollout_pct
        return FlagResolutionDetails(
            value=enabled, variant="on" if enabled else "off", reason=Reason.TARGETING_MATCH
        )

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        flag = self._variant_flags.get(flag_key)
        if flag is None:
            return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)
        bucket = _bucket(_entity_of(evaluation_context), flag_key)
        cumulative = 0
        for value, weight in flag.variants:
            cumulative += weight
            if bucket < cumulative:
                return FlagResolutionDetails(value=value, variant=value, reason=Reason.TARGETING_MATCH)
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)
