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

from typing import TYPE_CHECKING, ClassVar

import attrs

from airflow.sdk.definitions.partition_mappers.wait_policy import WaitForAll, WaitPolicy

if TYPE_CHECKING:
    from airflow.sdk.definitions.partition_mappers.window import Window


def _validate_max_downstream_keys(instance, attribute, value):
    if value is not None and (not isinstance(value, int) or value < 1):
        raise ValueError(f"max_downstream_keys must be a positive integer or None, got {value!r}")


@attrs.define
class PartitionMapper:
    """
    Base partition mapper class.

    Maps keys from asset events to target Dag run partitions.
    """

    is_rollup: ClassVar[bool] = False
    #: Declared decoded type produced by ``decode_downstream`` for this mapper.
    #: ``RollupMapper.__init__`` rejects pairings where this stays at the base
    #: identity ``str`` but the window needs a different type (e.g. ``datetime``).
    #: Temporal mappers override to ``datetime``.
    expected_decoded_type: ClassVar[type] = str

    max_downstream_keys: int | None = attrs.field(
        default=None, kw_only=True, validator=_validate_max_downstream_keys
    )


@attrs.define
class RollupMapper(PartitionMapper):
    """
    Partition mapper that rolls up many upstream keys into one downstream key.

    Compose an ``upstream_mapper`` (which normalizes each upstream key to the
    downstream granularity) with a ``window`` that declares the full set of
    upstream keys required for a given downstream key, and a
    ``wait_policy`` that decides when the downstream Dag run fires given
    the expected window and the upstream keys that have actually arrived.

    The ``wait_policy`` is a :class:`WaitPolicy` instance. The default
    ``WaitForAll()`` fires only when every expected upstream key has arrived.
    ``MinimumCount(n)`` fires once at least ``n`` keys have arrived when
    ``n`` is positive, or once at most ``-n`` keys are still missing when
    ``n`` is negative.
    """

    is_rollup: ClassVar[bool] = True

    upstream_mapper: PartitionMapper = attrs.field(kw_only=True)
    window: Window = attrs.field(kw_only=True)
    wait_policy: WaitPolicy = attrs.field(factory=WaitForAll, kw_only=True)

    def __attrs_post_init__(self) -> None:
        # Mirrors the core-side ``RollupMapper.__init__`` check so user code
        # ``from airflow.sdk import RollupMapper`` fails at Dag parse time rather
        # than slipping through to the scheduler tick (where the misconfiguration
        # would otherwise be swallowed by the bare ``except`` in
        # ``_create_dagruns_for_partitioned_asset_dags`` and surface only as
        # "Failed to deserialize Dag" spam).
        if self.upstream_mapper.expected_decoded_type is str and self.window.expected_decoded_type is not str:
            raise TypeError(
                f"{type(self.window).__name__} expects decoded values of type "
                f"{self.window.expected_decoded_type.__name__!r}, but "
                f"{type(self.upstream_mapper).__name__} decodes to 'str' (SDK PartitionMapper default). "
                f"Pair the window with an upstream mapper whose 'expected_decoded_type' is "
                f"{self.window.expected_decoded_type.__name__}, or use a window whose "
                f"'expected_decoded_type' accepts str."
            )
