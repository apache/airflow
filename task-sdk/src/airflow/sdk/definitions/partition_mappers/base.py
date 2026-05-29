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

if TYPE_CHECKING:
    from airflow.sdk.definitions.partition_mappers.window import Window


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
    #: Whether ``to_downstream`` collapses every upstream key onto a single
    #: downstream partition key. Anchor-independent windows (segment windows,
    #: see :attr:`Window.requires_collapsing_mapper`) require this so that every
    #: upstream event lands in the same downstream partition and can accumulate
    #: into one rollup. Only :class:`ConstantMapper` sets this to ``True``.
    collapses_to_constant: ClassVar[bool] = False


class RollupMapper(PartitionMapper):
    """
    Partition mapper that rolls up many upstream keys into one downstream key.

    Compose a ``upstream_mapper`` (which normalizes each upstream key to the
    downstream granularity) with a ``window`` that declares the full set of
    upstream keys required for a given downstream key. The scheduler holds
    the Dag run until every upstream key in the window has arrived.
    """

    is_rollup: ClassVar[bool] = True

    def __init__(self, *, upstream_mapper: PartitionMapper, window: Window) -> None:
        # Mirrors the core-side ``RollupMapper.__init__`` check so user code
        # ``from airflow.sdk import RollupMapper`` fails at Dag parse time rather
        # than slipping through to the scheduler tick (where the misconfiguration
        # would otherwise be swallowed by the bare ``except`` in
        # ``_create_dagruns_for_partitioned_asset_dags`` and surface only as
        # "Failed to deserialize Dag" spam).
        if upstream_mapper.expected_decoded_type is str and window.expected_decoded_type is not str:
            raise TypeError(
                f"{type(window).__name__} expects decoded values of type "
                f"{window.expected_decoded_type.__name__!r}, but "
                f"{type(upstream_mapper).__name__} decodes to 'str' (SDK PartitionMapper default). "
                f"Pair the window with an upstream mapper whose 'expected_decoded_type' is "
                f"{window.expected_decoded_type.__name__}, or use a window whose "
                f"'expected_decoded_type' accepts str."
            )
        # Anchor-independent windows return the same upstream set for every
        # downstream key, so the rollup only fills up if every upstream event
        # collapses onto a single downstream partition. A non-collapsing mapper
        # (e.g. IdentityMapper) instead fans each upstream key into its own
        # partition, so no partition ever accumulates the full set and the gate
        # never opens. Reject the pairing at parse time rather than letting it
        # hang silently on the scheduler.
        if window.requires_collapsing_mapper and not upstream_mapper.collapses_to_constant:
            raise TypeError(
                f"{type(window).__name__} returns the same upstream set for every downstream "
                f"key, so all upstream events must collapse onto one downstream partition. "
                f"{type(upstream_mapper).__name__} maps each upstream key to a distinct "
                f"downstream partition, so the rollup gate can never be satisfied. Pair the "
                f"window with a collapsing mapper such as ConstantMapper."
            )
        self.upstream_mapper = upstream_mapper
        self.window = window
