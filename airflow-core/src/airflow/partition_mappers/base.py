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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, TypeGuard

if TYPE_CHECKING:
    from collections.abc import Iterable

    from airflow.partition_mappers.window import Window


class PartitionMapper(ABC):
    """
    Base partition mapper class.

    Maps keys from asset events to target Dag run partitions.
    """

    is_rollup: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        decode_overridden = cls.decode_downstream is not PartitionMapper.decode_downstream
        encode_overridden = cls.encode_upstream is not PartitionMapper.encode_upstream
        if decode_overridden ^ encode_overridden:
            raise TypeError(
                f"{cls.__qualname__} overrides only one side of the decode/encode pair. "
                "Both 'decode_downstream' and 'encode_upstream' must be overridden together "
                "or not at all. Overriding only one side leaves RollupMapper.to_upstream "
                "producing non-str members, causing the scheduler's upstream-window check "
                "to silently never satisfy and the Dag run to be held forever."
            )

    @abstractmethod
    def to_downstream(self, key: str) -> str | Iterable[str]:
        """Return the target key that the given source partition key maps to."""

    def decode_downstream(self, downstream_key: str) -> Any:
        """
        Recover the canonical decoded form of *downstream_key*.

        Used by :class:`RollupMapper` to hand the window an opaque "anchor"
        for the downstream period; the window iterates in this decoded space
        and the mapper re-encodes each expected upstream via
        :meth:`encode_upstream`. Default is identity (string in, string out)
        — temporal mappers override to return ``datetime``, future segment
        mappers will return whatever shape suits them.

        .. warning::

            ``decode_downstream`` and :meth:`encode_upstream` form an inverse
            pair. If you override this method to return a non-str (e.g.
            ``datetime``, ``tuple``), you **must** also override
            ``encode_upstream`` to convert the decoded form back to an
            upstream key string. Overriding only one side leaves
            :class:`RollupMapper.to_upstream` producing non-str members, the
            scheduler's upstream-window check silently never satisfies, and
            the Dag run is held forever with no audit log entry.
        """
        return downstream_key

    def encode_upstream(self, decoded: Any) -> str:
        """
        Encode an expected upstream object back into a key string.

        Pair of :meth:`decode_downstream`. Default is identity. Temporal
        mappers override to apply timezone + ``input_format``.

        .. warning::

            The default identity implementation is only correct when
            :meth:`decode_downstream` also uses the identity default (str in,
            str out). See :meth:`decode_downstream` for the consequences of
            overriding only one side of the pair.
        """
        return decoded

    def serialize(self) -> dict[str, Any]:
        return {}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionMapper:
        return cls()


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
        decode_overridden = type(upstream_mapper).decode_downstream is not PartitionMapper.decode_downstream
        if not decode_overridden and window.expected_decoded_type is not str:
            raise TypeError(
                f"{type(window).__name__} expects decoded values of type "
                f"{window.expected_decoded_type.__name__!r}, but "
                f"{type(upstream_mapper).__name__} does not override "
                f"'decode_downstream' (base default returns str). Pair the window "
                f"with an upstream mapper that decodes to "
                f"{window.expected_decoded_type.__name__}, or use a window whose "
                f"'expected_decoded_type' accepts str."
            )
        self.upstream_mapper = upstream_mapper
        self.window = window

    def to_downstream(self, key: str) -> str | Iterable[str]:
        return self.upstream_mapper.to_downstream(key)

    def to_upstream(self, downstream_key: str) -> frozenset[str]:
        """Return the complete set of upstream partition keys required for *downstream_key*."""
        decoded = self.upstream_mapper.decode_downstream(downstream_key)
        return frozenset(
            self.upstream_mapper.encode_upstream(expected_upstream)
            for expected_upstream in self.window.to_upstream(decoded)
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_partition_mapper, encode_window

        return {
            "upstream_mapper": encode_partition_mapper(self.upstream_mapper),
            "window": encode_window(self.window),
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionMapper:
        from airflow.serialization.decoders import decode_partition_mapper, decode_window

        return cls(
            upstream_mapper=decode_partition_mapper(data["upstream_mapper"]),
            window=decode_window(data["window"]),
        )


def is_rollup(mapper: PartitionMapper) -> TypeGuard[RollupMapper]:
    """Return ``True`` if *mapper* is a rollup mapper, narrowing the type to :class:`RollupMapper`."""
    return mapper.is_rollup
