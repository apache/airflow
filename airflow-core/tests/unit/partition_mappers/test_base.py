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

import re
from datetime import datetime, timezone

import pytest

from airflow.partition_mappers.base import PartitionMapper, RollupMapper
from airflow.partition_mappers.fixed_key import FixedKeyMapper
from airflow.partition_mappers.identity import IdentityMapper
from airflow.partition_mappers.temporal import StartOfDayMapper
from airflow.partition_mappers.window import DayWindow, SegmentWindow
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper
from airflow.serialization.enums import Encoding


class TestCarryPartitionDate:
    def test_base_returns_none_by_default(self):
        """Non-identity mappers don't carry the producer's date; it's derived from the key instead."""
        dt = datetime(2026, 5, 20, 1, 0, 0, tzinfo=timezone.utc)
        assert StartOfDayMapper().carry_partition_date(dt) is None
        assert (
            RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow()).carry_partition_date(dt)
            is None
        )


class TestPartitionMapperInitSubclass:
    """Verify that __init_subclass__ enforces the decode/encode pair contract."""

    def test_subclass_overriding_both_decode_and_encode_is_accepted(self):
        """A subclass that overrides both sides of the pair is valid."""

        class BothSides(PartitionMapper):
            def to_downstream(self, key: str) -> str:
                return key

            def decode_downstream(self, downstream_key: str) -> object:
                return downstream_key

            def encode_upstream(self, decoded: object) -> str:
                return str(decoded)

        # No TypeError raised during class definition; instantiation also works.
        instance = BothSides()
        assert instance.decode_downstream("k") == "k"

    def test_subclass_overriding_only_decode_raises_typeerror(self):
        """A subclass that overrides only decode_downstream must raise TypeError."""
        with pytest.raises(TypeError, match="decode_downstream"):
            type(
                "OnlyDecode",
                (PartitionMapper,),
                {
                    "to_downstream": lambda self, key: key,
                    "decode_downstream": lambda self, downstream_key: downstream_key,
                },
            )

    def test_subclass_overriding_only_encode_raises_typeerror(self):
        """A subclass that overrides only encode_upstream must raise TypeError."""
        with pytest.raises(TypeError, match="encode_upstream"):
            type(
                "OnlyEncode",
                (PartitionMapper,),
                {
                    "to_downstream": lambda self, key: key,
                    "encode_upstream": lambda self, decoded: str(decoded),
                },
            )

    def test_subclass_inheriting_both_unchanged_is_accepted(self):
        """A subclass that overrides neither decode nor encode is valid (identity defaults)."""

        class NeitherSide(PartitionMapper):
            def to_downstream(self, key: str) -> str:
                return key

        # No TypeError raised; default identity implementations apply.
        assert NeitherSide().encode_upstream("x") == "x"

    def test_existing_builtin_mappers_load_without_raising(self):
        """All built-in mapper classes must load without triggering TypeError."""
        import airflow.partition_mappers

        # Importing the package is sufficient — __init_subclass__ fires at class
        # definition time, so a TypeError would surface during import, not here.
        assert airflow.partition_mappers is not None


class TestRollupMapperInit:
    """Verify RollupMapper.__init__ rejects incompatible (upstream_mapper, window) pairings."""

    def test_rejects_identity_mapper_with_temporal_window(self):
        """RollupMapper raises when paired with a mapper that leaves decode_downstream at the base identity."""
        from airflow.partition_mappers.base import RollupMapper
        from airflow.partition_mappers.window import DayWindow

        class _StringOnlyMapper(PartitionMapper):
            def to_downstream(self, key):
                return key

        with pytest.raises(TypeError, match="DayWindow expects decoded values of type 'datetime'"):
            RollupMapper(upstream_mapper=_StringOnlyMapper(), window=DayWindow())

    def test_accepts_temporal_mapper_with_temporal_window(self):
        """RollupMapper accepts the canonical (temporal mapper, temporal window) pairing."""
        from airflow.partition_mappers.base import RollupMapper
        from airflow.partition_mappers.temporal import StartOfDayMapper
        from airflow.partition_mappers.window import DayWindow

        # Should not raise.
        RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow())

    def test_accepts_string_only_window_with_identity_mapper(self):
        """RollupMapper accepts an identity-decoding mapper when paired with a window whose expected_decoded_type is str."""
        from airflow.partition_mappers.base import RollupMapper
        from airflow.partition_mappers.window import Window

        class _StringOnlyMapper(PartitionMapper):
            def to_downstream(self, key):
                return key

        class _AlphaWindow(Window):
            expected_decoded_type = str

            def to_upstream(self, decoded_downstream):
                return [decoded_downstream]

        # Should not raise.
        RollupMapper(upstream_mapper=_StringOnlyMapper(), window=_AlphaWindow())

    def test_accepts_decode_overriding_str_mapper_with_str_window(self):
        """Overriding decode/encode while still decoding to str is a legal str-window pairing.

        The guard must key off the declared ``expected_decoded_type`` (str by
        default), not off whether ``decode_downstream`` is overridden.
        """

        class _CasefoldMapper(PartitionMapper):
            def to_downstream(self, key):
                return key.casefold()

            def decode_downstream(self, downstream_key):
                return downstream_key.casefold()

            def encode_upstream(self, decoded_upstream):
                return str(decoded_upstream)

        # Should not raise.
        RollupMapper(upstream_mapper=_CasefoldMapper(), window=SegmentWindow(["us", "eu"]))

    @pytest.mark.parametrize(
        ("upstream_mapper_factory", "window_factory", "match"),
        [
            pytest.param(
                lambda: FixedKeyMapper("all"),
                lambda: SegmentWindow(["us", "eu"]),
                None,
                id="str_mapper-str_window-valid",
            ),
            pytest.param(
                StartOfDayMapper,
                DayWindow,
                None,
                id="datetime_mapper-datetime_window-valid",
            ),
            pytest.param(
                lambda: FixedKeyMapper("all"),
                DayWindow,
                "DayWindow expects decoded values of type 'datetime'",
                id="str_mapper-datetime_window-invalid",
            ),
            pytest.param(
                StartOfDayMapper,
                lambda: SegmentWindow(["us"]),
                "SegmentWindow expects decoded values of type 'str'",
                id="datetime_mapper-str_window-invalid",
            ),
        ],
    )
    def test_upstream_mapper_window_type_pairing(self, upstream_mapper_factory, window_factory, match):
        """RollupMapper's guard must catch a decoded-type mismatch in either direction.

        Guards the ``datetime_mapper-str_window`` direction in particular: left
        unchecked, that pairing survives construction and serialization, then
        raises ``AttributeError`` inside ``to_upstream`` at scheduler tick time.
        """
        if match is None:
            # Should not raise.
            RollupMapper(upstream_mapper=upstream_mapper_factory(), window=window_factory())
        else:
            with pytest.raises(TypeError, match=match):
                RollupMapper(upstream_mapper=upstream_mapper_factory(), window=window_factory())


class TestPartitionMapperMaxDownstreamKeysValidator:
    """Verify the max_downstream_keys validator on the PartitionMapper base class.

    Uses IdentityMapper as the most lightweight concrete subclass — the
    validator lives on the base class so any subclass exercises it.
    """

    def test_max_downstream_keys_none_is_accepted(self):
        """Default (None) leaves max_downstream_keys as None."""
        mapper = IdentityMapper()
        assert mapper.max_downstream_keys is None

    def test_max_downstream_keys_one_is_accepted(self):
        """Minimum positive integer value is accepted."""
        mapper = IdentityMapper(max_downstream_keys=1)
        assert mapper.max_downstream_keys == 1

    @pytest.mark.parametrize(
        "bad_value",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
            pytest.param(1.0, id="float"),
            pytest.param("5", id="string"),
        ],
    )
    def test_max_downstream_keys_invalid_raises(self, bad_value):
        """Reject non-positive-integer values with the full validator message."""
        with pytest.raises(
            ValueError,
            match=re.escape(f"max_downstream_keys must be a positive integer or None, got {bad_value!r}"),
        ):
            IdentityMapper(max_downstream_keys=bad_value)

    def test_subclass_skipping_base_init_still_resolves_to_none(self):
        """A subclass whose __init__ never calls the base __init__ (e.g. an
        attrs-generated one in a plugin) must still resolve max_downstream_keys
        via the class-level default instead of raising AttributeError."""

        class NoSuperMapper(PartitionMapper):
            def __init__(self):
                pass

            def to_downstream(self, key: str) -> str:
                return key

        assert NoSuperMapper().max_downstream_keys is None


class TestRollupMapperMaxDownstreamKeys:
    def test_max_downstream_keys_encode_decode_roundtrip(self):
        mapper = RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow(), max_downstream_keys=5)
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert restored.max_downstream_keys == 5

    def test_max_downstream_keys_absent_from_default_encoded_payload(self):
        mapper = RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow())
        encoded_var = encode_partition_mapper(mapper)[Encoding.VAR]
        assert "max_downstream_keys" not in encoded_var
