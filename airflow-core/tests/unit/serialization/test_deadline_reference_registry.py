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

import pytest

from airflow import plugins_manager
from airflow.models.deadline import ReferenceModels
from airflow.serialization.definitions.deadline import SerializedReferenceModels
from airflow.serialization.helpers import (
    DeadlineReferenceNotRegistered,
    find_registered_custom_deadline_reference,
)


class _RegisteredCustomReference(ReferenceModels.BaseDeadlineReference):
    """Fake deadline reference registered through a plugin in these tests."""

    required_kwargs: set[str] = set()

    @classmethod
    def deserialize_reference(cls, reference_data: dict):
        return cls()

    def serialize_reference(self) -> dict:
        return {}

    def _evaluate_with(self, *, session, **kwargs):
        return None


_IMPORTABLE = f"{_RegisteredCustomReference.__module__}._RegisteredCustomReference"


@pytest.fixture
def fake_plugin_registry(monkeypatch):
    """Stub `get_deadline_references_plugins` to advertise a single registered class."""
    registered = {_IMPORTABLE: _RegisteredCustomReference}
    monkeypatch.setattr(
        plugins_manager,
        "get_deadline_references_plugins",
        lambda: registered,
    )
    return registered


def test_find_registered_returns_class(fake_plugin_registry):
    assert find_registered_custom_deadline_reference(_IMPORTABLE) is _RegisteredCustomReference


def test_find_registered_raises_for_unknown(fake_plugin_registry):
    with pytest.raises(DeadlineReferenceNotRegistered) as exc_info:
        find_registered_custom_deadline_reference("not.registered.SomeReference")
    assert exc_info.value.type_string == "not.registered.SomeReference"
    assert "not.registered.SomeReference" in str(exc_info.value)


def test_find_registered_raises_when_registry_empty(monkeypatch):
    monkeypatch.setattr(
        plugins_manager,
        "get_deadline_references_plugins",
        lambda: {},
    )
    with pytest.raises(DeadlineReferenceNotRegistered):
        find_registered_custom_deadline_reference("anything.at.all.MyReference")


def test_serialized_custom_reference_uses_registry(fake_plugin_registry):
    result = SerializedReferenceModels.SerializedCustomReference.deserialize_reference(
        {"__class_path": _IMPORTABLE}
    )

    assert isinstance(result, SerializedReferenceModels.SerializedCustomReference)
    assert isinstance(result.inner_ref, _RegisteredCustomReference)


def test_serialized_custom_reference_rejects_unregistered(monkeypatch):
    monkeypatch.setattr(
        plugins_manager,
        "get_deadline_references_plugins",
        lambda: {},
    )
    with pytest.raises(DeadlineReferenceNotRegistered):
        SerializedReferenceModels.SerializedCustomReference.deserialize_reference(
            {"__class_path": "some.other.module.UnregisteredReference"}
        )


@pytest.mark.parametrize(
    "reference_data",
    [
        pytest.param({"reference_type": "TotallyUnknownReference"}, id="unknown_type_no_class_path"),
        pytest.param({"reference_type": "X", "__class_path": ""}, id="empty_class_path"),
    ],
)
def test_serialized_custom_reference_missing_class_path_raises_clear_error(reference_data):
    """A reference routed to SerializedCustomReference but lacking a usable ``__class_path``
    (corrupt / hand-edited row, blob from a newer version, or a custom ref whose plugin is gone)
    must raise a clear ValueError — NOT a bare ``KeyError: '__class_path'``."""
    with pytest.raises(ValueError, match="unrecognized reference_type"):
        SerializedReferenceModels.SerializedCustomReference.deserialize_reference(reference_data)


class FixedDatetimeDeadline(ReferenceModels.BaseDeadlineReference):
    """Custom reference whose class name deliberately collides with a builtin reference name."""

    required_kwargs: set[str] = set()

    def serialize_reference(self) -> dict:
        return {"reference_type": self.reference_name, "marker": "i-am-custom"}

    def _evaluate_with(self, *, session, **kwargs):
        raise AssertionError("custom evaluate should not be exercised in this test")


class DagRunLogicalDateDeadline(ReferenceModels.BaseDeadlineReference):
    """Custom reference colliding with a builtin that has no required deserialize fields."""

    required_kwargs: set[str] = set()

    def serialize_reference(self) -> dict:
        return {"reference_type": self.reference_name}

    def _evaluate_with(self, *, session, **kwargs):
        raise AssertionError("custom evaluate should not be exercised in this test")


_COLLIDING_REFS = {
    f"{FixedDatetimeDeadline.__module__}.FixedDatetimeDeadline": FixedDatetimeDeadline,
    f"{DagRunLogicalDateDeadline.__module__}.DagRunLogicalDateDeadline": DagRunLogicalDateDeadline,
}


@pytest.fixture
def colliding_plugin_registry(monkeypatch):
    """Advertise custom references whose names collide with builtin reference names."""
    monkeypatch.setattr(
        plugins_manager,
        "get_deadline_references_plugins",
        lambda: _COLLIDING_REFS,
    )
    return _COLLIDING_REFS


@pytest.mark.parametrize(
    "custom_cls",
    [FixedDatetimeDeadline],
)
def test_custom_reference_name_collision_routes_to_custom(colliding_plugin_registry, custom_cls):
    """
    A custom reference whose class name collides with a builtin must round-trip as the custom
    class, not silently decode as the builtin (which loses the user's evaluation logic or raises
    a spurious KeyError on builtin-only fields).

    Regression test: ``decode_deadline_reference`` previously routed solely by the
    ``reference_type`` name string, ignoring the authoritative ``__class_path`` key.
    """
    from airflow.serialization.decoders import decode_deadline_reference
    from airflow.serialization.encoders import encode_deadline_reference

    encoded = encode_deadline_reference(custom_cls())
    # The encoder stamps __class_path for custom references regardless of name collision.
    assert encoded["__class_path"] == f"{custom_cls.__module__}.{custom_cls.__name__}"

    decoded = decode_deadline_reference(encoded)

    assert isinstance(decoded, SerializedReferenceModels.SerializedCustomReference)
    assert isinstance(decoded.inner_ref, custom_cls)
