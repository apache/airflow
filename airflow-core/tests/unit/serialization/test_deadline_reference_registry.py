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
