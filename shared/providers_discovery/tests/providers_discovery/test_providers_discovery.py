#
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

from email.message import Message
from unittest.mock import Mock

import jsonschema
import pytest

from airflow_shared.providers_discovery import (
    LazyDictWithCache,
    discover_all_providers_from_packages,
    providers_discovery as providers_discovery_module,
)


@pytest.mark.parametrize(
    ("value", "expected_outputs"),
    [
        ("a", "a"),
        (1, 1),
        (None, None),
        (lambda: 0, 0),
        (lambda: None, None),
        (lambda: "z", "z"),
    ],
)
def test_lazy_cache_dict_resolving(value, expected_outputs):
    lazy_cache_dict = LazyDictWithCache()
    lazy_cache_dict["key"] = value
    assert lazy_cache_dict["key"] == expected_outputs
    # Retrieve it again to see if it is correctly returned again
    assert lazy_cache_dict["key"] == expected_outputs


def test_lazy_cache_dict_raises_error():
    def raise_method():
        raise RuntimeError("test")

    lazy_cache_dict = LazyDictWithCache()
    lazy_cache_dict["key"] = raise_method
    with pytest.raises(RuntimeError, match="test"):
        _ = lazy_cache_dict["key"]


def test_lazy_cache_dict_del_item():
    lazy_cache_dict = LazyDictWithCache()

    def answer():
        return 42

    lazy_cache_dict["spam"] = answer
    assert "spam" in lazy_cache_dict._raw_dict
    assert "spam" not in lazy_cache_dict._resolved  # Not resoled yet
    assert lazy_cache_dict["spam"] == 42
    assert "spam" in lazy_cache_dict._resolved
    del lazy_cache_dict["spam"]
    assert "spam" not in lazy_cache_dict._raw_dict
    assert "spam" not in lazy_cache_dict._resolved

    lazy_cache_dict["foo"] = answer
    assert lazy_cache_dict["foo"] == 42
    assert "foo" in lazy_cache_dict._resolved
    # Emulate some mess in data, e.g. value from `_raw_dict` deleted but not from `_resolved`
    del lazy_cache_dict._raw_dict["foo"]
    assert "foo" in lazy_cache_dict._resolved
    with pytest.raises(KeyError):
        # Error expected here, but we still expect to remove also record into `resolved`
        del lazy_cache_dict["foo"]
    assert "foo" not in lazy_cache_dict._resolved

    lazy_cache_dict["baz"] = answer
    # Key in `_resolved` not created yet
    assert "baz" in lazy_cache_dict._raw_dict
    assert "baz" not in lazy_cache_dict._resolved
    del lazy_cache_dict._raw_dict["baz"]
    assert "baz" not in lazy_cache_dict._raw_dict
    assert "baz" not in lazy_cache_dict._resolved


def test_lazy_cache_dict_clear():
    def answer():
        return 42

    lazy_cache_dict = LazyDictWithCache()
    assert len(lazy_cache_dict) == 0
    lazy_cache_dict["spam"] = answer
    lazy_cache_dict["foo"] = answer
    lazy_cache_dict["baz"] = answer

    assert len(lazy_cache_dict) == 3
    assert len(lazy_cache_dict._raw_dict) == 3
    assert not lazy_cache_dict._resolved
    assert lazy_cache_dict["spam"] == 42
    assert len(lazy_cache_dict._resolved) == 1
    # Emulate some mess in data, contain some data into the `_resolved`
    lazy_cache_dict._resolved.add("biz")
    assert len(lazy_cache_dict) == 3
    assert len(lazy_cache_dict._resolved) == 2
    # And finally cleanup everything
    lazy_cache_dict.clear()
    assert len(lazy_cache_dict) == 0
    assert not lazy_cache_dict._raw_dict
    assert not lazy_cache_dict._resolved


def test_discover_all_providers_preserves_commas_in_project_url(monkeypatch):
    provider_info = {
        "package-name": "apache-airflow-providers-test",
        "name": "Test",
        "description": "Test provider",
        "versions": ["1.0.0"],
    }
    metadata = Message()
    metadata["Name"] = "apache-airflow-providers-test"
    metadata["Project-URL"] = " Documentation , https://example.invalid/docs?query=a,b "
    dist = Mock(metadata=metadata, version="1.0.0")
    entry_point = Mock()
    entry_point.load.return_value = lambda: provider_info
    monkeypatch.setattr(
        providers_discovery_module,
        "entry_points_with_dist",
        lambda group: [(entry_point, dist)],
    )

    providers = {}
    discover_all_providers_from_packages(providers, Mock())

    assert providers["apache-airflow-providers-test"].data["documentation-url"] == (
        "https://example.invalid/docs?query=a,b"
    )


def _provider_entry(package_name, provider_info):
    metadata = Message()
    metadata["Name"] = package_name
    dist = Mock(metadata=metadata, version="1.0.0")
    entry_point = Mock()
    entry_point.load.return_value = lambda: provider_info
    return entry_point, dist


def _valid_provider_info(package_name):
    return {"package-name": package_name, "name": package_name, "description": package_name}


class TestDiscoveryIsolatesABadProvider:
    """
    ``provider_schema_validator.validate()`` used to be called unwrapped, so one provider with
    malformed metadata aborted discovery for every provider in the process: everything after it
    in entry-point order was never registered, and ``initialize_providers_list()`` raised on
    that and every later call.

    These drive the real schema validator rather than a mock, so they cannot be satisfied by
    catching an exception type that jsonschema never raises.
    """

    # Mirrors the parts of provider_info.schema.json these tests depend on: 'package-name' is
    # not required, and `triggers` items are closed with additionalProperties: false. Defined
    # here rather than loaded from the real schema because that load imports airflow, which
    # this distribution's own test environment deliberately does not install.
    SCHEMA = {
        "type": "object",
        "properties": {
            "package-name": {"type": "string"},
            "name": {"type": "string"},
            "description": {"type": "string"},
            "triggers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "integration-name": {"type": "string"},
                        "python-modules": {"type": "array", "items": {"type": "string"}},
                    },
                    "additionalProperties": False,
                },
            },
        },
        "required": ["name", "description"],
    }

    @classmethod
    def _real_validator(cls):
        return jsonschema.validators.validator_for(cls.SCHEMA)(cls.SCHEMA)

    @classmethod
    def _discover(cls, entries):
        monkeypatched = pytest.MonkeyPatch()
        monkeypatched.setattr(providers_discovery_module, "entry_points_with_dist", lambda group: entries)
        providers: dict = {}
        try:
            discover_all_providers_from_packages(providers, cls._real_validator())
        finally:
            monkeypatched.undo()
        return providers

    def test_the_fixture_schema_matches_the_assumptions_these_tests_rely_on(self):
        validator = self._real_validator()
        # 'package-name' absent still validates, which is what makes the guard below reachable.
        validator.validate({"name": "n", "description": "d"})
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(
                {
                    "name": "n",
                    "description": "d",
                    "triggers": [{"integration-name": "x", "unrecognised-key": "v"}],
                }
            )

    def test_schema_invalid_provider_is_skipped_and_the_others_still_load(self):
        # An unrecognised key inside a `triggers` item is one of the few substructures the
        # runtime schema closes with additionalProperties: false, so this really is invalid.
        bad_info = _valid_provider_info("apache-airflow-providers-bad")
        bad_info["triggers"] = [{"integration-name": "x", "python-modules": ["m"], "unrecognised-key": "v"}]

        providers = self._discover(
            [
                _provider_entry(
                    "apache-airflow-providers-first",
                    _valid_provider_info("apache-airflow-providers-first"),
                ),
                _provider_entry("apache-airflow-providers-bad", bad_info),
                _provider_entry(
                    "apache-airflow-providers-last",
                    _valid_provider_info("apache-airflow-providers-last"),
                ),
            ]
        )

        assert sorted(providers) == [
            "apache-airflow-providers-first",
            "apache-airflow-providers-last",
        ]

    def test_provider_without_a_package_name_is_skipped(self):
        # 'package-name' is not in the runtime schema's required list, so this validates and
        # would otherwise reach an unguarded subscript.
        no_name = {"name": "nameless", "description": "nameless"}

        providers = self._discover(
            [
                _provider_entry("apache-airflow-providers-nameless", no_name),
                _provider_entry(
                    "apache-airflow-providers-last",
                    _valid_provider_info("apache-airflow-providers-last"),
                ),
            ]
        )

        assert sorted(providers) == ["apache-airflow-providers-last"]

    def test_a_broken_schema_is_not_reported_as_every_provider_being_malformed(self):
        # A validator-side defect must stay loud instead of being swallowed once per provider.
        validator = Mock()
        validator.validate.side_effect = jsonschema.exceptions.SchemaError("bad schema")
        monkeypatched = pytest.MonkeyPatch()
        monkeypatched.setattr(
            providers_discovery_module,
            "entry_points_with_dist",
            lambda group: [
                _provider_entry(
                    "apache-airflow-providers-first",
                    _valid_provider_info("apache-airflow-providers-first"),
                )
            ],
        )
        try:
            with pytest.raises(jsonschema.exceptions.SchemaError):
                discover_all_providers_from_packages({}, validator)
        finally:
            monkeypatched.undo()
