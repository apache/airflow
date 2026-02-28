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

from unittest.mock import Mock

import pytest
import yaml as pyyaml

from airflowctl.ctl.utils import yaml


def test_safe_load_uses_csafe_loader_when_available(monkeypatch):
    load_mock = Mock(return_value={"k": "v"})

    class FakeLoader:
        pass

    monkeypatch.setattr(pyyaml, "load", load_mock)
    monkeypatch.setattr(pyyaml, "CSafeLoader", FakeLoader, raising=False)

    result = yaml.safe_load("k: v")

    assert result == {"k": "v"}
    load_mock.assert_called_once_with("k: v", FakeLoader)


def test_safe_load_falls_back_to_safe_loader(monkeypatch):
    load_mock = Mock(return_value={"k": "v"})

    class FakeLoader:
        pass

    monkeypatch.setattr(pyyaml, "load", load_mock)
    monkeypatch.delattr(pyyaml, "CSafeLoader", raising=False)
    monkeypatch.setattr(pyyaml, "SafeLoader", FakeLoader, raising=False)

    result = yaml.safe_load("k: v")

    assert result == {"k": "v"}
    load_mock.assert_called_once_with("k: v", FakeLoader)


def test_dump_uses_csafe_dumper_when_available(monkeypatch):
    dump_mock = Mock(return_value="k: v\n")

    class FakeDumper:
        pass

    monkeypatch.setattr(pyyaml, "dump", dump_mock)
    monkeypatch.setattr(pyyaml, "CSafeDumper", FakeDumper, raising=False)

    result = yaml.dump({"k": "v"}, default_flow_style=False)

    assert result == "k: v\n"
    dump_mock.assert_called_once_with({"k": "v"}, Dumper=FakeDumper, default_flow_style=False)


def test_dump_falls_back_to_safe_dumper(monkeypatch):
    dump_mock = Mock(return_value="k: v\n")

    class FakeDumper:
        pass

    monkeypatch.setattr(pyyaml, "dump", dump_mock)
    monkeypatch.delattr(pyyaml, "CSafeDumper", raising=False)
    monkeypatch.setattr(pyyaml, "SafeDumper", FakeDumper, raising=False)

    result = yaml.dump({"k": "v"}, default_flow_style=False)

    assert result == "k: v\n"
    dump_mock.assert_called_once_with({"k": "v"}, Dumper=FakeDumper, default_flow_style=False)


def test_getattr_delegates_to_pyyaml():
    assert yaml.safe_dump is pyyaml.safe_dump


def test_getattr_full_loader_path():
    assert yaml.FullLoader is pyyaml.FullLoader


def test_getattr_raises_attribute_error_for_unknown_name():
    with pytest.raises(AttributeError):
        yaml.does_not_exist
