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

import sys
import types
from unittest.mock import MagicMock

import pytest

from airflowctl.ctl import cli_parser
from airflowctl.ctl.cli_config import (
    ARG_AUTH_TOKEN,
    ActionCommand,
    Arg,
    GroupCommand,
    add_auth_token_to_all_commands,
)


def _make_action(name: str = "ping") -> ActionCommand:
    return ActionCommand(
        name=name,
        help="ping",
        func=lambda args: None,
        args=(Arg(flags=("--echo",), help="echo"),),
    )


def _fake_provider_entry_point(name: str, get_provider_info_fn):
    """Build a fake ``apache_airflow_provider`` entry point."""
    ep = MagicMock()
    ep.name = name
    ep.value = f"airflow.providers.{name}.get_provider_info:get_provider_info"
    ep.load.return_value = get_provider_info_fn
    return ep


def _install_factory_module(monkeypatch, dotted_path: str, factory):
    """Install ``factory`` so ``import_string(dotted_path)`` resolves to it.

    ``import_string("a.b.c.factory")`` runs ``import_module("a.b.c")`` then
    ``getattr(mod, "factory")``, so we just need ``a.b.c`` registered in
    ``sys.modules`` with a ``factory`` attribute.
    """
    module_path, _, attr = dotted_path.rpartition(".")
    parts = module_path.split(".")
    # Build/patch every prefix module so the import chain resolves.
    for i in range(1, len(parts) + 1):
        prefix = ".".join(parts[:i])
        if prefix not in sys.modules:
            mod = types.ModuleType(prefix)
            monkeypatch.setitem(sys.modules, prefix, mod)
    setattr(sys.modules[module_path], attr, factory)


@pytest.fixture
def patched_provider_discovery(monkeypatch):
    """Patch the provider entry-point lookup and install a logger spy."""
    import importlib.metadata as md

    holder: dict = {"eps": [], "warnings": []}

    def fake_entry_points(group: str):
        if group == cli_parser.APACHE_AIRFLOW_PROVIDER_ENTRY_POINT_GROUP:
            return holder["eps"]
        return []

    fake_log = MagicMock()
    fake_log.warning.side_effect = lambda *a, **kw: holder["warnings"].append((a, kw))

    monkeypatch.setattr(md, "entry_points", fake_entry_points)
    monkeypatch.setattr(cli_parser, "log", fake_log)
    return holder


def test_discover_returns_empty_when_no_providers(patched_provider_discovery):
    assert cli_parser._discover_provider_commands() == []
    assert patched_provider_discovery["warnings"] == []


def test_discover_skips_provider_without_ctl_key(patched_provider_discovery):
    patched_provider_discovery["eps"] = [
        _fake_provider_entry_point("nofab", lambda: {"package-name": "x"}),
    ]
    assert cli_parser._discover_provider_commands() == []
    assert patched_provider_discovery["warnings"] == []


def test_discover_returns_commands_from_ctl_factory(patched_provider_discovery, monkeypatch):
    action = _make_action(name="echo")
    group = GroupCommand(name="myprov", help="Provider commands", subcommands=(action,))

    factory_path = "airflow.providers.myprov.airflowctl_commands.definition.get_myprov_airflowctl_commands"
    _install_factory_module(monkeypatch, factory_path, lambda: [group])

    patched_provider_discovery["eps"] = [
        _fake_provider_entry_point("myprov", lambda: {"ctl": [factory_path]}),
    ]

    discovered = cli_parser._discover_provider_commands()
    assert len(discovered) == 1
    assert discovered[0].name == "myprov"
    assert discovered[0].subcommands == (action,)
    assert patched_provider_discovery["warnings"] == []


def test_discover_skips_factory_returning_non_list(patched_provider_discovery, monkeypatch):
    factory_path = "airflow.providers.bad.airflowctl_commands.definition.bad_factory"
    _install_factory_module(monkeypatch, factory_path, lambda: "not a list")

    patched_provider_discovery["eps"] = [
        _fake_provider_entry_point("bad", lambda: {"ctl": [factory_path]}),
    ]

    discovered = cli_parser._discover_provider_commands()
    assert discovered == []
    assert any(
        "expected list[CLICommand]" in str(call_args[0])
        for call_args, _ in patched_provider_discovery["warnings"]
    )


def test_discover_swallows_factory_exceptions(patched_provider_discovery, monkeypatch):
    factory_path = "airflow.providers.broken.airflowctl_commands.definition.boom"

    def boom():
        raise RuntimeError("kaboom")

    _install_factory_module(monkeypatch, factory_path, boom)

    patched_provider_discovery["eps"] = [
        _fake_provider_entry_point("broken", lambda: {"ctl": [factory_path]}),
    ]

    discovered = cli_parser._discover_provider_commands()
    assert discovered == []
    assert len(patched_provider_discovery["warnings"]) == 1


def test_discover_continues_after_one_broken_provider(patched_provider_discovery, monkeypatch):
    good_action = _make_action(name="ok")
    good_group = GroupCommand(name="good", help="ok", subcommands=(good_action,))

    good_path = "airflow.providers.good.airflowctl_commands.definition.good_factory"
    _install_factory_module(monkeypatch, good_path, lambda: [good_group])

    bad_path = "airflow.providers.broken.airflowctl_commands.definition.boom"

    def boom():
        raise RuntimeError("kaboom")

    _install_factory_module(monkeypatch, bad_path, boom)

    patched_provider_discovery["eps"] = [
        _fake_provider_entry_point("broken", lambda: {"ctl": [bad_path]}),
        _fake_provider_entry_point("good", lambda: {"ctl": [good_path]}),
    ]

    discovered = cli_parser._discover_provider_commands()
    assert [c.name for c in discovered] == ["good"]
    assert len(patched_provider_discovery["warnings"]) == 1


def test_discover_handles_get_provider_info_failure(patched_provider_discovery):
    def boom():
        raise RuntimeError("info failed")

    patched_provider_discovery["eps"] = [_fake_provider_entry_point("brokeninfo", boom)]
    discovered = cli_parser._discover_provider_commands()
    assert discovered == []
    assert len(patched_provider_discovery["warnings"]) == 1


def test_discover_skips_provider_with_non_dict_info(patched_provider_discovery):
    patched_provider_discovery["eps"] = [_fake_provider_entry_point("weird", lambda: ["not", "a", "dict"])]
    discovered = cli_parser._discover_provider_commands()
    assert discovered == []
    assert len(patched_provider_discovery["warnings"]) == 1


def test_provider_action_commands_get_auth_token_arg(patched_provider_discovery, monkeypatch):
    """The parser-level merge applies ``add_auth_token_to_all_commands`` to provider commands."""
    bare_action = ActionCommand(
        name="solo",
        help="solo",
        func=lambda args: None,
        args=(Arg(flags=("--x",), help="x"),),
    )
    factory_path = "airflow.providers.solo.airflowctl_commands.definition.solo_factory"
    _install_factory_module(monkeypatch, factory_path, lambda: [bare_action])

    patched_provider_discovery["eps"] = [
        _fake_provider_entry_point("solo", lambda: {"ctl": [factory_path]}),
    ]

    decorated = add_auth_token_to_all_commands(cli_parser._discover_provider_commands())
    assert ARG_AUTH_TOKEN in decorated[0].args
