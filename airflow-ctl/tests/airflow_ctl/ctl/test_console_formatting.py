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
from rich.box import ASCII_DOUBLE_HEAD

from airflowctl.ctl import console_formatting


class _StdoutWithIsAtty:
    def __init__(self, is_tty: bool):
        self._is_tty = is_tty

    def isatty(self) -> bool:
        return self._is_tty


class _StdoutWithoutIsAtty:
    pass


def test_is_tty_returns_false_without_isatty(monkeypatch):
    monkeypatch.setattr(console_formatting.sys, "stdout", _StdoutWithoutIsAtty())
    assert console_formatting.is_tty() is False


@pytest.mark.parametrize("is_tty_value", [True, False])
def test_is_tty_returns_stdout_isatty_value(monkeypatch, is_tty_value):
    monkeypatch.setattr(console_formatting.sys, "stdout", _StdoutWithIsAtty(is_tty_value))
    assert console_formatting.is_tty() is is_tty_value


def test_is_data_sequence_returns_true_for_dict_sequence():
    assert console_formatting.is_data_sequence([{"a": 1}, {"b": 2}])


def test_is_data_sequence_returns_false_for_mixed_sequence():
    assert not console_formatting.is_data_sequence([{"a": 1}, 2])


def test_console_width_is_forced_when_not_tty(monkeypatch):
    monkeypatch.setattr(console_formatting, "is_tty", lambda: False)
    console = console_formatting.AirflowConsole(record=True)
    assert console._width == 200


def test_console_width_is_not_forced_when_tty(monkeypatch):
    monkeypatch.setattr(console_formatting, "is_tty", lambda: True)
    console = console_formatting.AirflowConsole(width=123, record=True)
    assert console._width == 123


@pytest.mark.parametrize(
    ("value", "output", "expected"),
    [
        ((1, 2), "table", "1,2"),
        ([1, 2], "json", ["1", "2"]),
        ({"a": 1}, "json", {"a": "1"}),
        ({"a": 1}, "table", "{'a': 1}"),
        (None, "json", None),
    ],
)
def test_normalize_data(value, output, expected):
    console = console_formatting.AirflowConsole(record=True)
    assert console._normalize_data(value, output) == expected


def test_print_as_raises_for_unknown_output():
    console = console_formatting.AirflowConsole(record=True)
    with pytest.raises(ValueError, match="Unknown formatter"):
        console.print_as([{"a": 1}], output="xml")


def test_print_as_raises_for_non_dict_without_mapper():
    console = console_formatting.AirflowConsole(record=True)
    with pytest.raises(ValueError, match="mapper"):
        console.print_as([1, 2], output="json")


def test_print_as_uses_mapper(monkeypatch):
    console = console_formatting.AirflowConsole(record=True)
    renderer_mock = Mock()
    monkeypatch.setattr(console, "print_as_json", renderer_mock)

    console.print_as([1, 2], output="json", mapper=lambda value: {"value": value})

    renderer_mock.assert_called_once_with([{"value": "1"}, {"value": "2"}])


def test_print_as_normalizes_dict_data(monkeypatch):
    console = console_formatting.AirflowConsole(record=True)
    renderer_mock = Mock()
    monkeypatch.setattr(console, "print_as_json", renderer_mock)

    console.print_as([{"a": 1, "b": None}], output="json")

    renderer_mock.assert_called_once_with([{"a": "1", "b": None}])


def test_print_as_table_prints_no_data_for_empty_input(monkeypatch):
    console = console_formatting.AirflowConsole(record=True)
    print_mock = Mock()
    monkeypatch.setattr(console, "print", print_mock)

    console.print_as_table([])

    print_mock.assert_called_once_with("No data found")


def test_print_as_plain_table_prints_no_data_for_empty_input(monkeypatch):
    console = console_formatting.AirflowConsole(record=True)
    print_mock = Mock()
    monkeypatch.setattr(console, "print", print_mock)

    console.print_as_plain_table([])

    print_mock.assert_called_once_with("No data found")


def test_print_as_plain_table_prints_headers_and_values():
    console = console_formatting.AirflowConsole(record=True)
    console.print_as_plain_table([{"name": "alpha", "state": "ok"}])

    output = console.export_text()
    assert "name" in output
    assert "state" in output
    assert "alpha" in output
    assert "ok" in output


def test_simple_table_has_expected_defaults():
    table = console_formatting.SimpleTable()

    assert table.show_edge is False
    assert table.pad_edge is False
    assert table.box == ASCII_DOUBLE_HEAD
    assert table.show_header is False
    assert table.title_style == "bold green"
    assert table.title_justify == "left"
    assert table.caption == " "


def test_simple_table_add_column_smoke():
    table = console_formatting.SimpleTable()
    table.add_column("column_1")

    assert table.columns[0].header == "column_1"
