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

import zipfile

import pytest
from task_sdk.coordinators.java._jar_test_utils import make_jar, write_manifest

from airflow.sdk.coordinators.java._jar_manifest import parse_main_attributes, read_main_attributes

LONG_VALUE = "META-INF/airflow/dag-code/org/apache/airflow/example/nativedag/InterfaceExample.java"


class TestParseMainAttributes:
    @pytest.mark.parametrize("newline", [b"\r\n", b"\n", b"\r"], ids=["crlf", "lf", "cr"])
    def test_splits_on_every_line_ending(self, newline):
        data = newline.join([b"Manifest-Version: 1.0", b"Main-Class: com.example.Main", b"", b""])

        assert parse_main_attributes(data) == {"manifest-version": "1.0", "main-class": "com.example.Main"}

    @pytest.mark.parametrize("newline", [b"\r\n", b"\n"], ids=["crlf", "lf"])
    def test_unfolds_a_value_folded_once_and_twice(self, newline):
        twice = "com.example." + "x" * 150
        data = write_manifest({"Airflow-Java-SDK-Dag-Code": LONG_VALUE, "Main-Class": twice}, newline=newline)
        assert data.count(newline + b" ") == 3

        attributes = parse_main_attributes(data)

        assert attributes["airflow-java-sdk-dag-code"] == LONG_VALUE
        assert attributes["main-class"] == twice

    def test_decodes_a_character_the_fold_splits(self):
        # "Main-Class: " is 12 bytes, so the two-byte "é" straddles the 72-byte fold.
        value = "c" * 59 + "é" + "d" * 10
        data = write_manifest({"Main-Class": value})
        assert "é".encode() not in data

        assert parse_main_attributes(data)["main-class"] == value

    def test_stops_at_the_end_of_the_main_section(self):
        data = (
            b"Main-Class: com.example.Main\r\n\r\nName: com/example/Main.class\r\nMain-Class: other.Main\r\n"
        )

        assert parse_main_attributes(data) == {"main-class": "com.example.Main"}

    def test_reads_a_manifest_without_a_trailing_newline(self):
        assert parse_main_attributes(b"Manifest-Version: 1.0\r\nMain-Class: com.example.Main") == {
            "manifest-version": "1.0",
            "main-class": "com.example.Main",
        }

    def test_names_are_case_insensitive_and_a_later_duplicate_wins(self):
        data = b"MAIN-CLASS: first.Main\r\nmain-class: second.Main\r\n"

        assert parse_main_attributes(data) == {"main-class": "second.Main"}

    def test_ignores_a_stray_continuation_and_a_line_without_a_colon(self):
        data = b" orphan\r\nnot a header\r\nMain-Class: com.example.Main\r\n"

        assert parse_main_attributes(data) == {"main-class": "com.example.Main"}

    def test_empty_manifest(self):
        assert parse_main_attributes(b"") == {}


class TestReadMainAttributes:
    def test_reads_the_manifest_entry(self, tmp_path):
        jar = make_jar(tmp_path / "app.jar", attributes={"Main-Class": "com.example.Main"})

        with zipfile.ZipFile(jar) as zf:
            assert read_main_attributes(zf) == {"main-class": "com.example.Main"}

    def test_returns_none_without_a_manifest(self, tmp_path):
        jar = make_jar(tmp_path / "lib.jar", entries={"com/example/Lib.class": b"\xca\xfe"})

        with zipfile.ZipFile(jar) as zf:
            assert read_main_attributes(zf) is None
