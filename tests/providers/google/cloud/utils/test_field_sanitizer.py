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

import unittest
from copy import deepcopy

import pytest

from airflow.providers.google.cloud.utils.field_sanitizer import GcpBodyFieldSanitizer


class TestGcpBodyFieldSanitizer(unittest.TestCase):
    def test_sanitize_should_sanitize_empty_body_and_fields(self):
        body = {}
        fields_to_sanitize = []

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert {} == body

    def test_sanitize_should_not_fail_with_none_body(self):
        body = None
        fields_to_sanitize = []

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert body is None

    def test_sanitize_should_fail_with_none_fields(self):
        body = {}
        fields_to_sanitize = None

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)

        with pytest.raises(TypeError):
            sanitizer.sanitize(body)

    def test_sanitize_should_not_fail_if_field_is_absent_in_body(self):
        body = {}
        fields_to_sanitize = ["kind"]

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert {} == body

    def test_sanitize_should_not_remove_fields_for_incorrect_specification(self):
        actual_body = [
            {"kind": "compute#instanceTemplate", "name": "instance"},
            {"kind": "compute#instanceTemplate1", "name": "instance1"},
            {"kind": "compute#instanceTemplate2", "name": "instance2"},
        ]
        body = deepcopy(actual_body)
        fields_to_sanitize = ["kind"]

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert actual_body == body

    def test_sanitize_should_remove_all_fields_from_root_level(self):
        body = {"kind": "compute#instanceTemplate", "name": "instance"}
        fields_to_sanitize = ["kind"]

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert {"name": "instance"} == body

    def test_sanitize_should_remove_for_multiple_fields_from_root_level(self):
        body = {"kind": "compute#instanceTemplate", "name": "instance"}
        fields_to_sanitize = ["kind", "name"]

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert {} == body

    def test_sanitize_should_remove_all_fields_in_a_list_value(self):
        body = {
            "fields": [
                {"kind": "compute#instanceTemplate", "name": "instance"},
                {"kind": "compute#instanceTemplate1", "name": "instance1"},
                {"kind": "compute#instanceTemplate2", "name": "instance2"},
            ]
        }
        fields_to_sanitize = ["fields.kind"]

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert {
            "fields": [
                {"name": "instance"},
                {"name": "instance1"},
                {"name": "instance2"},
            ]
        } == body

    def test_sanitize_should_remove_all_fields_in_any_nested_body(self):
        fields_to_sanitize = [
            "kind",
            "properties.disks.kind",
            "properties.metadata.kind",
        ]

        body = {
            "kind": "compute#instanceTemplate",
            "name": "instance",
            "properties": {
                "disks": [
                    {
                        "name": "a",
                        "kind": "compute#attachedDisk",
                        "type": "PERSISTENT",
                        "mode": "READ_WRITE",
                    },
                    {
                        "name": "b",
                        "kind": "compute#attachedDisk",
                        "type": "PERSISTENT",
                        "mode": "READ_WRITE",
                    },
                ],
                "metadata": {"kind": "compute#metadata", "fingerprint": "GDPUYxlwHe4="},
            },
        }
        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert {
            "name": "instance",
            "properties": {
                "disks": [
                    {"name": "a", "type": "PERSISTENT", "mode": "READ_WRITE"},
                    {"name": "b", "type": "PERSISTENT", "mode": "READ_WRITE"},
                ],
                "metadata": {"fingerprint": "GDPUYxlwHe4="},
            },
        } == body

    def test_sanitize_should_not_fail_if_specification_has_none_value(self):
        fields_to_sanitize = [
            "kind",
            "properties.disks.kind",
            "properties.metadata.kind",
        ]

        body = {"kind": "compute#instanceTemplate", "name": "instance", "properties": {"disks": None}}

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert {"name": "instance", "properties": {"disks": None}} == body

    def test_sanitize_should_not_fail_if_no_specification_matches(self):
        fields_to_sanitize = [
            "properties.disks.kind1",
            "properties.metadata.kind2",
        ]

        body = {"name": "instance", "properties": {"disks": None}}

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert {"name": "instance", "properties": {"disks": None}} == body

    def test_sanitize_should_not_fail_if_type_in_body_do_not_match_with_specification(self):
        fields_to_sanitize = [
            "properties.disks.kind",
            "properties.metadata.kind2",
        ]

        body = {"name": "instance", "properties": {"disks": 1}}

        sanitizer = GcpBodyFieldSanitizer(fields_to_sanitize)
        sanitizer.sanitize(body)

        assert {"name": "instance", "properties": {"disks": 1}} == body
