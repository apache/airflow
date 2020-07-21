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

import unittest

import pytest

from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.json import deserialize, serialize


class TestSerialize(unittest.TestCase):

    def test_none(self):
        assert serialize(None) is None

    def test_integer(self):
        assert serialize(1) == 1
        assert serialize(300) == 300

    def test_boolean(self):
        assert serialize(True)
        assert not serialize(False)

    def test_string(self):
        assert serialize("string") == "string"

    def test_float(self):
        assert serialize(0.1) == 0.1

    def test_list_of_primitives(self):
        assert serialize([1, "a", True]) == [1, "a", True]

    def test_list_of_sets(self):
        values = [
            {1},
            {2}
        ]
        expected_serialization = [
            {
                Encoding.VAR: [1],
                Encoding.TYPE: DAT.SET
            },
            {
                Encoding.VAR: [2],
                Encoding.TYPE: DAT.SET
            },
        ]
        assert serialize(values) == expected_serialization

    def test_dictionary_of_primitives(self):
        value = {"key": "value"}
        expected_serialization = {
            Encoding.VAR: {"key": "value"},
            Encoding.TYPE: DAT.DICT
        }
        assert serialize(value) == expected_serialization

    def test_dictionary_of_list_of_sets(self):
        value = {
            "key_to_list": [
                set(["a"]),
                set(["b"])
            ]
        }
        expected_serialization = {
            Encoding.VAR: {
                "key_to_list": [
                    {
                        Encoding.VAR: ["a"],
                        Encoding.TYPE: DAT.SET
                    },
                    {
                        Encoding.VAR: ["b"],
                        Encoding.TYPE: DAT.SET
                    }
                ]
            },
            Encoding.TYPE: DAT.DICT
        }
        assert serialize(value) == expected_serialization

    def test_set(self):
        value = {1, 2, 3}
        expected_serialization = {
            Encoding.VAR: [1, 2, 3],
            Encoding.TYPE: DAT.SET
        }
        assert serialize(value) == expected_serialization

    def test_tuple(self):
        value = (1, 2, 3)
        expected_serialization = {
            Encoding.VAR: [1, 2, 3],
            Encoding.TYPE: DAT.TUPLE
        }
        assert serialize(value) == expected_serialization

    def test_enum_raises_exception(self):
        value = Encoding.VAR
        with pytest.raises(TypeError) as err:
            serialize(value)
        assert err.value.args[0] == "Unable to serialize <enum 'Encoding'>"


class TestDeserialize(unittest.TestCase):

    def test_none(self):
        assert deserialize(None) is None

    def test_integer(self):
        assert deserialize(1) == 1
        assert deserialize(300) == 300

    def test_boolean(self):
        assert deserialize(True)
        assert not deserialize(False)

    def test_string(self):
        assert deserialize("string") == "string"

    def test_float(self):
        assert deserialize(0.1) == 0.1

    def test_list_of_primitives(self):
        assert deserialize([1, "a", True]) == [1, "a", True]

    def test_list_of_sets(self):
        values = [
            {
                Encoding.VAR: [1],
                Encoding.TYPE: DAT.SET
            },
            {
                Encoding.VAR: [2],
                Encoding.TYPE: DAT.SET
            }
        ]
        expected_deserialization = [
            {1},
            {2}
        ]
        assert deserialize(values) == expected_deserialization

    def test_dictionary_of_primitives(self):
        value = {
            Encoding.VAR: {"key": "value"},
            Encoding.TYPE: DAT.DICT
        }
        expected_deserialization = {"key": "value"}

        assert deserialize(value) == expected_deserialization

    def test_dictionary_of_list_of_sets(self):
        value = {
            Encoding.VAR: {
                "key_to_list": [
                    {
                        Encoding.VAR: ["a"],
                        Encoding.TYPE: DAT.SET
                    },
                    {
                        Encoding.VAR: ["b"],
                        Encoding.TYPE: DAT.SET
                    }
                ]
            },
            Encoding.TYPE: DAT.DICT
        }
        expected_deserialization = {
            "key_to_list": [
                set(["a"]),
                set(["b"])
            ]
        }
        assert deserialize(value) == expected_deserialization

    def test_set(self):
        value = {
            Encoding.VAR: [1, 2, 3],
            Encoding.TYPE: DAT.SET
        }
        expected_deserialization = {1, 2, 3}
        assert deserialize(value) == expected_deserialization

    def test_tuple(self):
        value = {
            Encoding.VAR: [1, 2, 3],
            Encoding.TYPE: DAT.TUPLE
        }
        expected_deserialization = (1, 2, 3)
        assert deserialize(value) == expected_deserialization

    def test_unsupported_value(self):
        value = Encoding.VAR
        with pytest.raises(ValueError) as err:
            deserialize(value)
        assert err.value.args[0] == "Unable to deserialize <enum 'Encoding'>"

    def test_unsupported_dict_type(self):
        value = {
            Encoding.VAR: [1, 2, 3],
            Encoding.TYPE: DAT.DAG
        }
        with pytest.raises(TypeError) as err:
            deserialize(value)
        assert err.value.args[0] == "Unable to deserialize dict of __type: dag"
