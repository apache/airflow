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

"""Unit tests for serialized objects module."""
from airflow.serialization.enums import DagAttributeTypes, Encoding
from airflow.serialization.serialized_objects import BaseSerialization


def test_can_serialize_a_serialized_object():
    input = {"hi": "yo"}
    serialized = BaseSerialization._serialize(input)
    assert serialized == {Encoding.TYPE: DagAttributeTypes.DICT, Encoding.VAR: input}
    reserialized = BaseSerialization._serialize(serialized)
    assert reserialized == {
        "__var": {
            "__encoding_enum__-__var": {"__var": {"hi": "yo"}, "__type": "dict"},
            "__encoding_enum__-__type": "dict",
        },
        "__type": "dict",
    }
    de_reserialized = BaseSerialization._deserialize(reserialized)
    de_serialized = BaseSerialization._deserialize(de_reserialized)
    assert de_serialized == input
