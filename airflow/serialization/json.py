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

from typing import Any

from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding


def encode(value: Any, type_: Any) -> dict:
    """Encode value and type into a JSON-friendly dict."""
    return {Encoding.VAR: value, Encoding.TYPE: type_}


_return_primitive = lambda value: value


def _serialize_list(list_: list) -> list:
    return [serialize(item) for item in list_]


def _serialize_dict(dict_: dict) -> dict:
    value = {k: serialize(v) for k, v in dict_.items()}
    return encode(value=value, type_=DAT.DICT)


def _serialize_set(set_: set) -> dict:
    value = [serialize(item) for item in set_]
    return encode(value=value, type_=DAT.SET)


def _serialize_tuple(tuple_: tuple) -> dict:
    value = [serialize(item) for item in tuple_]
    return encode(value=value, type_=DAT.TUPLE)


def serialize(value):
    """Serialize a value so it can be stored as JSON.

    The serialization protocol is:

    (1) keep JSON supported types: primitives, dict, list;
    (2) encode other types such as tuples and sets as
       ``{TYPE: 'foo', VAR: 'bar'}``
    """
    serialization_function_by_type = {
        type(None): _return_primitive,
        int: _return_primitive,
        bool: _return_primitive,
        float: _return_primitive,
        str: _return_primitive,
        list: _serialize_list,
        dict: _serialize_dict,
        set: _serialize_set,
        tuple: _serialize_tuple
    }
    try:
        function = serialization_function_by_type[type(value)]
    except KeyError:
        raise TypeError(f"Unable to serialize {type(value)}")
    return function(value)


def _deserialize_list(list_: list) -> list:
    return [deserialize(item) for item in list_]


def _deserialize_dict(dict_: dict) -> dict:
    type_ = dict_[Encoding.TYPE]
    value = dict_[Encoding.VAR]
    if type_ == DAT.DICT:
        value = {k: deserialize(v) for k, v in value.items()}
    elif type_ == DAT.SET:
        value = _deserialize_set(value)
    elif type_ == DAT.TUPLE:
        value = _deserialize_tuple(value)
    else:
        raise TypeError(f"Unable to deserialize dict of {Encoding.TYPE}: {type_}")
    return value


def _deserialize_set(set_: set) -> set:
    return {deserialize(item) for item in set_}


def _deserialize_tuple(tuple_: tuple) -> tuple:
    return tuple(deserialize(item) for item in tuple_)


def deserialize(value):
    """Deserialize a JSON-compatible value into its original value.

    The deserialization protocol is:

    (1) keep JSON supported types: primitives, lists
    (2) decode other types which might have been encoded in dicts using
       ``{TYPE: 'foo', VAR: 'bar'}``
    """

    deserialization_function_by_type = {
        type(None): _return_primitive,
        int: _return_primitive,
        bool: _return_primitive,
        float: _return_primitive,
        str: _return_primitive,
        list: _deserialize_list,
        dict: _deserialize_dict
    }
    try:
        function = deserialization_function_by_type[type(value)]
    except KeyError:
        raise ValueError(f"Unable to deserialize {type(value)}")
    return function(value)
