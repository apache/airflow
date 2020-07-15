import enum
import json
from typing import Any, Union

from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding


def encode(value: Any, type_: Any) -> [Encoding, Any]:
    "Encode value and type into a JSON dict."
    return {Encoding.VAR: value, Encoding.TYPE: type_}


_return_primitive = lambda value: value


def _serialize_list(list_: list) -> list:
    return [serialize(item) for item in list_]


def _serialize_dict(dict_: dict) -> dict:
    value = {k: serialize(v) for k,v in dict_.items()}
    return encode( 
        value=value,
        type_=DAT.DICT
    )


def _serialize_set(set_: set) -> dict:
    value = [serialize(item) for item in set_]
    return encode(
        value=value,
        type_=DAT.SET
    )


def _serialize_tuple(tupe_: tuple) -> dict:
    value = [serialize(item) for item in tuple_]
    return encode(
        value=value,
        type_=DAT.TUPLE
    )


def serialize(value: Any) -> Any:
    "Serialize a value so it can be stored as JSON."
    serialization_function_by_type = {
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
        value = {k: deserialize(v) for k,v in value.items()}
    elif type_ == DAT.SET:
        value = _deserialize_set(value)
    elif type_ == DAT.TUPLE:
        value = _deserialize_tuple()
    else:
        raise TypeError(f"Unable to deserialize dict of {Encoding.TYPE}: {type_}")
    return value


def _deserialize_set(set_: set) -> set:
    return set([deserialize(item) for item in set_])


def _deserialize_tuple(tuple_: tuple) -> tuple:
    return tuple(deserialize(item) for item in tuple_)


def deserialize(value: Any) -> Any:
    "Deserialize a JSON-compatible value into its original value."
    deserialization_function_by_type = {
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
