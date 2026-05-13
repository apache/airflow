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

import json

import pytest

from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.serialization.definitions.notset import NOTSET
from airflow.serialization.helpers import serialize_template_field


def test_serialize_template_field_with_very_small_max_length(monkeypatch):
    """Test that truncation message is prioritized even for very small max_length."""
    monkeypatch.setenv("AIRFLOW__CORE__MAX_TEMPLATED_FIELD_LENGTH", "1")

    result = serialize_template_field("This is a long string", "test")

    # The truncation message should be shown even if it exceeds max_length
    # This ensures users always see why content is truncated
    assert result
    assert "Truncated. You can change this behaviour" in result


def test_serialize_template_field_truncation_kicks_in(monkeypatch):
    """Long serialized output must be truncated with the standard message."""
    monkeypatch.setenv("AIRFLOW__CORE__MAX_TEMPLATED_FIELD_LENGTH", "20")

    long_value = {"k": "x" * 500}
    result = serialize_template_field(long_value, "field")

    assert "Truncated. You can change this behaviour" in result


def test_serialize_template_field_with_notset():
    """NOTSET must serialize deterministically via serialize(), not str() fallback."""
    result = serialize_template_field(NOTSET, "logical_date")
    assert result == "NOTSET"


def test_serialize_template_field_with_set_during_execution():
    """SetDuringExecution must use its own serialize() override."""
    result = serialize_template_field(SET_DURING_EXECUTION, "logical_date")
    assert result == "DYNAMIC (set during execution)"


def test_argnotset_repr_and_str():
    """repr/str should return the stable serialized sentinel string."""
    assert repr(NOTSET) == "NOTSET"
    assert str(NOTSET) == "NOTSET"
    assert repr(SET_DURING_EXECUTION) == "DYNAMIC (set during execution)"
    assert str(SET_DURING_EXECUTION) == "DYNAMIC (set during execution)"


def test_serialize_template_field_with_dict_value_callable():

    def fn_returns_callable():
        def get_arg():
            pass

        return get_arg

    template_name = "op_kwargs"

    def make_value():
        return {"values": [3, 1, 2], "sort_key": lambda x: x}

    result1 = serialize_template_field(make_value(), template_name)
    result2 = serialize_template_field(make_value(), template_name)

    assert result1 == result2

    def make_value_nested():
        return {
            "values": [3, 1, 2],
            "sort_key_nested": {"b": lambda x: fn_returns_callable(), "a": "test"},
        }

    result1_nested = serialize_template_field(make_value_nested(), template_name)
    result2_nested = serialize_template_field(make_value_nested(), template_name)

    assert result1_nested == result2_nested


def test_serialize_template_field_with_mixed_key_dict_and_callable():
    """Mixed-key dicts containing callables must serialize deterministically without TypeError."""
    template_name = "op_kwargs"

    def make_value():
        return {1: "a", "b": lambda x: x, 2: "c"}

    result1 = serialize_template_field(make_value(), template_name)
    result2 = serialize_template_field(make_value(), template_name)

    assert result1 == result2
    assert any(isinstance(v, str) and "<callable " in v for v in result1.values())


def test_serialize_template_field_with_mixed_key_jsonable_dict():
    """Jsonable mixed-key dicts must not raise when sorted for deterministic output."""
    template_name = "op_kwargs"

    def make_value():
        return {1: "a", "b": "c", 2: "d", 3: True}

    result1 = serialize_template_field(make_value(), template_name)
    result2 = serialize_template_field(make_value(), template_name)

    assert result1 == result2


@pytest.mark.parametrize(
    "value",
    [None, "hello", 0, 42, -1, 3.14, True, False],
    ids=["none", "str", "zero", "int", "neg_int", "float", "true", "false"],
)
def test_serialize_template_field_primitives_pass_through(value):
    """Primitives (None, str, int, float, bool) must be returned unchanged and keep their type."""
    result = serialize_template_field(value, "field")
    assert result == value
    assert type(result) is type(value)


def test_serialize_template_field_tuple_becomes_list():
    """Top-level and nested tuples must flatten to lists for JSON compatibility."""
    result = serialize_template_field((1, 2, (3, 4)), "field")
    assert result == [1, 2, [3, 4]]


def test_serialize_template_field_tuple_key_normalized():
    """Tuple keys must be normalized to a string so the dict stays JSON-encodable."""
    result1 = serialize_template_field({(1, 2): "v", (3, 4): "w"}, "op_kwargs")
    result2 = serialize_template_field({(3, 4): "w", (1, 2): "v"}, "op_kwargs")

    assert result1 == result2
    assert all(isinstance(k, str) for k in result1)
    json.dumps(result1)  # must not raise


def test_serialize_template_field_frozenset_key_normalized():
    """Frozenset keys must be normalized to a string."""
    result = serialize_template_field({frozenset([1, 2]): "v"}, "op_kwargs")
    assert isinstance(next(iter(result)), str)
    json.dumps(result)


def test_serialize_template_field_callable_key_uses_qualname():
    """Callable keys must serialize via qualname so memory addresses don't leak into the hash."""

    def my_fn():
        pass

    result = serialize_template_field({my_fn: "v"}, "op_kwargs")
    key = next(iter(result))
    assert key.startswith("<callable ")
    assert "my_fn" in key
    assert "at 0x" not in key


def test_serialize_template_field_mixed_exotic_keys_deterministic():
    """A dict with str, int, tuple, and callable keys must serialize the same way every call."""

    def my_fn():
        pass

    def make_value():
        return {"a": 1, 2: "b", (3, 4): "c", my_fn: "d"}

    r1 = serialize_template_field(make_value(), "op_kwargs")
    r2 = serialize_template_field(make_value(), "op_kwargs")
    assert r1 == r2
    json.dumps(r1)


def test_serialize_template_field_object_with_serialize_method():
    """An object exposing serialize() must use it (recursively) instead of str()."""

    class Custom:
        def serialize(self):
            return {"kind": "custom", "values": (1, 2, 3)}

    result = serialize_template_field(Custom(), "field")
    assert result == {"kind": "custom", "values": [1, 2, 3]}


def test_serialize_template_field_object_with_getattr_no_serialize():
    """Objects with custom __getattr__ but no real serialize attribute must fall through to str()."""

    class Tricky:
        def __getattr__(self, item):
            # Mimic SQLAlchemy / proxy objects that return *something* for any attribute access
            return lambda *a, **kw: "should-not-be-called"

        def __str__(self):
            return "tricky-object"

    result = serialize_template_field(Tricky(), "field")
    assert result == "tricky-object"


def test_serialize_template_field_non_kubernetes_to_dict_falls_through_to_str():
    """User classes that happen to define to_dict() must not be treated as K8s payloads."""

    class CustomWithToDict:
        def to_dict(self):
            return {"should": "not be used"}

        def __str__(self):
            return "custom-via-str"

    result = serialize_template_field(CustomWithToDict(), "field")
    assert result == "custom-via-str"


def test_serialize_template_field_kubernetes_object_uses_to_dict():
    """Objects whose class is defined under the kubernetes.* namespace are normalized via to_dict()."""

    class FakeK8sObject:
        def to_dict(self):
            return {"kind": "Pod", "metadata": {"name": "test"}}

    FakeK8sObject.__module__ = "kubernetes.client.models.v1_pod"

    result = serialize_template_field(FakeK8sObject(), "field")
    assert result == {"kind": "Pod", "metadata": {"name": "test"}}


def test_serialize_template_field_bytes_become_str():
    """Bytes are not JSON-encodable; they must be coerced via str()."""
    result = serialize_template_field(b"binary", "field")
    assert isinstance(result, str)


def test_serialize_template_field_no_memory_address_in_output():
    """Output must never contain `<function ... at 0x...>` repr leaks (which would break DAG hashing)."""

    def my_fn():
        pass

    value = {
        "a": my_fn,
        "b": [my_fn, {"c": my_fn}],
        my_fn: "as-key",
        ("tup",): my_fn,
    }
    result = serialize_template_field(value, "op_kwargs")
    assert "at 0x" not in str(result)


def test_serialize_template_field_plain_object_has_no_memory_address():
    """Objects relying on the default object.__str__ would leak `<ClassName object at 0x...>`."""

    class Opaque:
        pass

    result = serialize_template_field(Opaque(), "field")
    assert isinstance(result, str)
    assert "at 0x" not in result
    assert "Opaque" in result


def test_serialize_template_field_plain_object_repr_preserved_when_custom():
    """A user-defined __repr__ is a meaningful representation and must be kept as-is."""

    class WithRepr:
        def __repr__(self):
            return "stable-repr"

    result = serialize_template_field(WithRepr(), "field")
    assert result == "stable-repr"


def test_serialize_template_field_set_of_plain_objects_is_deterministic():
    """Repeated serialization of a set of plain objects must produce identical output across calls."""

    class Opaque:
        pass

    first = serialize_template_field({Opaque(), Opaque()}, "field")
    second = serialize_template_field({Opaque(), Opaque()}, "field")
    assert first == second
    assert "at 0x" not in str(first)


def test_serialize_template_field_output_is_jsonable():
    """Whatever shape we pass in, the result must be directly JSON-encodable."""

    def my_fn():
        pass

    value = {
        "callable_value": my_fn,
        "nested": {"list": [1, (2, 3), my_fn], "deep": {("k",): my_fn}},
        frozenset([1, 2]): [my_fn],
        my_fn: {"x": 1},
    }
    result = serialize_template_field(value, "op_kwargs")
    json.dumps(result)


def test_serialize_template_field_deeply_nested_determinism():
    """Determinism across new instances of the same nested structure (key ordering must not matter)."""

    def my_fn():
        pass

    def make_a():
        return {
            "z": [3, 2, 1],
            "a": {"nested": my_fn, "items": (1, 2)},
            10: ("x", "y"),
        }

    def make_b():
        # Same content, different insertion order
        return {
            10: ("x", "y"),
            "a": {"items": (1, 2), "nested": my_fn},
            "z": [3, 2, 1],
        }

    assert serialize_template_field(make_a(), "f") == serialize_template_field(make_b(), "f")


def test_serialize_template_field_bool_not_collapsed_to_int():
    """bool must be preserved as bool (Python treats True == 1, but JSON distinguishes them)."""
    result = serialize_template_field({"flag": True, "count": 1}, "op_kwargs")
    assert result["flag"] is True
    assert result["count"] == 1
    assert type(result["flag"]) is bool


def test_serialize_template_field_none_preserved():
    """None must round-trip as None, not the string 'None'."""
    result = serialize_template_field({"x": None, "y": [None, 1]}, "op_kwargs")
    assert result == {"x": None, "y": [None, 1]}


def test_serialize_template_field_list_with_callables_and_objects():
    """Lists must recursively serialize callables and objects without leaking repr."""

    def my_fn():
        pass

    class Custom:
        def serialize(self):
            return "custom-serialized"

    result = serialize_template_field([1, my_fn, Custom(), (2, my_fn)], "field")
    assert result[0] == 1
    assert result[1].startswith("<callable ")
    assert "my_fn" in result[1]
    assert result[2] == "custom-serialized"
    assert result[3][0] == 2
    assert result[3][1].startswith("<callable ")


def test_serialize_template_field_key_with_serialize_returning_nested_callable():
    """A key whose .serialize() returns a structure containing callables must not leak memory addresses."""

    def my_fn():
        pass

    class Custom:
        def serialize(self):
            return {"k": my_fn}  # nested callable inside serialize() output

    result = serialize_template_field({Custom(): "v"}, "op_kwargs")
    assert "at 0x" not in str(result)
    json.dumps(result)


def test_serialize_template_field_key_with_serialize_returning_primitive():
    """A key whose .serialize() returns a primitive must use that primitive directly (no str() wrap)."""

    class Custom:
        def serialize(self):
            return "stable-id-v1"

    result = serialize_template_field({Custom(): "v"}, "op_kwargs")
    assert result == {"stable-id-v1": "v"}


def test_serialize_template_field_key_with_serialize_returning_list_with_callable():
    """Sibling case to the dict-with-callable test: list output with nested callables must also be cleaned before str()."""

    def my_fn():
        pass

    class Custom:
        def serialize(self):
            return [1, my_fn, (2, my_fn)]

    result1 = serialize_template_field({Custom(): "v"}, "op_kwargs")
    result2 = serialize_template_field({Custom(): "v"}, "op_kwargs")

    key = next(iter(result1))
    assert "at 0x" not in key
    assert "<callable " in key
    assert result1 == result2
    json.dumps(result1)


def test_serialize_template_field_key_falls_back_to_str_when_no_serialize():
    """A non-primitive, non-callable key without .serialize() must use str() of the original object"""

    class NoSerialize:
        def __str__(self):
            return "no-serialize-stringified"

    result = serialize_template_field({NoSerialize(): "v"}, "op_kwargs")
    assert result == {"no-serialize-stringified": "v"}


def test_serialize_template_field_set_value_with_callable_no_memory_address_leak():
    """A set containing a callable must replace the callable via qualname, not leak `at 0x...`."""

    def my_fn():
        pass

    result = serialize_template_field({my_fn}, "op_kwargs")

    assert "at 0x" not in str(result)
    assert "<callable " in str(result)


def test_serialize_template_field_frozenset_value_with_callable_no_memory_address_leak():
    """Same regression as set, but with frozenset as a value."""

    def my_fn():
        pass

    result = serialize_template_field({"items": frozenset([my_fn])}, "op_kwargs")

    assert "at 0x" not in str(result)
    assert "<callable " in str(result)


def test_serialize_template_field_frozenset_key_with_callable_member_no_memory_address_leak():
    """A frozenset key containing a callable must serialize without leaking memory addresses."""

    def my_fn():
        pass

    # frozenset of hashables (functions are hashable) is a valid dict key
    result = serialize_template_field({frozenset([my_fn]): "v"}, "op_kwargs")

    key = next(iter(result))
    assert "at 0x" not in key
    assert "<callable " in key


def test_serialize_template_field_set_value_flattens_to_list():
    """Set must serialize to a JSON-compatible list, not a Python set repr string."""

    result = serialize_template_field({"items": {1, 2, 3}}, "op_kwargs")

    assert isinstance(result["items"], list)
    assert sorted(result["items"]) == [1, 2, 3]
    json.dumps(result)


def test_serialize_template_field_set_of_strings_deterministic_ordering():
    """Set of strings must serialize with deterministic ordering — not affected by PYTHONHASHSEED.

    Sets are walked then sorted by (type_name, str(element)), so the output ordering
    depends on the elements rather than on hash randomization across processes.
    """
    # Same content, two independent set instances
    a = serialize_template_field({"items": {"banana", "apple", "cherry"}}, "op_kwargs")
    b = serialize_template_field({"items": {"cherry", "banana", "apple"}}, "op_kwargs")

    assert a == b
    assert isinstance(a["items"], list)
    assert a["items"] == sorted(a["items"])


def test_serialize_template_field_nested_set_with_callable():
    """Set nested deep inside a dict/list must still recursively clean callables."""

    def my_fn():
        pass

    value = {"outer": [{"inner": {my_fn, "literal"}}]}
    result = serialize_template_field(value, "op_kwargs")

    assert "at 0x" not in str(result)
    json.dumps(result)


def test_serialize_template_field_callable_keys_sort_by_qualname_not_address():
    """Two distinct named callables as dict keys must sort by qualname, not memory address.

    Without this guarantee, two semantically-identical inputs that happen to allocate
    the callables in a different order produce different serialized output, and re-parsing
    the same Dag in another process can produce a different hash.
    """

    def fn_a():
        pass

    def fn_b():
        pass

    # Two dicts with the same content but different insertion orders must produce
    # the same output once sorting is keyed on qualname.
    r1 = serialize_template_field({fn_a: 1, fn_b: 2}, "op_kwargs")
    r2 = serialize_template_field({fn_b: 2, fn_a: 1}, "op_kwargs")

    assert r1 == r2
    # The serialized iteration order must follow qualname (fn_a before fn_b),
    # not memory address.
    keys = list(r1.keys())
    assert len(keys) == 2
    assert "fn_a" in keys[0]
    assert "fn_b" in keys[1]


def test_serialize_template_field_lambda_keys_collapse_deterministically():
    """Multiple lambdas as keys collapse to one entry deterministically across parses.

    Each call to ``make_value()`` produces *new* lambda objects with new memory
    addresses. The serialized result must not depend on those addresses.
    """

    def make_value():
        # Two lambdas; both qualnames are ``<lambda>``, so they collapse to the same
        # serialized key. The assertion below targets stability across calls,
        # not key preservation between the two lambdas.
        return {(lambda x: x): "a", (lambda y: y): "b"}

    r1 = serialize_template_field(make_value(), "op_kwargs")
    r2 = serialize_template_field(make_value(), "op_kwargs")

    assert r1 == r2
    assert "at 0x" not in str(r1)


def test_serialize_template_field_dict_with_serializable_keys_sort_by_serialized_form():
    """Custom objects whose .serialize() returns a stable string must be sorted by that string, not by repr."""

    class StableId:
        def __init__(self, name):
            self.name = name

        def serialize(self):
            return self.name

    # Insert in reverse alphabetical order — sorting by serialized form must reverse it.
    r1 = serialize_template_field({StableId("zeta"): 1, StableId("alpha"): 2}, "op_kwargs")
    r2 = serialize_template_field({StableId("alpha"): 2, StableId("zeta"): 1}, "op_kwargs")

    assert r1 == r2
    assert list(r1.keys()) == ["alpha", "zeta"]


@pytest.mark.parametrize(
    ("value", "expected_keys"),
    [
        ({1: "a", 2: "b"}, {"1", "2"}),
        ({True: "a", False: "b"}, {"True", "False"}),
        ({None: "a"}, {"None"}),
        ({1.5: "a", 2.5: "b"}, {"1.5", "2.5"}),
        ({1: "a", "b": "c"}, {"1", "b"}),
    ],
    ids=["int_keys", "bool_keys", "none_key", "float_keys", "mixed_int_str"],
)
def test_serialize_template_field_primitive_keys_coerced_to_string(value, expected_keys):
    """All dict keys must be coerced to str so json.dumps(sort_keys=True) downstream cannot raise."""
    result = serialize_template_field(value, "op_kwargs")
    assert set(result.keys()) == expected_keys
    assert all(isinstance(k, str) for k in result)


def test_serialize_template_field_mixed_primitive_keys_jsonable_sort_keys():
    """Output of mixed-type primitive keys must survive ``json.dumps(..., sort_keys=True)``."""
    value = {1: "a", "b": "c", 2: "d", 3: True, None: "z", False: "y"}
    result = serialize_template_field(value, "op_kwargs")

    json.dumps(result, sort_keys=True)


def test_serialize_template_field_mixed_primitive_keys_deterministic_across_calls():
    """Same input parsed twice must yield identical output once keys are stringified."""

    def fn_a():
        pass

    def fn_b():
        pass

    def make_value():
        return {1: "a", "b": "c", 2: "d", None: "z", "test": fn_b, fn_a: 3.5}

    assert serialize_template_field(make_value(), "op_kwargs") == serialize_template_field(
        make_value(), "op_kwargs"
    )


def test_serialize_template_field_nested_mixed_primitive_keys_jsonable():
    """Nested mixed-type primitive keys (dict inside dict) must also be coerced and jsonable."""
    value = {"outer": {1: "a", "b": "c", None: "z"}}
    result = serialize_template_field(value, "op_kwargs")

    assert all(isinstance(k, str) for k in result["outer"])
    json.dumps(result, sort_keys=True)


def test_serialize_template_field_deeply_nested_dict_keys_recursively_normalized():
    """Every nested dict must apply key normalization and sorting recursively.

    Mixed-type primitive keys and callable keys appear at multiple depths; the
    helper must stringify and sort them at each level so the full output is
    deterministic across calls and safe for ``json.dumps(sort_keys=True)``.
    """

    def fn_inner():
        pass

    def make_value():
        return {
            "level1": {
                1: "a",
                fn_inner: {
                    None: "deep",
                    "nested_str": "value",
                    2.5: {fn_inner: "deepest"},
                },
                "b": {3: "three", 4: "four"},
            },
        }

    r1 = serialize_template_field(make_value(), "op_kwargs")
    r2 = serialize_template_field(make_value(), "op_kwargs")

    assert r1 == r2
    assert all(isinstance(k, str) for k in r1["level1"])
    callable_key = next(k for k in r1["level1"] if "fn_inner" in k)
    inner = r1["level1"][callable_key]
    assert all(isinstance(k, str) for k in inner)
    float_key = next(k for k in inner if k == "2.5")
    assert all(isinstance(k, str) for k in inner[float_key])
    assert "at 0x" not in str(r1)
    json.dumps(r1, sort_keys=True)
