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

import contextlib
import datetime

import pytest

from airflow.exceptions import SerializationError
from airflow.providers.common.compat.sdk import DAG, task_group
from airflow.providers.standard.decorators.stub import stub

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS


def _to_dict(dag):
    """Serialize a Dag through core Dag serialization -- _arg_bindings materializes here."""
    try:
        from airflow.serialization.serialized_objects import DagSerialization
    except ImportError:  # Airflow 2 exposes the round-trip API on SerializedDAG
        from airflow.serialization.serialized_objects import SerializedDAG as DagSerialization

    return DagSerialization.to_dict(dag)


def _round_trip(dag):
    """Round-trip a Dag through core Dag serialization -- _arg_bindings materializes here."""
    try:
        from airflow.serialization.serialized_objects import DagSerialization
    except ImportError:  # Airflow 2 exposes the round-trip API on SerializedDAG
        from airflow.serialization.serialized_objects import SerializedDAG as DagSerialization

    return DagSerialization.from_dict(DagSerialization.to_dict(dag))


def fn_ellipsis(): ...


def fn_pass(): ...


def fn_doc():
    """Some string"""


def fn_doc_pass():
    """Some string"""
    pass


def fn_code():
    return None


@pytest.mark.parametrize(
    ("fn", "error"),
    [
        pytest.param(fn_ellipsis, contextlib.nullcontext(), id="ellipsis"),
        pytest.param(fn_pass, contextlib.nullcontext(), id="pass"),
        pytest.param(fn_doc, contextlib.nullcontext(), id="doc"),
        pytest.param(fn_doc_pass, contextlib.nullcontext(), id="doc-and-pass"),
        pytest.param(fn_code, pytest.raises(ValueError, match="must be an empty function"), id="not-empty"),
    ],
)
def test_stub_signature(fn, error):
    with error:
        stub(fn)()


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="retry_policy added in Airflow 3.3")
def test_stub_rejects_retry_policy():
    from airflow.sdk.definitions.retry_policy import ExceptionRetryPolicy

    with pytest.raises(ValueError, match="does not support `retry_policy`"):
        stub(fn_pass, retry_policy=ExceptionRetryPolicy(rules=[]))()


def test_stub_allows_retries():
    stub(fn_pass, retries=5)()


def fn_extract(): ...


def fn_transform(country: str, extracted: dict, retries_num: int = 3): ...


def fn_untyped(a, b): ...


def fn_varargs(*args): ...


def fn_kwonly_varkw(**kwargs): ...


def fn_context_key(ti): ...


class TestStubTaskflowArgs:
    """The TaskFlow call on a stub captures the ordered positional-arg spec (``_arg_bindings``),
    materialized by core Dag serialization from the stub's bound TaskFlow call args."""

    def test_literal_and_xcom_spec(self):
        with DAG(dag_id="d") as dag:
            extracted = stub(fn_extract)()
            result = stub(fn_transform)("uk", extracted)

        assert _round_trip(dag).task_dict["fn_transform"]._arg_bindings == [
            {"name": "country", "kind": "literal", "value_schema": {"type": "string"}, "value": "uk"},
            {
                "name": "extracted",
                "kind": "xcom",
                "value_schema": {"type": "object", "additionalProperties": True},
                "task_id": "fn_extract",
            },
            {
                "name": "retries_num",
                "kind": "literal",
                "value_schema": {"type": "integer", "format": "int64"},
                "value": 3,
                "from_default": True,
            },
        ]
        assert result.operator.upstream_task_ids == {"fn_extract"}

    def test_kwargs_normalize_to_declaration_order(self):
        with DAG(dag_id="d") as dag:
            extracted = stub(fn_extract)()
            stub(fn_transform)(extracted=extracted, country="fr", retries_num=7)

        assert _round_trip(dag).task_dict["fn_transform"]._arg_bindings == [
            {"name": "country", "kind": "literal", "value_schema": {"type": "string"}, "value": "fr"},
            {
                "name": "extracted",
                "kind": "xcom",
                "value_schema": {"type": "object", "additionalProperties": True},
                "task_id": "fn_extract",
            },
            {
                "name": "retries_num",
                "kind": "literal",
                "value_schema": {"type": "integer", "format": "int64"},
                "value": 7,
            },
        ]

    def test_explicitly_passing_the_default_value_is_not_from_default(self):
        """The flag tracks provenance, not value equality: an author-passed argument is explicit
        even when it equals the signature default, so keyword-style consumers must still claim it."""
        with DAG(dag_id="d") as dag:
            extracted = stub(fn_extract)()
            stub(fn_transform)("uk", extracted, retries_num=3)

        assert _round_trip(dag).task_dict["fn_transform"]._arg_bindings[2] == {
            "name": "retries_num",
            "kind": "literal",
            "value_schema": {"type": "integer", "format": "int64"},
            "value": 3,
        }

    def test_custom_xcom_key_rejected(self):
        with DAG(dag_id="d") as dag:
            extracted = stub(fn_extract)()
            stub(fn_transform)("uk", extracted["part"])

        with pytest.raises(SerializationError, match="indexing an output by a custom key"):
            _to_dict(dag)

    def test_zero_param_stub_has_no_spec(self):
        with DAG(dag_id="d") as dag:
            stub(fn_pass)()

        assert _round_trip(dag).task_dict["fn_pass"]._arg_bindings is None

    def test_untyped_params_omit_value_schema(self):
        """Key absence (never ``None``) is the wire contract for an unconstrained argument."""
        with DAG(dag_id="d") as dag:
            stub(fn_untyped)(1, "x")

        assert _round_trip(dag).task_dict["fn_untyped"]._arg_bindings == [
            {"name": "a", "kind": "literal", "value": 1},
            {"name": "b", "kind": "literal", "value": "x"},
        ]

    def test_unresolvable_annotation_omits_value_schema(self):
        def fn(x): ...

        fn.__annotations__ = {"x": "NotARealType"}
        with DAG(dag_id="d") as dag:
            stub(fn)("v")

        assert _round_trip(dag).task_dict["fn"]._arg_bindings == [
            {"name": "x", "kind": "literal", "value": "v"}
        ]

    def test_varargs_rejected(self):
        with DAG(dag_id="d") as dag:
            stub(fn_varargs)(1, 2)

        with pytest.raises(SerializationError, match="fixed number of parameters"):
            _to_dict(dag)

    def test_varkw_rejected(self):
        with DAG(dag_id="d") as dag:
            stub(fn_kwonly_varkw)(x=1)

        with pytest.raises(SerializationError, match="fixed number of parameters"):
            _to_dict(dag)

    def test_context_key_param_rejected(self):
        with DAG(dag_id="d") as dag:
            stub(fn_context_key)(1)

        with pytest.raises(SerializationError, match="is an Airflow context key"):
            _to_dict(dag)

    @pytest.mark.parametrize("fn", [fn_varargs, fn_kwonly_varkw, fn_context_key], ids=lambda f: f.__name__)
    def test_argless_call_skips_signature_checks(self, fn):
        """Pre-TaskFlow stub Dags never passed arguments; their signatures must keep serializing."""
        with DAG(dag_id="d") as dag:
            stub(fn)()

        assert _round_trip(dag).task_dict[fn.__name__]._arg_bindings is None

    def test_argless_call_captures_no_spec_for_defaulted_params(self):
        def fn(limit: int = 10): ...

        with DAG(dag_id="d") as dag:
            stub(fn)()

        assert _round_trip(dag).task_dict["fn"]._arg_bindings is None

    def test_non_json_literal_rejected(self):
        with DAG(dag_id="d") as dag:
            stub(fn_transform)("uk", object())

        with pytest.raises(SerializationError, match="not JSON-serializable"):
            _to_dict(dag)

    def test_nan_literal_rejected(self):
        with DAG(dag_id="d") as dag:
            stub(fn_transform)("uk", {"ratio": float("nan")})

        with pytest.raises(SerializationError, match="not JSON-serializable"):
            _to_dict(dag)

    def test_temporal_literal_rejected(self):
        def fn(when: datetime.datetime): ...

        with DAG(dag_id="d") as dag:
            stub(fn)(datetime.datetime(2020, 1, 1))

        with pytest.raises(SerializationError, match="not JSON-serializable"):
            _to_dict(dag)

    @pytest.mark.parametrize("wrap", [lambda x: [x], lambda x: {"data": x}], ids=["list", "dict"])
    def test_xcom_nested_in_collection_literal_rejected(self, wrap):
        with DAG(dag_id="d") as dag:
            extracted = stub(fn_extract)()
            stub(fn_transform)("uk", wrap(extracted))

        with pytest.raises(SerializationError, match="nested inside"):
            _to_dict(dag)

    def test_mapped_xcom_arg_rejected(self):
        with DAG(dag_id="d") as dag:
            extracted = stub(fn_extract)()
            stub(fn_transform)("uk", extracted.map(lambda v: v))

        with pytest.raises(SerializationError, match="only direct upstream task outputs"):
            _to_dict(dag)

    def test_mapped_upstream_aggregated_output_rejected(self):
        def fn_produce(n: int): ...

        with DAG(dag_id="d") as dag:
            vals = stub(fn_produce).expand(n=[1, 2])
            stub(fn_transform)("uk", vals)

        with pytest.raises(SerializationError, match="aggregated output of the mapped task"):
            _to_dict(dag)

    def test_expand_builds_mapped_stub_without_parse_time_bindings(self):
        """Mapped stubs capture no spec: their call args keep the legacy ignored behavior for now."""
        with DAG(dag_id="d") as dag:
            result = stub(fn_transform).expand(country=["uk", "fr"], extracted=[{}, {}])
        # op_kwargs_expand_input/partial_kwargs (not is_mapped) so the assertions also
        # hold on the Airflow 2.x MappedOperator, which the provider still supports.
        assert result.operator.op_kwargs_expand_input.value == {
            "country": ["uk", "fr"],
            "extracted": [{}, {}],
        }
        assert "_arg_bindings" not in result.operator.partial_kwargs

        # The wrapping MappedOperator never inherits the stub marker, so it serializes
        # cleanly with no materialized spec of its own.
        assert _round_trip(dag).task_dict["fn_transform"].inherits_from_stub_operator is False

    def test_stub_with_args_inside_mapped_task_group_rejected(self):
        @task_group
        def group(n):
            stub(fn_transform)("uk", {})

        with DAG(dag_id="d") as dag:
            group.expand(n=[1, 2])

        with pytest.raises(SerializationError, match="mapped task group"):
            _to_dict(dag)

    def test_argless_stub_inside_mapped_task_group_allowed(self):
        @task_group
        def group(n):
            stub(fn_extract)()

        with DAG(dag_id="d") as dag:
            group.expand(n=[1, 2])

        _to_dict(dag)  # must not raise
