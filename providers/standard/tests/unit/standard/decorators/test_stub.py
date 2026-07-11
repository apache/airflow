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
import typing
from typing import Any

import pytest

from airflow.providers.standard.decorators.stub import _data_type_from_annotation, stub

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS


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
    """The TaskFlow call on a stub captures the ordered positional-arg spec (``_arg_bindings``)."""

    def test_literal_and_xcom_spec(self):
        from airflow.sdk import DAG

        with DAG(dag_id="d"):
            extracted = stub(fn_extract)()
            result = stub(fn_transform)("uk", extracted)

        op = result.operator
        assert op._arg_bindings == [
            {"kind": "literal", "data_type": "string", "value": "uk"},
            {"kind": "xcom", "data_type": "object", "task_id": "fn_extract", "key": "return_value"},
            {"kind": "literal", "data_type": "integer", "value": 3},
        ]
        assert op.upstream_task_ids == {"fn_extract"}

    def test_kwargs_normalize_to_declaration_order(self):
        from airflow.sdk import DAG

        with DAG(dag_id="d"):
            extracted = stub(fn_extract)()
            result = stub(fn_transform)(extracted=extracted["part"], country="fr", retries_num=7)

        assert result.operator._arg_bindings == [
            {"kind": "literal", "data_type": "string", "value": "fr"},
            {"kind": "xcom", "data_type": "object", "task_id": "fn_extract", "key": "part"},
            {"kind": "literal", "data_type": "integer", "value": 7},
        ]

    def test_zero_param_stub_has_no_spec(self):
        assert stub(fn_pass)().operator._arg_bindings is None

    def test_untyped_params_degrade_to_any(self):
        from airflow.sdk import DAG

        with DAG(dag_id="d"):
            result = stub(fn_untyped)(1, "x")

        assert result.operator._arg_bindings == [
            {"kind": "literal", "data_type": "any", "value": 1},
            {"kind": "literal", "data_type": "any", "value": "x"},
        ]

    def test_unresolvable_annotation_degrades_to_any(self):
        def fn(x): ...

        fn.__annotations__ = {"x": "NotARealType"}
        from airflow.sdk import DAG

        with DAG(dag_id="d"):
            result = stub(fn)("v")

        assert result.operator._arg_bindings == [{"kind": "literal", "data_type": "any", "value": "v"}]

    def test_varargs_rejected(self):
        with pytest.raises(ValueError, match="fixed number of parameters"):
            stub(fn_varargs)()

    def test_varkw_rejected(self):
        with pytest.raises(ValueError, match="fixed number of parameters"):
            stub(fn_kwonly_varkw)()

    def test_context_key_param_rejected(self):
        with pytest.raises(ValueError, match="is an Airflow context key"):
            stub(fn_context_key)(1)

    def test_non_json_literal_rejected(self):
        from airflow.sdk import DAG

        with DAG(dag_id="d"), pytest.raises(ValueError, match="not JSON-serializable"):
            stub(fn_transform)("uk", object())

    def test_mapped_xcom_arg_rejected(self):
        from airflow.sdk import DAG

        with DAG(dag_id="d"):
            extracted = stub(fn_extract)()
            with pytest.raises(ValueError, match="MapXComArg"):
                stub(fn_transform)("uk", extracted.map(lambda v: v))

    def test_expand_rejected_at_parse_time(self):
        from airflow.sdk import DAG

        with DAG(dag_id="d"):
            with pytest.raises(TypeError, match="do not support dynamic task mapping"):
                stub(fn_transform).expand(country=["uk", "fr"], extracted=[{}, {}])


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        pytest.param(str, "string", id="str"),
        pytest.param(bool, "boolean", id="bool"),
        pytest.param(int, "integer", id="int"),
        pytest.param(float, "number", id="float"),
        pytest.param(dict, "object", id="dict"),
        pytest.param(dict[str, int], "object", id="dict-parameterized"),
        pytest.param(typing.Mapping[str, int], "object", id="mapping"),
        pytest.param(list, "array", id="list"),
        pytest.param(list[int], "array", id="list-parameterized"),
        pytest.param(tuple, "array", id="tuple"),
        pytest.param(set, "array", id="set"),
        pytest.param(typing.Sequence[int], "array", id="sequence"),
        pytest.param(Any, "any", id="any"),
        pytest.param(None, "any", id="none"),
        pytest.param(bytes, "any", id="bytes"),
        pytest.param(typing.Optional[str], "string", id="optional-str"),  # noqa: UP045 -- legacy form on purpose
        pytest.param(typing.Union[int, str], "any", id="union"),  # noqa: UP007 -- legacy form on purpose
        pytest.param(str | None, "string", id="pep604-optional"),
        pytest.param(int | str, "any", id="pep604-union"),
        pytest.param(contextlib.AbstractContextManager, "any", id="custom-class"),
    ],
)
def test_data_type_from_annotation(annotation, expected):
    assert _data_type_from_annotation(annotation) == expected
