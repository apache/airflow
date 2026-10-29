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

import pytest

from airflow.sdk.execution_time import dag_run_conf


@pytest.mark.parametrize(
    ("serialized", "expected"),
    [
        ("null", None),
        ("{}", {}),
        ('{"nested": [{"flag": false, "text": "\\u03bb"}]}', {"nested": [{"flag": False, "text": "\u03bb"}]}),
        ('{"number": 18446744073709551616}', {"number": 18446744073709551616}),
        ('{"number": -9223372036854775809}', {"number": -9223372036854775809}),
        ('{"number": 1.7e308}', {"number": 1.7e308}),
    ],
)
def test_materialize_conf_once(make_ti_context, mocker, serialized, expected):
    context = make_ti_context()
    context.serialized_dag_run_conf = serialized
    decode = mocker.spy(dag_run_conf, "decode_dag_run_conf")
    parse = mocker.spy(dag_run_conf, "from_json")

    dag_run_conf.materialize_dag_run_conf(context)
    decoded = context.dag_run.conf
    assert decoded == expected
    if expected is not None and "number" in expected:
        assert type(decoded["number"]) is type(expected["number"])
    if decoded is not None:
        decoded["added_by_task"] = True
    dag_run_conf.materialize_dag_run_conf(context)

    assert context.dag_run.conf is decoded
    assert context.serialized_dag_run_conf is None
    decode.assert_called_once_with(serialized)
    parse.assert_called_once_with(serialized, cache_strings="keys")


def test_preserve_legacy_decoded_conf(make_ti_context):
    context = make_ti_context(conf={"value": False})
    original = context.dag_run.conf

    dag_run_conf.materialize_dag_run_conf(context)

    assert context.dag_run.conf is original


@pytest.mark.parametrize("serialized", ['{"secret-canary":', "[]", "false", "42", '"secret-canary"'])
def test_reject_invalid_conf_without_exposing_payload(make_ti_context, serialized):
    context = make_ti_context()
    context.serialized_dag_run_conf = serialized

    with pytest.raises(ValueError, match="JSON object or null") as error:
        dag_run_conf.materialize_dag_run_conf(context)

    assert "secret-canary" not in str(error.value)
    assert context.dag_run.conf is None
    assert context.serialized_dag_run_conf == serialized


def test_reject_ambiguous_conf(make_ti_context):
    context = make_ti_context(conf={})
    context.serialized_dag_run_conf = '{"value": 1}'

    with pytest.raises(ValueError, match="both decoded and serialized"):
        dag_run_conf.materialize_dag_run_conf(context)
