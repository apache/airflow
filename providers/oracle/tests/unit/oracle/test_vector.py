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

from array import array
from unittest import mock

import pytest

from airflow.providers.oracle import vector
from airflow.providers.oracle.vector import OracleVectorFormat, _OracleJsonFilterBuilder, _quote_identifier


@pytest.mark.parametrize(
    ("identifier", "allow_schema"),
    [
        ("docs", False),
        ("my_schema.docs", True),
        ('"Some.random.periods.in.my.owner"."some.more.random.periods.in.my.table.name"', True),
    ],
)
def test_quote_identifier_returns_valid_driver_names_unchanged(identifier, allow_schema):
    assert _quote_identifier(identifier, allow_schema=allow_schema) == identifier


@mock.patch.object(vector.oracledb, "is_simple_sql_name", new=None, create=True)
@mock.patch.object(vector.oracledb, "is_qualified_sql_name", new=None, create=True)
def test_quote_identifier_uses_compatibility_validation_for_older_drivers():
    assert _quote_identifier("docs") == "docs"
    assert _quote_identifier('"owner.with.periods"."table.with.periods"', allow_schema=True) == (
        '"owner.with.periods"."table.with.periods"'
    )


@pytest.mark.parametrize(
    ("identifier", "allow_schema", "expected"),
    [
        ("docs;drop table x", True, '"DOCS;DROP TABLE X"'),
        ("docs where 1=1", True, '"DOCS WHERE 1=1"'),
        ("1docs", True, '"1DOCS"'),
        ("docs--", True, '"DOCS--"'),
        ("schema.table.extra", False, '"SCHEMA.TABLE.EXTRA"'),
    ],
)
def test_quote_identifier_quotes_invalid_names(identifier, allow_schema, expected):
    assert _quote_identifier(identifier, allow_schema=allow_schema) == expected


@pytest.mark.parametrize(
    ("embedding_format", "embedding", "expected"),
    [
        (OracleVectorFormat.FLOAT32, [1.0, 2.0, 3.0], array("f", [1.0, 2.0, 3.0])),
        (OracleVectorFormat.FLOAT64, [1.0, 2.0, 3.0], array("d", [1.0, 2.0, 3.0])),
        (OracleVectorFormat.INT8, [1, 2, 3], array("b", [1, 2, 3])),
        (OracleVectorFormat.BINARY, [1, 2, 3], array("B", [1, 2, 3])),
    ],
)
def test_vector_to_bind_value_uses_type_matching_vector_format(embedding_format, embedding, expected):
    assert vector._vector_to_bind_value(embedding, embedding_format) == expected


@pytest.mark.parametrize("embedding_format", [OracleVectorFormat.INT8, OracleVectorFormat.BINARY])
def test_vector_to_bind_value_rejects_fractional_integer_embeddings(embedding_format):
    with pytest.raises(ValueError, match="must contain whole numbers"):
        vector._vector_to_bind_value([1.5], embedding_format)


def test_vector_to_bind_value_preserves_native_driver_vectors():
    dense_vector = array("d", [1.0, 2.0, 3.0])
    sparse_vector = vector.oracledb.SparseVector(5, [1, 3], array("b", [4, 5]))

    assert vector._vector_to_bind_value(dense_vector) is dense_vector
    assert vector._vector_to_bind_value(sparse_vector) is sparse_vector


def test_vector_to_result_serializes_sparse_vector():
    sparse_vector = vector.oracledb.SparseVector(5, [1, 3], array("f", [4.0, 5.0]))

    assert vector.vector_to_result(sparse_vector) == {
        "num_dimensions": 5,
        "indices": [1, 3],
        "values": [4.0, 5.0],
    }


def test_coerce_json_dict_returns_existing_dict():
    metadata = {"source": "unit"}

    assert vector._coerce_json_dict(metadata) is metadata


def test_filter_builder_logical_operators():
    builder = _OracleJsonFilterBuilder('"METADATA"')
    clause, binds = builder.build({"$and": [{"source": "unit"}, {"version": {"$gte": 2}}]})
    assert "JSON_VALUE" in clause
    assert "AND" in clause
    assert binds["vf_1"] == "unit"
    assert binds["vf_2"] == 2


def test_filter_builder_supports_quoted_field_names():
    builder = _OracleJsonFilterBuilder("metadata")

    clause, binds = builder.build({'"source.version"': "unit"})

    assert '$."source.version"' in clause
    assert binds == {"vf_1": "unit"}


def test_filter_builder_accepts_iterable_between_bounds():
    builder = _OracleJsonFilterBuilder("metadata")

    clause, binds = builder.build({"version": {"$between": iter((1, 2))}})

    assert "BETWEEN :vf_1 AND :vf_2" in clause
    assert binds == {"vf_1": 1, "vf_2": 2}


def test_filter_builder_rejects_unknown_operator():
    builder = _OracleJsonFilterBuilder('"METADATA"')
    with pytest.raises(ValueError, match=r"Unsupported metadata filter operator: \$bad"):
        builder.build({"source": {"$bad": "x"}})
