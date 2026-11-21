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

"""Utilities for Oracle AI Vector Search support."""

from __future__ import annotations

import json
import re
from array import array
from collections.abc import Iterable, Mapping, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import oracledb

if TYPE_CHECKING:
    from oracledb import SparseVector

_SIMPLE_SQL_NAME_PATTERN = r'(?:"[^"]*"|[A-Za-z][A-Za-z0-9_$#]*)'
_SIMPLE_SQL_NAME_RE = re.compile(rf"^\s*{_SIMPLE_SQL_NAME_PATTERN}\s*$")
_QUALIFIED_SQL_NAME_RE = re.compile(
    rf"^\s*{_SIMPLE_SQL_NAME_PATTERN}(?:\s*\.\s*{_SIMPLE_SQL_NAME_PATTERN})*"
    rf"(?:\s*@\s*{_SIMPLE_SQL_NAME_PATTERN}(?:\s*\.\s*{_SIMPLE_SQL_NAME_PATTERN})*)?\s*$"
)

DenseVector: TypeAlias = Sequence[int | float] | array
VectorInput: TypeAlias = "DenseVector | SparseVector"
VectorValue: TypeAlias = "array | SparseVector"
VectorResult: TypeAlias = list[int | float] | dict[str, int | list[int] | list[int | float]]

__all__ = [
    "DenseVector",
    "OracleVectorDistance",
    "OracleVectorFormat",
    "OracleVectorIndexType",
    "VectorInput",
    "VectorResult",
    "VectorValue",
    "vector_to_result",
]


class OracleVectorDistance(str, Enum):
    """Supported Oracle VECTOR_DISTANCE metrics."""

    EUCLIDEAN = "EUCLIDEAN"
    COSINE = "COSINE"
    DOT = "DOT"


class OracleVectorIndexType(str, Enum):
    """Supported Oracle vector index organizations."""

    HNSW = "HNSW"
    IVF = "IVF"


class OracleVectorFormat(str, Enum):
    """Supported Oracle VECTOR storage formats."""

    INT8 = "INT8"
    FLOAT32 = "FLOAT32"
    FLOAT64 = "FLOAT64"
    BINARY = "BINARY"
    FLEXIBLE = "*"

    @property
    def array_type_code(self) -> str:
        """Return the Python array type code for this vector format."""
        return {
            OracleVectorFormat.INT8: "b",
            OracleVectorFormat.FLOAT32: "f",
            OracleVectorFormat.FLOAT64: "d",
            OracleVectorFormat.BINARY: "B",
            OracleVectorFormat.FLEXIBLE: "f",
        }[self]


def _quote_identifier(identifier: str, *, allow_schema: bool = False) -> str:
    """
    Return an Oracle SQL name unchanged when valid, otherwise quote it safely.

    Use python-oracledb's SQL-name validation and quoting helpers when they are
    available. Compatibility validation and quoting support older python-oracledb
    versions supported by the provider.
    """
    if not isinstance(identifier, str) or not identifier:
        raise ValueError("Identifier must be a non-empty string")
    if "\x00" in identifier:
        raise ValueError("Identifier contains an invalid null byte")

    validator_name = "is_qualified_sql_name" if allow_schema else "is_simple_sql_name"
    validator = getattr(oracledb, validator_name, None)
    if validator is not None:
        is_valid = validator(identifier)
    else:
        pattern = _QUALIFIED_SQL_NAME_RE if allow_schema else _SIMPLE_SQL_NAME_RE
        is_valid = pattern.fullmatch(identifier) is not None
    if is_valid:
        return identifier

    enquote_name = getattr(oracledb, "enquote_name", None)
    if enquote_name is not None:
        return enquote_name(identifier)
    if '"' in identifier:
        raise ValueError("Identifier contains an embedded double quote")
    return f'"{identifier.upper()}"'


def _normalize_distance(distance: OracleVectorDistance | str) -> OracleVectorDistance:
    """Normalize a user supplied distance metric."""
    if isinstance(distance, OracleVectorDistance):
        return distance
    try:
        return OracleVectorDistance(str(distance).upper())
    except ValueError as exc:
        allowed = ", ".join(item.value for item in OracleVectorDistance)
        raise ValueError(
            f"Unsupported Oracle vector distance {distance!r}. Expected one of: {allowed}"
        ) from exc


def _normalize_index_type(index_type: OracleVectorIndexType | str) -> OracleVectorIndexType:
    """Normalize a user supplied index type."""
    if isinstance(index_type, OracleVectorIndexType):
        return index_type
    try:
        return OracleVectorIndexType(str(index_type).upper())
    except ValueError as exc:
        allowed = ", ".join(item.value for item in OracleVectorIndexType)
        raise ValueError(
            f"Unsupported Oracle vector index type {index_type!r}. Expected one of: {allowed}"
        ) from exc


def _normalize_vector_format(vector_format: OracleVectorFormat | str) -> OracleVectorFormat:
    """Normalize a user supplied vector storage format."""
    if isinstance(vector_format, OracleVectorFormat):
        return vector_format
    try:
        return OracleVectorFormat(str(vector_format).upper())
    except ValueError as exc:
        allowed = ", ".join(item.value for item in OracleVectorFormat)
        raise ValueError(
            f"Unsupported Oracle vector format {vector_format!r}. Expected one of: {allowed}"
        ) from exc


def _is_sparse_vector(value: Any) -> bool:
    """Return whether a value is a python-oracledb sparse vector."""
    sparse_vector = getattr(oracledb, "SparseVector", None)
    return sparse_vector is not None and isinstance(value, sparse_vector)


def vector_to_result(value: Any) -> VectorResult:
    """Convert an Oracle VECTOR value to an XCom-safe result."""
    if value is None:
        return []
    if _is_sparse_vector(value):
        return {
            "num_dimensions": value.num_dimensions,
            "indices": list(value.indices),
            "values": list(value.values),
        }
    if isinstance(value, (array, list, tuple)):
        return list(value)
    if hasattr(value, "tolist"):
        return list(value.tolist())
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            result = json.loads(stripped)
            if isinstance(result, list):
                return result
    try:
        return list(value)
    except TypeError as exc:
        raise ValueError(f"Cannot convert value of type {type(value).__name__!r} to vector result") from exc


def _vector_to_bind_value(
    value: VectorInput,
    embedding_format: OracleVectorFormat | str = OracleVectorFormat.FLOAT32,
) -> array | SparseVector:
    """Convert a convenience dense vector to a python-oracledb bind value."""
    if _is_sparse_vector(value):
        return cast("SparseVector", value)
    if isinstance(value, array):
        return value
    dense_value = cast("DenseVector", value)
    vector_format = _normalize_vector_format(embedding_format)
    if vector_format in {OracleVectorFormat.INT8, OracleVectorFormat.BINARY}:
        if any(isinstance(item, float) and not item.is_integer() for item in dense_value):
            raise ValueError(f"{vector_format.value} embeddings must contain whole numbers")
    return array(vector_format.array_type_code, dense_value)


def _coerce_json_dict(value: Any) -> dict[str, Any]:
    """Convert Oracle JSON/CLOB values to dict."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if hasattr(value, "read"):
        value = value.read()
    if isinstance(value, bytes):
        value = value.decode()
    if isinstance(value, str):
        if not value.strip():
            return {}
        loaded = json.loads(value)
        if loaded is None:
            return {}
        if not isinstance(loaded, dict):
            raise ValueError("Metadata JSON value must be an object")
        return loaded
    if isinstance(value, Mapping):
        return dict(value)
    raise ValueError(f"Cannot convert value of type {type(value).__name__!r} to metadata dict")


def _ensure_json_serializable(value: Mapping[str, Any] | None) -> str:
    """Serialize metadata as compact JSON object text."""
    if value is None:
        value = {}
    if not isinstance(value, Mapping):
        raise ValueError("Metadata must be a mapping/dictionary")
    return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _materialize_iterable(name: str, value: Iterable[Any] | None) -> list[Any] | None:
    """Materialize an iterable once so input lengths can be validated."""
    if value is None:
        return None
    return list(value)


def _validate_positive_int(
    name: str, value: int | None, *, minimum: int = 1, maximum: int | None = None
) -> None:
    """Validate an optional positive integer range."""
    if value is None:
        return
    if not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}")


class _OracleJsonFilterBuilder:
    """
    Translate supported JSON metadata filters into Oracle SQL predicates.

    The generated SQL always uses bind variables for values. Only JSON field
    names and operators are interpreted, and field names are safely quoted.
    """

    _COMPARISON_OPERATORS = {
        "$eq",
        "$ne",
        "$gt",
        "$lt",
        "$gte",
        "$lte",
        "$between",
        "$like",
        "$startsWith",
        "$hasSubstring",
        "$instr",
        "$in",
        "$nin",
        "$exists",
        "$not",
    }
    _LOGICAL_OPERATORS = {"$and", "$or", "$nor"}

    def __init__(self, metadata_column: str, *, bind_prefix: str = "vf") -> None:
        self.metadata_column = metadata_column
        self.bind_prefix = bind_prefix
        self.binds: dict[str, Any] = {}
        self._counter = 0

    def build(self, filter_: Mapping[str, Any] | None) -> tuple[str, dict[str, Any]]:
        if not filter_:
            return "", {}
        clause = self._parse_mapping(filter_)
        return clause, dict(self.binds)

    def _new_bind(self, value: Any) -> str:
        self._counter += 1
        name = f"{self.bind_prefix}_{self._counter}"
        self.binds[name] = value
        return f":{name}"

    def _json_path(self, field: str) -> str:
        return "$." + _quote_identifier(field, allow_schema=True)

    def _json_value(self, field: str) -> str:
        path = self._json_path(field).replace("'", "''")
        return f"JSON_VALUE({self.metadata_column}, '{path}' RETURNING VARCHAR2(4000) NULL ON ERROR)"

    def _json_exists(self, field: str) -> str:
        path = self._json_path(field).replace("'", "''")
        return f"JSON_EXISTS({self.metadata_column}, '{path}')"

    def _parse_mapping(self, mapping: Mapping[str, Any]) -> str:
        if not isinstance(mapping, Mapping):
            raise ValueError("Metadata filter must be a mapping")
        parts: list[str] = []
        for key, value in mapping.items():
            if key in self._LOGICAL_OPERATORS:
                parts.append(self._parse_logical(key, value))
            elif key.startswith("$"):
                raise ValueError(f"Unsupported logical metadata filter operator: {key}")
            else:
                parts.append(self._parse_field(key, value))
        if not parts:
            return "1 = 1"
        return "(" + " AND ".join(parts) + ")"

    def _parse_logical(self, operator: str, value: Any) -> str:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            raise ValueError(f"{operator} requires a list of filter expressions")
        expressions = [self._parse_mapping(item) for item in value]
        if not expressions:
            raise ValueError(f"{operator} requires at least one filter expression")
        if operator == "$and":
            return "(" + " AND ".join(expressions) + ")"
        if operator == "$or":
            return "(" + " OR ".join(expressions) + ")"
        if operator == "$nor":
            return "NOT (" + " OR ".join(expressions) + ")"
        raise ValueError(f"Unsupported logical operator: {operator}")

    def _parse_field(self, field: str, value: Any) -> str:
        if isinstance(value, Mapping) and any(str(k).startswith("$") for k in value):
            parts = [self._parse_comparison(field, str(op), operand) for op, operand in value.items()]
            return "(" + " AND ".join(parts) + ")"
        return self._parse_comparison(field, "$eq", value)

    def _parse_comparison(self, field: str, operator: str, operand: Any) -> str:
        if operator not in self._COMPARISON_OPERATORS:
            raise ValueError(f"Unsupported metadata filter operator: {operator}")
        json_value = self._json_value(field)

        if operator == "$exists":
            return self._json_exists(field) if bool(operand) else f"NOT {self._json_exists(field)}"
        if operator == "$not":
            return f"NOT ({self._parse_field(field, operand)})"
        if operator == "$eq":
            return f"{json_value} = {self._new_bind(operand)}"
        if operator == "$ne":
            return f"({json_value} <> {self._new_bind(operand)} OR {json_value} IS NULL)"
        if operator == "$gt":
            return f"TO_NUMBER({json_value} DEFAULT NULL ON CONVERSION ERROR) > {self._new_bind(operand)}"
        if operator == "$lt":
            return f"TO_NUMBER({json_value} DEFAULT NULL ON CONVERSION ERROR) < {self._new_bind(operand)}"
        if operator == "$gte":
            return f"TO_NUMBER({json_value} DEFAULT NULL ON CONVERSION ERROR) >= {self._new_bind(operand)}"
        if operator == "$lte":
            return f"TO_NUMBER({json_value} DEFAULT NULL ON CONVERSION ERROR) <= {self._new_bind(operand)}"
        if operator == "$between":
            if isinstance(operand, (str, bytes, bytearray)):
                raise ValueError("$between requires two values")
            try:
                lower, upper = operand
            except (TypeError, ValueError) as exc:
                raise ValueError("$between requires two values") from exc
            return (
                f"TO_NUMBER({json_value} DEFAULT NULL ON CONVERSION ERROR) BETWEEN "
                f"{self._new_bind(lower)} AND {self._new_bind(upper)}"
            )
        if operator == "$like":
            return f"{json_value} LIKE {self._new_bind(operand)}"
        if operator == "$startsWith":
            return f"{json_value} LIKE {self._new_bind(str(operand) + '%')}"
        if operator == "$hasSubstring":
            return f"{json_value} LIKE {self._new_bind('%' + str(operand) + '%')}"
        if operator == "$instr":
            return f"INSTR({json_value}, {self._new_bind(operand)}) > 0"
        if operator in {"$in", "$nin"}:
            if not isinstance(operand, Sequence) or isinstance(operand, (str, bytes, bytearray)):
                raise ValueError(f"{operator} requires a list of values")
            if len(operand) == 0:
                return "1 = 0" if operator == "$in" else "1 = 1"
            placeholders = ", ".join(self._new_bind(item) for item in operand)
            comparison = f"{json_value} IN ({placeholders})"
            return comparison if operator == "$in" else f"NOT ({comparison})"
        raise ValueError(f"Unsupported metadata filter operator: {operator}")
