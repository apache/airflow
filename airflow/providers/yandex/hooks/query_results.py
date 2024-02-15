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

# This file is a copy of https://github.com/ydb-platform/ydb/tree/284b7efb67edcdade0b12c849b7fad40739ad62b/ydb/core/fq/libs/http_api_client
# It is highly recommended to modify original file first in YDB project and merge it here afterwards

from __future__ import annotations
from typing import Any, Optional
import base64
import pprint
import dateutil.parser
from datetime import datetime
from decimal import Decimal


class YQResults:
    """Holds and formats query execution results"""

    def __init__(self, results: dict[str, Any]):
        self._raw_results = results
        self._results = None

    @staticmethod
    def _convert_from_float(value: float | str) -> float:
        # special values, e.g inf encoded as str, normal values are in float
        return float(value)

    @staticmethod
    def _convert_from_pgfloat(value: str | None) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def _convert_from_pgint(value: str | None) -> Optional[int]:
        if value is None:
            return None
        return int(value)

    @staticmethod
    def _convert_from_decimal(value: str) -> Decimal:
        return Decimal(value)

    @staticmethod
    def _convert_from_pgnumeric(value: str | None) -> Optional[Decimal]:
        if value is None:
            return None
        return Decimal(value)

    @staticmethod
    def _convert_from_base64(value: str) -> str | bytes:
        b = base64.b64decode(value)
        try:
            return b.decode("utf-8")
        except UnicodeDecodeError:
            return b

    @staticmethod
    def _convert_from_datetime(value: str) -> datetime:
        # suitable for yql data and datetime parsing
        return dateutil.parser.isoparse(value)

    @staticmethod
    def _convert_from_pgdatetime(value: str | None) -> Optional[datetime]:
        if value is None:
            return None
        return dateutil.parser.isoparse(value)

    @staticmethod
    def _convert_from_enum(value: list) -> str:
        return str(value[0])

    @staticmethod
    def _extract_from_optional(type_name: str) -> str:
        # Uint16? -> Uint16
        if type_name.endswith("?"):
            return type_name[0:-1]

        # Optional<Uint16> -> Uint16
        return type_name[len("Optional<") : -1]

    @staticmethod
    def _extract_from_set(type_name: str) -> str:
        # Set<Uint16> -> Uint16
        return type_name[len("Set<") : -1]

    @staticmethod
    def _extract_from_list(type_name: str) -> str:
        # List<Uint16> -> Uint16
        return type_name[len("List<") : -1]

    @staticmethod
    def _split_type_list(type_list: str) -> list[str]:
        # naive implementation
        # fixme: fix it
        return type_list.split(",")

    @staticmethod
    def _extract_from_tuple(type_name: str) -> list[str]:
        # Tuple<Uint16, String, Double> -> [Uint16, String, Double]
        return YQResults._split_type_list(type_name[len("Tuple<") : -1])

    @staticmethod
    def _extract_from_dict(type_name: str) -> tuple[str, str]:
        # Dict<Uint16, String> -> (Uint16, String)
        [key_type, value_type] = YQResults._split_type_list(type_name[len("Dict<") : -1])
        return key_type, value_type

    @staticmethod
    def _extract_from_variant_over_struct(type_name: str) -> tuple[str, str]:
        # Variant<'One':Int32,'Two':String> -> {One: Int32, Two: String}
        types_with_names = YQResults._split_type_list(type_name[len("Variant<") : -1])
        result = {}
        for t in types_with_names:
            [n, t] = t.split(":")
            # strip '
            n = n[1:-1]
            result[n] = t
        return result

    @staticmethod
    def _extract_from_variant_over_tuple(type_name: str) -> tuple[str, str]:
        # Variant<Int32,String> -> [Int32, String]
        return YQResults._split_type_list(type_name[len("Variant<") : -1])

    @staticmethod
    def _convert_from_optional(value: list[Any]) -> Optional[Any]:
        # Optional types are encoded as [[]] objects
        # If type is Uint16, value is encoded as {"rows":[[value]]}
        # If type is Optional<Uint16>, value is encoded as {"rows":[[[value]]]}
        # If value is None than result is {"rows":[[[]]]}
        # So check if len equals 1 it means that it contains value
        # if len is 0 it means it has no value i.e. value is None
        assert len(value) < 2, str(value)
        if len(value) == 1:
            return value[0]

        return None

    @staticmethod
    def id(v):
        return v

    @staticmethod
    def _get_converter(column_type: str) -> Any:
        """Returns converter based on column type"""

        # primitives
        if column_type in [
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Uint8",
            "Uint16",
            "Uint32",
            "Uint64",
            "Bool",
            "Utf8",
            "Uuid",
            "Void",
            "Null",
            "EmptyList",
            "Struct<>",
            "Tuple<>",
        ]:
            return YQResults.id

        if column_type == "String":
            return YQResults._convert_from_base64

        if column_type in ["Float", "Double"]:
            return YQResults._convert_from_float

        if column_type.startswith("Decimal("):
            return YQResults._convert_from_decimal

        if column_type.startswith("Enum<"):
            return YQResults._convert_from_enum

        if column_type in ["Date", "Datetime", "Timestamp"]:
            return YQResults._convert_from_datetime

        # containers
        if column_type.startswith("Optional<") or column_type.endswith("?"):
            # If type is Optional than get base type
            inner_converter = YQResults._get_converter(YQResults._extract_from_optional(column_type))

            # Remove "Optional" encoding
            # and convert resulting value as others
            def convert(x):
                inner_value = YQResults._convert_from_optional(x)
                if inner_value is None:
                    return None
                return inner_converter(inner_value)

            return convert

        if column_type.startswith("Set<"):
            inner_converter = YQResults._get_converter(YQResults._extract_from_set(column_type))

            def convert(x):
                return {inner_converter(v) for v in x}

            return convert

        if column_type.startswith("List<"):
            inner_converter = YQResults._get_converter(YQResults._extract_from_list(column_type))

            def convert(x):
                return [inner_converter(v) for v in x]

            return convert

        if column_type.startswith("Tuple<"):
            inner_types = YQResults._extract_from_tuple(column_type)
            inner_converters = [YQResults._get_converter(t) for t in inner_types]

            def convert(x):
                assert len(x) == len(
                    inner_converters
                ), f"Wrong lenght for tuple value: {len(x)} != {len(inner_converters)}"
                return tuple([c(v) for (c, v) in zip(inner_converters, x)])

            return convert

        # variant over struct
        if column_type.startswith("Variant<'"):
            inner_types = YQResults._extract_from_variant_over_struct(column_type)
            inner_converters = {k: YQResults._get_converter(t) for k, t in inner_types.items()}

            def convert(x):
                return inner_converters[x[0]](x[1])

            return convert

        # variant over tuple
        if column_type.startswith("Variant<"):
            inner_types = YQResults._extract_from_variant_over_tuple(column_type)
            inner_converters = [YQResults._get_converter(t) for t in inner_types]

            def convert(x):
                return inner_converters[x[0]](x[1])

            return convert

        if column_type == "EmptyDict":

            def convert(x):
                return {}

            return convert

        if column_type.startswith("Dict<"):
            key_type, value_type = YQResults._extract_from_dict(column_type)
            key_converter = YQResults._get_converter(key_type)
            value_converter = YQResults._get_converter(value_type)

            def convert(x):
                return {key_converter(v[0]): value_converter(v[1]) for v in x}

            return convert

        # pg types
        if column_type.startswith("pgfloat"):
            return YQResults._convert_from_pgfloat

        if column_type in ["pgint2", "pgint4", "pgint8"]:
            return YQResults._convert_from_pgint

        if column_type == "pgnumeric":
            return YQResults._convert_from_pgnumeric

        if column_type in ["pgdate", "pgtimestamp"]:
            return YQResults._convert_from_pgdatetime

        if column_type.startswith("pg"):
            return YQResults.id

        # unsupported type
        return YQResults.id

    def _convert(self):
        converters = []
        converted_results = []
        for column in self._raw_results["columns"]:
            converters.append(YQResults._get_converter(column["type"]))

        for row in self._raw_results["rows"]:
            new_row = []
            for index, value in enumerate(row):
                converter = converters[index]
                new_row.append(value if converter is None else converter(value))

            converted_results.append(new_row)

        self._results = {"rows": converted_results, "columns": self._raw_results["columns"]}

    def _repr_pretty_(self, p, cycle):
        p.text(pprint.pformat(self._results))

    @property
    def results(self):
        if self._results is None:
            self._convert()

        return self._results

    @property
    def raw_results(self):
        return self._raw_results

    def to_table(self):
        return self._results["rows"]

    def to_dataframe(self):
        result_set = self._results
        columns = [column["name"] for column in result_set["columns"]]
        import pandas

        return pandas.DataFrame(result_set["rows"], columns=columns)
