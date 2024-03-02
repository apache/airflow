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

from datetime import datetime
from dateutil.tz import tzutc
from decimal import Decimal

from airflow.providers.yandex.yq_client.query_results import YQResults


class TestYQResults:
    def test_integral_results(self):
        # json response and results could be found here: https://github.com/ydb-platform/ydb/blob/284b7efb67edcdade0b12c849b7fad40739ad62b/ydb/tests/fq/http_api/test_http_api.py#L336
        r = YQResults(
            {
                "rows": [
                    [
                        100,
                        -100,
                        200,
                        200,
                        10000000000,
                        -20000000000,
                        "18014398509481984",
                        "-18014398509481984",
                        123.5,
                        -789.125,
                        "inf",
                        True,
                        False,
                        "aGVsbG8=",
                        "hello",
                        "1.23",
                        'he"llo_again',
                        "Я Привет",
                        1,
                        2,
                        3,
                        4,
                    ]
                ],
                "columns": [
                    {"name": "column0", "type": "Int32"},
                    {"name": "column1", "type": "Int32"},
                    {"name": "column2", "type": "Int64"},
                    {"name": "column3", "type": "Uint64"},
                    {"name": "column4", "type": "Uint64"},
                    {"name": "column5", "type": "Int64"},
                    {"name": "column6", "type": "Int64"},
                    {"name": "column7", "type": "Int64"},
                    {"name": "column8", "type": "Float"},
                    {"name": "column9", "type": "Double"},
                    {"name": "column10", "type": "Double"},
                    {"name": "column11", "type": "Bool"},
                    {"name": "column12", "type": "Bool"},
                    {"name": "column13", "type": "String"},
                    {"name": "column14", "type": "Utf8"},
                    {"name": "column15", "type": "Decimal(6,3)"},
                    {"name": "column16", "type": "Utf8"},
                    {"name": "column17", "type": "Utf8"},
                    {"name": "column18", "type": "Int8"},
                    {"name": "column19", "type": "Int16"},
                    {"name": "column20", "type": "Uint8"},
                    {"name": "column21", "type": "Uint16"},
                ],
            },
        )

        results = r.results

        assert results == {
            "rows": [
                [
                    100,
                    -100,
                    200,
                    200,
                    10000000000,
                    -20000000000,
                    "18014398509481984",
                    "-18014398509481984",
                    123.5,
                    -789.125,
                    float("inf"),
                    True,
                    False,
                    "hello",
                    "hello",
                    Decimal("1.23"),
                    'he"llo_again',
                    "Я Привет",
                    1,
                    2,
                    3,
                    4,
                ]
            ],
            "columns": [
                {"name": "column0", "type": "Int32"},
                {"name": "column1", "type": "Int32"},
                {"name": "column2", "type": "Int64"},
                {"name": "column3", "type": "Uint64"},
                {"name": "column4", "type": "Uint64"},
                {"name": "column5", "type": "Int64"},
                {"name": "column6", "type": "Int64"},
                {"name": "column7", "type": "Int64"},
                {"name": "column8", "type": "Float"},
                {"name": "column9", "type": "Double"},
                {"name": "column10", "type": "Double"},
                {"name": "column11", "type": "Bool"},
                {"name": "column12", "type": "Bool"},
                {"name": "column13", "type": "String"},
                {"name": "column14", "type": "Utf8"},
                {"name": "column15", "type": "Decimal(6,3)"},
                {"name": "column16", "type": "Utf8"},
                {"name": "column17", "type": "Utf8"},
                {"name": "column18", "type": "Int8"},
                {"name": "column19", "type": "Int16"},
                {"name": "column20", "type": "Uint8"},
                {"name": "column21", "type": "Uint16"},
            ],
        }

    def test_complex_results(self):
        # json response and results could be found here: https://github.com/ydb-platform/ydb/blob/284b7efb67edcdade0b12c849b7fad40739ad62b/ydb/tests/fq/http_api/test_http_api.py#L445
        r = YQResults(
            {
                "rows": [
                    [
                        [],
                        [1, 2],
                        [],
                        [["YWJj", 1]],
                        [["xyz", 1]],
                        None,
                        "PT15M",
                        "2019-09-16",
                        "2019-09-16T10:46:05Z",
                        "2019-09-16T11:27:44.345849Z",
                        "2019-09-16,Europe/Moscow",
                        "2019-09-16T14:32:40,Europe/Moscow",
                        "2019-09-16T14:32:55.874913,Europe/Moscow",
                        ["One", 12],
                        [1, "eHl6"],
                        ["a", 1],
                        ["monday", None],
                        1,
                        {},
                        {"a": 1, "b": "xyz"},
                        None,
                        None,
                        [[[1, [[177]]]]],
                        [[[1, []]]],
                        [[[1, []]]],
                        ["Foo", None],
                        ["Bar", None],
                        [],
                        [1, "cHJpdmV0", "2019-09-16"],
                    ]
                ],
                "columns": [
                    {"name": "column0", "type": "EmptyList"},
                    {"name": "column1", "type": "List<Int32>"},
                    {"name": "column2", "type": "EmptyDict"},
                    {"name": "column3", "type": "Dict<String,Int32>"},
                    {"name": "column4", "type": "Dict<Utf8,Int32>"},
                    {"name": "column5", "type": "Uuid"},
                    {"name": "column6", "type": "Interval"},
                    {"name": "column7", "type": "Date"},
                    {"name": "column8", "type": "Datetime"},
                    {"name": "column9", "type": "Timestamp"},
                    {"name": "column10", "type": "TzDate"},
                    {"name": "column11", "type": "TzDatetime"},
                    {"name": "column12", "type": "TzTimestamp"},
                    {"name": "column13", "type": "Variant<'One':Int32,'Two':String>"},
                    {"name": "column14", "type": "Variant<Int32,String>"},
                    {"name": "column15", "type": "Variant<'a':Int32>"},
                    {"name": "column16", "type": "Enum<'monday'>"},
                    {"name": "column17", "type": "Tagged<Int32,'my_tag'>"},
                    {"name": "column18", "type": "Struct<>"},
                    {"name": "column19", "type": "Struct<'a':Int32,'b':Utf8>"},
                    {"name": "column20", "type": "Void"},
                    {"name": "column21", "type": "Null"},
                    {"name": "column22", "type": "Optional<Variant<String,Int32??>?>"},
                    {"name": "column23", "type": "Optional<Variant<String,Int32??>?>"},
                    {"name": "column24", "type": "Optional<Variant<String,Int32?>?>"},
                    {"name": "column25", "type": "Enum<'Bar','Foo'>"},
                    {"name": "column26", "type": "Enum<'Bar','Foo'>"},
                    {"name": "column27", "type": "Tuple<>"},
                    {"name": "column28", "type": "Tuple<Int32,String,Date>"},
                ],
            },
        )

        results = r.results

        assert results == {
            "rows": [
                [
                    [],
                    [1, 2],
                    {},
                    {"abc": 1},
                    {"xyz": 1},
                    None,  # seems like http api doesn't support uuid values
                    "PT15M",
                    datetime(2019, 9, 16, 0, 0),
                    datetime(2019, 9, 16, 10, 46, 5, tzinfo=tzutc()),
                    datetime(2019, 9, 16, 11, 27, 44, 345849, tzinfo=tzutc()),
                    "2019-09-16,Europe/Moscow",
                    "2019-09-16T14:32:40,Europe/Moscow",
                    "2019-09-16T14:32:55.874913,Europe/Moscow",
                    12,
                    "xyz",
                    1,
                    "monday",
                    1,
                    {},
                    {"a": 1, "b": "xyz"},
                    None,
                    None,
                    177,
                    None,
                    None,
                    "Foo",
                    "Bar",
                    [],
                    (1, "privet", datetime(2019, 9, 16, 0, 0)),
                ]
            ],
            "columns": [
                {"name": "column0", "type": "EmptyList"},
                {"name": "column1", "type": "List<Int32>"},
                {"name": "column2", "type": "EmptyDict"},
                {"name": "column3", "type": "Dict<String,Int32>"},
                {"name": "column4", "type": "Dict<Utf8,Int32>"},
                {"name": "column5", "type": "Uuid"},
                {"name": "column6", "type": "Interval"},
                {"name": "column7", "type": "Date"},
                {"name": "column8", "type": "Datetime"},
                {"name": "column9", "type": "Timestamp"},
                {"name": "column10", "type": "TzDate"},
                {"name": "column11", "type": "TzDatetime"},
                {"name": "column12", "type": "TzTimestamp"},
                {"name": "column13", "type": "Variant<'One':Int32,'Two':String>"},
                {"name": "column14", "type": "Variant<Int32,String>"},
                {"name": "column15", "type": "Variant<'a':Int32>"},
                {"name": "column16", "type": "Enum<'monday'>"},
                {"name": "column17", "type": "Tagged<Int32,'my_tag'>"},
                {"name": "column18", "type": "Struct<>"},
                {"name": "column19", "type": "Struct<'a':Int32,'b':Utf8>"},
                {"name": "column20", "type": "Void"},
                {"name": "column21", "type": "Null"},
                {"name": "column22", "type": "Optional<Variant<String,Int32??>?>"},
                {"name": "column23", "type": "Optional<Variant<String,Int32??>?>"},
                {"name": "column24", "type": "Optional<Variant<String,Int32?>?>"},
                {"name": "column25", "type": "Enum<'Bar','Foo'>"},
                {"name": "column26", "type": "Enum<'Bar','Foo'>"},
                {"name": "column27", "type": "Tuple<>"},
                {"name": "column28", "type": "Tuple<Int32,String,Date>"},
            ],
        }
