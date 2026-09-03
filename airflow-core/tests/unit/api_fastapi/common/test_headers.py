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

import typing

import pytest
from fastapi import HTTPException

from airflow.api_fastapi.common.headers import (
    header_accept_json_or_ndjson_depends,
    header_accept_json_or_text_depends,
)
from airflow.api_fastapi.common.types import Mimetype


def _json_schema_extra_for(accept_header_depends_func) -> dict:
    """Pull the ``json_schema_extra`` dict off the ``Header(...)`` FieldInfo for the
    ``accept`` parameter of one of the header dependency functions, i.e. what actually
    ends up in the generated OpenAPI spec for that parameter's schema.
    """
    hints = typing.get_type_hints(accept_header_depends_func, include_extras=True)
    header_field_info = hints["accept"].__metadata__[0]
    return header_field_info.json_schema_extra


class TestAcceptHeaderOpenApiSchema:
    """Regression test for https://github.com/apache/airflow/issues/72466.

    The Accept header's OpenAPI schema must not declare its sample values via
    "enum". Some OpenAPI client generators (notably the Java generator) render
    each enum value as its own named constant, with a Javadoc comment built
    from that literal value - "*/*" contains "*/", which prematurely closes
    the generated Javadoc comment block and produces invalid Java.
    """

    @pytest.mark.parametrize(
        "accept_header_depends_func",
        [header_accept_json_or_text_depends, header_accept_json_or_ndjson_depends],
    )
    def test_schema_uses_examples_not_enum(self, accept_header_depends_func):
        schema_extra = _json_schema_extra_for(accept_header_depends_func)

        assert "enum" not in schema_extra
        assert "examples" in schema_extra
        # The literal that broke Java client generation must still be documented
        # somewhere (as a non-binding example), just not as an enum member.
        assert Mimetype.ANY in schema_extra["examples"]

    @pytest.mark.parametrize(
        "accept_header_depends_func",
        [header_accept_json_or_text_depends, header_accept_json_or_ndjson_depends],
    )
    def test_default_is_still_any(self, accept_header_depends_func):
        # The "default" the OpenAPI schema advertises comes from the function's
        # default parameter value, not from json_schema_extra - make sure switching
        # enum -> examples didn't accidentally drop or change it.
        assert accept_header_depends_func.__defaults__ == (Mimetype.ANY,)


class TestHeaderAcceptJsonOrText:
    @pytest.mark.parametrize(
        ("accept", "expected"),
        [
            (Mimetype.ANY, Mimetype.ANY),
            (Mimetype.JSON, Mimetype.JSON),
            (Mimetype.TEXT, Mimetype.TEXT),
            ("application/json; charset=utf-8", Mimetype.JSON),
        ],
    )
    def test_negotiates_supported_types(self, accept, expected):
        assert header_accept_json_or_text_depends(accept=accept) == expected

    def test_rejects_unsupported_type(self):
        with pytest.raises(HTTPException) as exc_info:
            header_accept_json_or_text_depends(accept="application/xml")
        assert exc_info.value.status_code == 406


class TestHeaderAcceptJsonOrNdjson:
    @pytest.mark.parametrize(
        ("accept", "expected"),
        [
            # */* is matched by the function's own first branch and returned as-is;
            # the ANY check in the ndjson branch below is unreachable for that reason.
            (Mimetype.ANY, Mimetype.ANY),
            (Mimetype.JSON, Mimetype.JSON),
            (Mimetype.NDJSON, Mimetype.NDJSON),
        ],
    )
    def test_negotiates_supported_types(self, accept, expected):
        assert header_accept_json_or_ndjson_depends(accept=accept) == expected

    def test_rejects_unsupported_type(self):
        with pytest.raises(HTTPException) as exc_info:
            header_accept_json_or_ndjson_depends(accept="application/xml")
        assert exc_info.value.status_code == 406
