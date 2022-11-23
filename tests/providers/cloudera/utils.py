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

"""Utils module for common utility methods used in the tests"""
from __future__ import annotations

from itertools import tee
from json import dumps
from typing import Any, Iterable

from requests import Response


def iter_len_plus_one(iterator: Iterable) -> int:
    """Return the length + 1 of the given iterator.
    The +1 is because in the tests the first side effect is already consumed"""
    return sum(1 for _ in tee(iterator)) + 1


def _get_call_arguments(self: tuple) -> dict[str, Any]:
    if len(self) == 2:
        # returned tuple is args, kwargs = self
        _, kwargs = self
    else:
        # returned tuple is name, args, kwargs = self
        _, _, kwargs = self

    return kwargs


def _make_response(status: int, body: dict | str | None, reason: str) -> Response:

    content = (
        None
        if body is None
        else dumps(body).encode("utf-8")
        if isinstance(body, dict)
        else body.encode("utf-8")
    )

    resp = Response()
    resp.status_code = status
    resp.encoding = "utf-8"
    resp._content = content
    resp.reason = reason
    return resp
