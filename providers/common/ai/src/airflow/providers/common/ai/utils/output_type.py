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
"""Helpers for handling pydantic-ai ``output_type`` shapes."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, get_args, get_origin

from pydantic import BaseModel


def iter_base_model_classes(output_type: Any) -> Iterator[type[BaseModel]]:
    """
    Yield every Pydantic ``BaseModel`` subclass reachable from ``output_type``.

    pydantic-ai accepts ``output_type`` as a single class, a ``Union`` /
    ``Optional`` of classes, a list of classes (multi-output), or a parametrised
    generic such as ``list[MyModel]``. The agent may return an instance of any
    ``BaseModel`` reachable from the type expression, so each must be registered
    for XCom deserialization, not just the top-level ``output_type``.
    """
    seen: set[type] = set()
    stack: list[Any] = [output_type]
    while stack:
        t = stack.pop()
        # ``list[A]`` returns ``True`` for ``isinstance(t, type)`` on Python 3.10+
        # but has a non-None ``get_origin``; check origin first so we recurse
        # into its args instead of treating ``list[A]`` as a leaf type.
        origin = get_origin(t)
        if origin is not None:
            stack.extend(get_args(t))
            continue
        if isinstance(t, type):
            if t in seen:
                continue
            seen.add(t)
            if issubclass(t, BaseModel):
                yield t
