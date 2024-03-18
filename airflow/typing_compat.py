#
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
"""This module provides helper code to make type annotation within Airflow codebase easier."""

from __future__ import annotations

__all__ = [
    "Literal",
    "ParamSpec",
    "Protocol",
    "TypedDict",
    "TypeGuard",
    "runtime_checkable",
]

import sys
from typing import Protocol, TypedDict, runtime_checkable

# Literal from typing module has various issues in different Python versions, see:
# - https://typing-extensions.readthedocs.io/en/latest/#Literal
# - bpo-45679: https://github.com/python/cpython/pull/29334
# - bpo-42345: https://github.com/python/cpython/pull/23294
# - bpo-42345: https://github.com/python/cpython/pull/23383
if sys.version_info >= (3, 10, 1) or (3, 9, 8) <= sys.version_info < (3, 10):
    from typing import Literal
else:
    from typing_extensions import Literal  # type: ignore[assignment]

if sys.version_info >= (3, 10):
    from typing import ParamSpec, TypeGuard
else:
    from typing_extensions import ParamSpec, TypeGuard
