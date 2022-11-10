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
"""
This module provides helper code to make type annotation within Airflow
codebase easier.
"""
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

if sys.version_info >= (3, 8):
    from typing import Protocol, TypedDict, runtime_checkable
else:
    from typing_extensions import Protocol, TypedDict, runtime_checkable

# Literal in 3.8 is limited to one single argument, not e.g. "Literal[1, 2]".
if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal

if sys.version_info >= (3, 10):
    from typing import ParamSpec, TypeGuard
else:
    from typing_extensions import ParamSpec, TypeGuard
