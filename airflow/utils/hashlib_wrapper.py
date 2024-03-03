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
from __future__ import annotations

import hashlib
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer


def md5(__string: ReadableBuffer = b"") -> hashlib._Hash:
    """
    Safely allows calling the ``hashlib.md5`` function when ``usedforsecurity`` is disabled in configuration.

    :param __string: The data to hash. Default to empty str byte.
    :return: The hashed value.
    """
    if sys.version_info >= (3, 9):
        return hashlib.md5(__string, usedforsecurity=False)  # type: ignore
    return hashlib.md5(__string)
