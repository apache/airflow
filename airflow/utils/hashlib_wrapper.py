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

from airflow import PY39


def md5(data: bytes, *, usedforsecurity: bool | None = None):
    """
    Safely allows calling the hashlib.md5 function with the "usedforsecurity" param.
    :param data: The data to hash.
    :param usedforsecurity: The value to pass to the md5 function's "usedforsecurity" param.
        Defaults to None.
    :return: The hashed value.
    :rtype: _Hash
    """
    if PY39 and usedforsecurity is not None:
        return hashlib.md5(data, usedforsecurity=usedforsecurity)  # type: ignore
    else:
        return hashlib.md5(data)
