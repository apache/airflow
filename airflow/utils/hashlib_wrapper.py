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
from airflow.configuration import conf


def md5(data: bytes = b""):
    """
    Safely allows calling the hashlib.md5 function with the "usedforsecurity" disabled
    when specified in the configuration.
    :param data: The data to hash.
        Default to empty str.
    :return: The hashed value.
    :rtype: _Hash
    """
    if PY39 and conf.getboolean("security", "disable_md5_for_security"):
        return hashlib.md5(data, usedforsecurity=False)  # type: ignore
    return hashlib.md5(data)
