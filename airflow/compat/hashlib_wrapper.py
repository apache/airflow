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

import hashlib
import inspect
from typing import Any

# Check if "usedforsecurity" is available for hashlib
sig = inspect.getfullargspec(hashlib.new)
HAS_USEDFORSECURITY = "usedforsecurity" in sig.kwonlyargs


def md5(data: Any, used_for_security: bool = None):
    """Safely allows calling the hashlib.md5 function with the "usedforsecurity" param.

    Args:
        data (Any): The data to hash.
        used_for_security (bool, optional): The value to pass to the md5 function's "usedforsecurity" param. Defaults to None.

    Returns:
        _Hash: The hashed value.
    """
    if HAS_USEDFORSECURITY and used_for_security is not None:
        return hashlib.md5(data, usedforsecurity=used_for_security)
    else:
        return hashlib.md5(data)
