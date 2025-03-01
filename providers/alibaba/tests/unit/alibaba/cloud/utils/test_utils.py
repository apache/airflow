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

from functools import wraps
from inspect import signature
from typing import Callable, TypeVar, cast

T = TypeVar("T", bound=Callable)


def skip_test_if_no_valid_conn_id(func: T) -> T:
    """
    Function decorator that skip this test function if no valid connection id is specified.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> None:
        bound_args = function_signature.bind(*args, **kwargs)
        self = args[0]

        if self.hook is not None:
            return func(*bound_args.args, **bound_args.kwargs)
        else:
            return None

    return cast(T, wrapper)
