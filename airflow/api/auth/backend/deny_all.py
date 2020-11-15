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
"""Authentication backend that denies all requests"""
from functools import wraps
from typing import Callable, Optional, Tuple, TypeVar, Union, cast

from flask import Response
from requests.auth import AuthBase

CLIENT_AUTH: Optional[Union[Tuple[str, str], AuthBase]] = None


def init_app(_):
    """Initializes authentication"""


T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def requires_authentication(function: T):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):  # pylint: disable=unused-argument
        return Response("Forbidden", 403)

    return cast(T, decorated)
