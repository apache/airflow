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

import inspect
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from snowflake.snowpark import Session


def inject_session_into_op_kwargs(
    python_callable: Callable, op_kwargs: dict, session: Session | None
) -> dict:
    """
    Inject Snowpark session into operator kwargs based on signature of python callable.

    If there is a keyword argument named `session` in the signature of the python callable,
    a Snowpark session object will be injected into kwargs.

    :param python_callable: Python callable
    :param op_kwargs: Operator kwargs
    :param session: Snowpark session
    """
    signature = inspect.signature(python_callable)
    if "session" in signature.parameters:
        return {**op_kwargs, "session": session}
    else:
        return op_kwargs
