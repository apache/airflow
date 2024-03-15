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

from typing import TYPE_CHECKING, Any, Callable

from kiota_abstractions.response_handler import NativeResponseType, ResponseHandler

if TYPE_CHECKING:
    from kiota_abstractions.serialization import ParsableFactory


class CallableResponseHandler(ResponseHandler):
    """
    CallableResponseHandler executes the passed callable_function with response as parameter.

    :param callable_function: Function which allows you to handle the response before returning.
    """

    def __init__(
        self,
        callable_function: Callable[[NativeResponseType, dict[str, ParsableFactory | None] | None], Any],
    ):
        self.callable_function = callable_function

    async def handle_response_async(
        self, response: NativeResponseType, error_map: dict[str, ParsableFactory | None] | None = None
    ) -> Any:
        return self.callable_function(response, error_map)
