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

import json
import locale
from base64 import b64encode
from contextlib import suppress
from datetime import datetime
from json import JSONDecodeError
from typing import Any
from uuid import UUID

import pendulum


class ResponseSerializer:
    """
    ResponseSerializer serializes the response as a string.
    """

    def __init__(self, encoding: (str, None) = None):
        self.encoding = encoding or locale.getpreferredencoding()

    def serialize(self, response) -> (str, None):
        def convert(value) -> (str, None):
            if value is not None:
                if isinstance(value, UUID):
                    return str(value)
                if isinstance(value, datetime):
                    return value.isoformat()
                if isinstance(value, pendulum.DateTime):
                    return value.to_iso8601_string()  # Adjust the format as needed
                raise TypeError(f"Object of type {type(value)} is not JSON serializable!")
            return None

        if response is not None:
            if isinstance(response, bytes):
                return b64encode(response).decode(self.encoding)
            with suppress(JSONDecodeError):
                return json.dumps(response, default=convert)
            return response
        return None

    def deserialize(self, response) -> Any:
        if isinstance(response, str):
            with suppress(JSONDecodeError):
                response = json.loads(response)
        return response
