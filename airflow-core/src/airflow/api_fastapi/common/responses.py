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

"""Custom response classes for optimized JSON serialization."""

from __future__ import annotations

import orjson
from fastapi.responses import JSONResponse


class ORJSONResponse(JSONResponse):
    """
    Custom JSON response using orjson for faster serialization.

    This response class uses orjson which provides ~2x performance improvement
    over standard JSON serialization. It's particularly beneficial for:
    - List endpoints returning multiple records
    - Endpoints with complex nested data structures
    - High-traffic endpoints requiring better throughput

    The response validates data using Pydantic models before serialization
    to ensure data integrity, but skips FastAPI's response model revalidation
    which causes performance overhead.
    """

    media_type = "application/json"

    def render(self, content) -> bytes:
        """
        Serialize content to JSON bytes using orjson.

        Args:
            content: Data to serialize (typically from Pydantic model.model_dump())

        Returns:
            JSON-encoded bytes
        """
        return orjson.dumps(
            content,
            option=orjson.OPT_NON_STR_KEYS | orjson.OPT_UTC_Z | orjson.OPT_SERIALIZE_NUMPY,
        )
