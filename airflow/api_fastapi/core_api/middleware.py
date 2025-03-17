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
import re
from json import JSONDecodeError
from typing import Any

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from airflow.api_fastapi.core_api.datamodels.common import RegexpMiddlewareResponse
from airflow.configuration import conf


# Custom Middleware Class
class FlaskExceptionsMiddleware(BaseHTTPMiddleware):
    """Middleware that converts exceptions thrown in the Flask application to Fastapi exceptions."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Check if the WSGI response contains an error
        if response.status_code >= 400 and response.media_type == "application/json":
            body = await response.json()
            if "error" in body:
                # Transform the WSGI app's exception into a FastAPI HTTPException
                raise HTTPException(
                    status_code=response.status_code,
                    detail=body["error"],
                )
        return response


class RegexpExceptionMiddleware(BaseHTTPMiddleware):
    """Middleware that converts exceptions response if any field in the request contains regexp pattern."""

    @classmethod
    def _detect_regexp(cls, key: str | None = None, value: Any = None) -> str | None:
        """Return the key value if the value contains a regexp pattern."""
        # Common evil regex patterns
        common_evil_regex = [
            # Three common regex structures
            # Please be proactive if there are important regex structures that are not included here
            "(a+)+",  # Nesting quantifiers
            "(a|a)+",  # Quantified overlapping disjunctions
            r"\d+\d+",  # Quantified Overlapping Adjacencies
        ]
        # There are infinite ways to write a regex pattern, so we are checking for common regex structures
        # If the value contains any of the common regex structures, we will consider it as a regex pattern
        regex_structures = [
            # Three common regex structures
            r"\[.*?\]",  # Character classes
            r"\(.*?\)",  # Grouping
            r"\{.*?,.*?\}",  # Quantifiers with ranges
            r"\^.*\$",  # start and end anchors
            r"\|",  # or operator
            r"\(\?.*?\)",  # non-capturing groups
            r"\.\*",  # common wildcard
            r"\.\+",
            r"\.\?",
            r"\\A",  # start of string
            r"\\b",  # word boundary
            r"\\B",  # non-word boundary
            r"\\d",  # digit
            r"\\D",  # non-digit
            r"\\s",  # whitespace
            r"\\S",  # non-whitespace
            r"\\w",  # word character
            r"\\W",  # non-word character
            r"\\Z",  # end of string
            r"\*",  # quantifier
            r"\+",  # quantifier
            r"\?",  # quantifier
            r"\\.",  # escaped dot
            r"\\-",  # escaped dash
            r"\\/",  # escaped slash
            r"\\\\",  # escaped backslash
        ]

        # Date-time pattern regex to exclude
        date_time_pattern = r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?)?(Z|[\+-]\d{2}:\d{2})?$"

        # Excluded keys to avoid checking for fields that can contain regexp like patterns
        # password and extra are excluded as they can contain any string for connections.
        # We should always ensure these fields not accepting regexp and validate accordingly.
        excluded_keys: set = {"password", "extra"}

        compiled_structures = [re.compile(pattern) for pattern in regex_structures]

        if isinstance(value, str):
            # Early return if the value is empty or the key is in the excluded keys
            if key in excluded_keys or value == "":
                return None
            # Check if the string is a valid JSON and call the function recursively
            try:
                dict_candidate = json.loads(value)
                return cls._detect_regexp(key=key, value=dict_candidate)
            except JSONDecodeError:
                pass

            # Include matching regex indicators and exclude date-time pattern
            if (
                any(structure.search(value) for structure in compiled_structures)
                or any(value in evil_regex for evil_regex in common_evil_regex)
            ) and not re.match(date_time_pattern, value):
                try:
                    re.compile(value)
                    return key
                except re.error:
                    pass
        elif isinstance(value, dict):
            # Call the function recursively for the dict values
            for key, dict_value in value.items():
                if not value:
                    continue
                found_key = cls._detect_regexp(key=key, value=dict_value)
                if found_key:
                    return found_key

        return None

    async def dispatch(self, request: Request, call_next):
        """Check if the request contains a regexp pattern in any of the fields."""
        # Bypass the middleware if the disable_regexp_middleware is set to True
        if conf.get("api", "disable_regexp_middleware") == "True":
            return await call_next(request)

        # Bypass the middleware if the request path is in the bypass list
        bypass_list = conf.get("api", "disable_regexp_middleware_paths").split(",")
        for bypass in bypass_list:
            if bypass in request.url.path:
                return await call_next(request)

        # Get the request payload and query parameters
        try:
            payload = await request.json()
        except JSONDecodeError:
            payload = {}

        params = request.query_params

        # Check if the payload or query parameters contain a regexp pattern
        found_key = self._detect_regexp(value=params) or self._detect_regexp(value=payload)

        # Return a response if a regexp pattern is found
        if found_key:
            return Response(
                status_code=400,
                content=RegexpMiddlewareResponse.model_validate(
                    {
                        "field": found_key,
                        "detail": f"Regex pattern detected in field '{found_key}'. It is not allowed.",
                    }
                ).model_dump_json(),
            )

        # Continue with the request if no regexp pattern is found
        return await call_next(request)
