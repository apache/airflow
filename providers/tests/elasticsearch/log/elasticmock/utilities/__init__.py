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

#
# The MIT License (MIT)
#
# Copyright (c) 2016 Marcos Cardoso
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""Utilities for Elastic mock"""
import base64
import random
import string
from datetime import date, datetime
from functools import wraps

from elasticsearch.exceptions import NotFoundError

DEFAULT_ELASTICSEARCH_ID_SIZE = 20
CHARSET_FOR_ELASTICSEARCH_ID = string.ascii_letters + string.digits
GLOBAL_PARAMS = ("pretty", "human", "error_trace", "format", "filter_path")


def get_random_id(size=DEFAULT_ELASTICSEARCH_ID_SIZE):
    """Returns random if for elasticsearch"""
    return "".join(random.choices(CHARSET_FOR_ELASTICSEARCH_ID, k=size))


def query_params(*es_query_params, **kwargs):
    """
    Decorator that pops all accepted parameters from method's kwargs and puts
    them in the params argument.
    """
    body_params = kwargs.pop("body_params", None)
    body_only_params = set(body_params or ()) - set(es_query_params)
    body_name = kwargs.pop("body_name", None)
    body_required = kwargs.pop("body_required", False)
    type_possible_in_params = "type" in es_query_params

    assert not (body_name and body_params)

    assert not (body_name and body_required)
    assert not body_required or body_params

    def _wrapper(func):
        @wraps(func)
        def _wrapped(*args, **kwargs):
            params = (kwargs.pop("params", None) or {}).copy()
            headers = {k.lower(): v for k, v in (kwargs.pop("headers", None) or {}).copy().items()}

            if "opaque_id" in kwargs:
                headers["x-opaque-id"] = kwargs.pop("opaque_id")

            http_auth = kwargs.pop("http_auth", None)
            api_key = kwargs.pop("api_key", None)

            using_body_kwarg = kwargs.get("body", None) is not None
            using_positional_args = args and len(args) > 1

            if type_possible_in_params:
                doc_type_in_params = params and "doc_type" in params
                doc_type_in_kwargs = "doc_type" in kwargs

                if doc_type_in_params:
                    params["type"] = params.pop("doc_type")
                if doc_type_in_kwargs:
                    kwargs["type"] = kwargs.pop("doc_type")

            if using_body_kwarg or using_positional_args:
                body_only_params_in_use = body_only_params.intersection(kwargs)
                if body_only_params_in_use:
                    params_prose = "', '".join(sorted(body_only_params_in_use))
                    plural_params = len(body_only_params_in_use) > 1

                    msg = (
                        f"The '{params_prose}' parameter{'s' if plural_params else ''} "
                        f"{'are' if plural_params else 'is'} only serialized in the "
                        f"request body and can't be combined with the 'body' parameter. "
                        f"Either stop using the 'body' parameter and use keyword-arguments "
                        f"only or move the specified parameters into the 'body'. "
                        f"See https://github.com/elastic/elasticsearch-py/issues/1698 "
                        f"for more information"
                    )
                    raise TypeError(msg)

            elif set(body_params or ()).intersection(kwargs):
                body = {}
                for param in body_params:
                    value = kwargs.pop(param, None)
                    if value is not None:
                        body[param.rstrip("_")] = value
                kwargs["body"] = body

            elif body_required:
                kwargs["body"] = {}

            if body_name:
                if body_name in kwargs:
                    if using_body_kwarg:
                        msg = (
                            f"Can't use '{body_name}' and 'body' parameters together"
                            f" because '{body_name}' is an alias for 'body'. "
                            f"Instead you should only use the '{body_name}' "
                            f"parameter. See https://github.com/elastic/elasticsearch-py/issues/1698 "
                            f"for more information"
                        )
                        raise TypeError(msg)
                    kwargs["body"] = kwargs.pop(body_name)

            if http_auth is not None and api_key is not None:
                raise ValueError("Only one of 'http_auth' and 'api_key' may be passed at a time")
            elif http_auth is not None:
                headers["authorization"] = f"Basic {_base64_auth_header(http_auth)}"
            elif api_key is not None:
                headers["authorization"] = f"ApiKey {_base64_auth_header(api_key)}"

            for p in es_query_params + GLOBAL_PARAMS:
                if p in kwargs:
                    v = kwargs.pop(p)
                    if v is not None:
                        params[p] = _escape(v)

            for p in ("ignore", "request_timeout"):
                if p in kwargs:
                    params[p] = kwargs.pop(p)
            return func(*args, params=params, headers=headers, **kwargs)

        return _wrapped

    return _wrapper


def to_str(x, encoding="ascii"):
    if not isinstance(x, str):
        return x.decode(encoding)
    return x


def to_bytes(x, encoding="ascii"):
    if not isinstance(x, bytes):
        return x.encode(encoding)
    return x


def _base64_auth_header(auth_value):
    """Takes either a 2-tuple or a base64-encoded string
    and returns a base64-encoded string to be used
    as an HTTP authorization header.
    """
    if isinstance(auth_value, (list, tuple)):
        auth_value = base64.b64encode(to_bytes(":".join(auth_value)))
    return to_str(auth_value)


def _escape(value):
    """
    Escape a single value of a URL string or a query parameter. If it is a list
    or tuple, turn it into a comma-separated string first.
    """

    # make sequences into comma-separated strings
    if isinstance(value, (list, tuple)):
        value = ",".join(value)

    # dates and datetimes into isoformat
    elif isinstance(value, (date, datetime)):
        value = value.isoformat()

    # make bools into true/false strings
    elif isinstance(value, bool):
        value = str(value).lower()

    # don't decode bytestrings
    elif isinstance(value, bytes):
        return value

    # encode strings to utf-8
    if isinstance(value, str):
        return value.encode("utf-8")

    return str(value)


class MissingIndexException(NotFoundError):
    """Exception representing a missing index."""

    def __init__(self, msg, query):
        self.msg = msg
        self.query = query

    def __str__(self):
        return f"IndexMissingException[[{self.msg}] missing] with query {self.query}"


class SearchFailedException(NotFoundError):
    """Exception representing a search failure."""

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"SearchFailedException: {self.msg}"
