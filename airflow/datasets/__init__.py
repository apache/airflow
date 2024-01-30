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

import os
from typing import TYPE_CHECKING, Any, Callable, ClassVar
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import attr

from airflow.providers_manager import ProvidersManager

if TYPE_CHECKING:
    from urllib.parse import SplitResult


def normalize_noop(parts: SplitResult) -> SplitResult:
    return parts


def _get_uri_normalizer(scheme: str) -> Callable[[SplitResult], SplitResult] | None:
    if scheme == "file":
        return normalize_noop
    return ProvidersManager().dataset_uri_handlers.get(scheme)


def _sanitize_uri(uri: str) -> str:
    if not uri:
        raise ValueError("Dataset URI cannot be empty")
    if uri.isspace():
        raise ValueError("Dataset URI cannot be just whitespace")
    if not uri.isascii():
        raise ValueError("Dataset URI must only consist of ASCII characters")
    parsed = urlsplit(uri)
    if (normalized_scheme := parsed.scheme.lower()) == "airflow":
        raise ValueError("Dataset scheme 'airflow' is reserved")
    _, auth_exists, normalized_netloc = parsed.netloc.rpartition("@")
    if auth_exists:
        raise ValueError("Dataset URI must not contain auth information")
    if parsed.query:
        normalized_query = urlencode(sorted(parse_qsl(parsed.query)))
    else:
        normalized_query = ""
    parsed = parsed._replace(
        scheme=normalized_scheme,
        netloc=normalized_netloc,
        path=parsed.path.rstrip("/") or "/",  # Remove all trailing slashes.
        query=normalized_query,
        fragment="",  # Ignore any fragments.
    )
    if (normalizer := _get_uri_normalizer(normalized_scheme)) is not None:
        parsed = normalizer(parsed)
    return urlunsplit(parsed)


@attr.define()
class Dataset(os.PathLike):
    """A Dataset is used for marking data dependencies between workflows."""

    uri: str = attr.field(
        converter=_sanitize_uri,
        validator=[attr.validators.min_len(1), attr.validators.max_len(3000)],
    )
    extra: dict[str, Any] | None = None

    __version__: ClassVar[int] = 1

    def __fspath__(self) -> str:
        return self.uri

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.uri == other.uri
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.uri)
