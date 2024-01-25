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
import urllib.parse
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Iterator, cast

import attrs

from airflow.providers_manager import ProvidersManager

if TYPE_CHECKING:
    from airflow.typing_compat import Self


@attrs.define(kw_only=True, slots=True)
class Uri:
    """Dataset URI."""

    scheme: str
    netloc: str
    path: str  # Includes the leading slash if present.
    query: str

    @classmethod
    def parse(cls, uri: str) -> Self:
        if uri.isspace():
            raise ValueError("Dataset URI cannot be just whitespace")
        if not uri.isascii():
            raise ValueError("Dataset URI must only consist of ASCII characters")
        parsed = urllib.parse.urlsplit(uri)
        if (normalized_scheme := parsed.scheme.lower()) == "airflow":
            raise ValueError("Dataset scheme 'airflow' is reserved")
        _, auth_exists, normalized_netloc = parsed.netloc.rpartition("@")
        if auth_exists:
            raise ValueError("Dataset URI must not contain auth information")
        return cls(
            scheme=normalized_scheme,
            netloc=normalized_netloc,
            path=parsed.path.rstrip("/") or "/",  # Remove all trailing slashes.
            query=parsed.query,
        )

    def __str__(self) -> str:
        """Build the URI back to a string."""

        def _iter_parts() -> Iterator[str]:
            if self.scheme:
                yield self.scheme
                yield ":"
            if self.netloc:
                yield "//"
                yield self.netloc
            yield self.path
            if self.query:
                yield "?"
                yield self.query

        return "".join(_iter_parts())

    @property
    def hostname(self) -> str | None:
        """Parsed out hostname value from netloc.

        Unfortunately ``urllib.parse`` does not expose the parsing logic
        directly, so we build the URI back and ask it to parse again.
        """
        return urllib.parse.urlsplit(str(self)).hostname

    @property
    def port(self) -> int | None:
        """Parsed out port value from netloc.

        Unfortunately ``urllib.parse`` does not expose the parsing logic
        directly, so we build the URI back and ask it to parse again.

        According to ``urlsplit``, this may raise ValueError if the port value
        is not a valid number.
        """
        return urllib.parse.urlsplit(str(self)).port

    def replace(self, **kwargs) -> Self:
        """Create a new instance with parts replaced."""
        return attrs.evolve(
            cast(Any, self),  # Attrs stubs do not handle this correctly.
            **kwargs,
        )


def normalize_noop(parts: Uri) -> Uri:
    return parts


def _get_uri_normalizer(scheme: str) -> Callable[[Uri], Uri] | None:
    if scheme == "file":
        return normalize_noop
    return ProvidersManager().dataset_uri_handlers.get(scheme)


def _sanitize_uri(inp: str) -> str:
    uri = Uri.parse(inp)
    if (normalizer := _get_uri_normalizer(uri.scheme)) is not None:
        uri = normalizer(uri)
    return str(uri)


@attrs.define()
class Dataset(os.PathLike):
    """A Dataset is used for marking data dependencies between workflows."""

    uri: str = attrs.field(
        converter=_sanitize_uri,
        validator=[attrs.validators.min_len(1), attrs.validators.max_len(3000)],
    )
    extra: dict[str, Any] | None = None

    __version__: ClassVar[int] = 1

    def __fspath__(self):
        return self.uri

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.uri == other.uri
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.uri)
