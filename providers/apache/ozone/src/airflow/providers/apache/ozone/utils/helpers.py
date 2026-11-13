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

import fnmatch
import json
import os
import posixpath
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TypeAlias
from urllib.parse import urlsplit, urlunsplit

from airflow.providers.apache.ozone.utils.errors import OzoneProviderError
from airflow.providers.apache.ozone.version_compat import redact

JsonScalar: TypeAlias = "str | int | float | bool | None"
JsonValue: TypeAlias = "JsonScalar | list[JsonValue] | dict[str, JsonValue]"
JsonDict: TypeAlias = "dict[str, JsonValue]"


class URIHelper:
    """URI and wildcard helpers for Ozone paths."""

    WILDCARD_CHARS = ("*", "?", "[")

    @classmethod
    def contains_wildcards(cls, value: str) -> bool:
        """Return True if path contains glob or wildcard characters."""
        return bool(value) and any(ch in value for ch in cls.WILDCARD_CHARS)

    @staticmethod
    def parse_ozone_uri(value: str) -> tuple[str, str, str]:
        """Parse Ozone URI and return (scheme, netloc, path_or_raw)."""
        parsed = urlsplit(value or "")
        if parsed.scheme:
            path = parsed.path or "/"
            if not path.startswith("/"):
                path = "/" + path
            return parsed.scheme, parsed.netloc or "", path
        return "", "", value or ""

    @staticmethod
    def build_ozone_uri(scheme: str, netloc: str, path_or_raw: str) -> str:
        """Build Ozone-style URI from components returned by parse_ozone_uri."""
        if not scheme:
            return path_or_raw
        path = path_or_raw or "/"
        if not path.startswith("/"):
            path = "/" + path
        return urlunsplit((scheme, netloc, path, "", ""))

    @classmethod
    def to_key_uri(cls, value: str) -> str:
        """Return an object-store key URI accepted by `ozone sh key` commands."""
        scheme, netloc, path_or_raw = cls.parse_ozone_uri(value)
        if scheme in {"ofs", "o3fs", "o3"}:
            return cls.build_ozone_uri("o3", netloc, path_or_raw)
        if scheme:
            return value
        return path_or_raw.lstrip("/")

    @classmethod
    def split_ozone_path(cls, value: str) -> tuple[str, str]:
        """
        Split Ozone path into parent directory and basename.

        Returns:
            tuple[parent_path, name]
        """
        target_value = (value or "").rstrip("/")
        if not target_value:
            return "", ""

        scheme, netloc, parsed_path = cls.parse_ozone_uri(target_value)
        parent_raw, name = posixpath.split(parsed_path)

        if not parent_raw:
            return "", name

        if scheme and parent_raw == "/":
            return "", name

        return cls.build_ozone_uri(scheme, netloc, parent_raw), name

    @classmethod
    def join_ozone_path(cls, dir_path: str, name: str) -> str:
        """Join Ozone directory path and child name preserving scheme and netloc."""
        target_dir = (dir_path or "").rstrip("/")
        child_name = (name or "").strip("/")

        if not target_dir:
            return child_name
        if not child_name:
            return target_dir

        scheme, netloc, parsed_path = cls.parse_ozone_uri(target_dir)
        base_path = parsed_path.rstrip("/") or ("/" if scheme else "")
        full_path = posixpath.join(base_path, child_name)
        return cls.build_ozone_uri(scheme, netloc, full_path)

    @classmethod
    def split_ozone_wildcard_path(cls, path: str) -> tuple[str, str]:
        """Split wildcard path into source directory URI and basename pattern."""
        scheme, netloc, uri_path = cls.parse_ozone_uri(path)
        source_dir_path, pattern = posixpath.split(uri_path)
        source_dir_path = source_dir_path or ("/" if scheme else "")
        source_dir = cls.build_ozone_uri(scheme, netloc, source_dir_path or "/")
        return source_dir, pattern

    @staticmethod
    def filter_paths_by_basename_pattern(paths: Sequence[str], pattern: str) -> list[str]:
        """Filter path list by basename using fnmatch semantics."""
        return [path for path in paths if fnmatch.fnmatch(posixpath.basename(path), pattern)]

    @classmethod
    def resolve_wildcard_matches(
        cls,
        wildcard_path: str,
        list_paths_func: Callable[[str], Sequence[str]],
    ) -> tuple[str, list[str]]:
        """Resolve wildcard path into source directory and matching file paths."""
        source_dir, pattern = cls.split_ozone_wildcard_path(wildcard_path)
        listed_paths = list_paths_func(source_dir)
        return source_dir, cls.filter_paths_by_basename_pattern(listed_paths, pattern)


class FileHelper:
    """Filesystem access helpers for local runtime paths."""

    @staticmethod
    def is_readable_file(path: str | Path) -> bool:
        """Return True when path points to an existing readable regular file."""
        target = Path(path)
        return target.is_file() and os.access(target, os.R_OK)

    @staticmethod
    def is_writable_target(path: str | Path) -> bool:
        """
        Return True when target file can be written.

        If target exists, it must be a writable file.
        If target does not exist, nearest existing parent directory must be writable/executable.
        """
        target = Path(path)
        if target.exists():
            return target.is_file() and os.access(target, os.W_OK)

        probe = target.parent if target.parent != Path("") else Path(".")
        while not probe.exists():
            if probe == probe.parent:
                return False
            probe = probe.parent
        return probe.is_dir() and os.access(probe, os.W_OK | os.X_OK)

    @staticmethod
    def get_file_size_bytes(path: str | Path) -> int:
        """Return file size in bytes for an existing local file path, or -1 when unavailable."""
        try:
            return Path(path).stat().st_size
        except OSError:
            return -1


class TypeNormalizationHelper:
    """Normalization and lightweight validation helpers for typed inputs."""

    @staticmethod
    def normalize_optional_str(value: JsonValue) -> str | None:
        """Return stripped string value or None for empty or None input."""
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @classmethod
    def require_optional_non_empty(cls, value: JsonValue, message: str) -> str | None:
        """Validate and normalize optional string input."""
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(message)
        normalized = cls.normalize_optional_str(value)
        if not normalized:
            raise ValueError(message)
        return normalized

    @staticmethod
    def normalize_flag_bool(value: JsonValue, default: bool = False) -> bool:
        """Normalize feature-flag style values used in connection extras/env."""
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "y", "on", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", "disabled"}:
            return False
        return default

    @staticmethod
    def ensure_json_dict(value: object) -> dict[str, JsonValue]:
        """Return dict value when input is a JSON object-like mapping, otherwise empty dict."""
        return value if isinstance(value, dict) else {}

    @staticmethod
    def parse_positive_int(value: JsonValue, default: int) -> int:
        """Parse positive integer value, falling back to default when invalid."""
        if value is None:
            return default
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default

    @staticmethod
    def parse_non_negative_int(value: JsonValue) -> int | None:
        """Parse non-negative integer value, returning None when invalid."""
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value if value >= 0 else None
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None

    @staticmethod
    def require_optional_positive_int(value: int | None, *, field_name: str) -> int | None:
        """Validate optional positive int parameter used by operators."""
        if value is None:
            return None
        if value <= 0:
            raise ValueError(f"{field_name} must be a positive integer when provided.")
        return value

    @staticmethod
    def parse_json_output(output: str) -> JsonValue:
        """Parse CLI output that may contain plain JSON or logs followed by JSON."""
        raw_output = (output or "").strip()
        if not raw_output:
            raise OzoneProviderError("Empty JSON output.")

        try:
            return json.loads(raw_output)
        except json.JSONDecodeError:
            pass

        decoder = json.JSONDecoder()
        for index, char in enumerate(raw_output):
            if char not in "[{":
                continue
            try:
                parsed, _ = decoder.raw_decode(raw_output, idx=index)
                return parsed
            except json.JSONDecodeError:
                continue

        raise OzoneProviderError(f"Failed to parse JSON output: {redact(raw_output)}")
