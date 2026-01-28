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

import datetime
import logging
import os
import re
import zipfile
from collections.abc import Callable, Collection, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import jinja2
import jinja2.nativetypes
import jinja2.sandbox

from airflow.sdk import ObjectStoragePath
from airflow.sdk.definitions._internal.mixins import ResolveMixin
from airflow.sdk.definitions.context import render_template_as_native, render_template_to_string

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.types import Operator


# Regex pattern to detect zip file paths: matches "path/to/archive.zip/inner/path"
ZIP_REGEX = re.compile(rf"((.*\.zip){re.escape(os.sep)})?(.*)")


@dataclass(frozen=True)
class LiteralValue(ResolveMixin):
    """
    A wrapper for a value that should be rendered as-is, without applying jinja templating to its contents.

    :param value: The value to be rendered without templating
    """

    value: Any

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        return ()

    def resolve(self, context: Context) -> Any:
        return self.value


log = logging.getLogger(__name__)


# This loader addresses the issue where template files in zipped DAG packages
# could not be resolved by the standard FileSystemLoader.
# See: https://github.com/apache/airflow/issues/59310
class ZipAwareFileSystemLoader(jinja2.FileSystemLoader):
    """
    A Jinja2 template loader that supports resolving templates from zipped DAG packages.

    Search paths may include filesystem directories, zip files, or subdirectories
    within zip files. Searchpath ordering is preserved across zip and non-zip entries.
    """

    def __init__(
        self,
        searchpath: str | os.PathLike[str] | Sequence[str | os.PathLike[str]],
        encoding: str = "utf-8",
        followlinks: bool = False,
    ) -> None:
        # Convert to list first to process
        if isinstance(searchpath, (str, os.PathLike)):
            searchpath = [searchpath]
        all_paths = [os.fspath(p) for p in searchpath]

        # Separate zip paths from regular paths at initialization time (once)
        # Store zip info by index to preserve searchpath order
        self._zip_path_map: dict[int, tuple[str, str]] = {}  # {index: (archive_path, internal_base_path)}
        regular_paths: list[str] = []

        for idx, path in enumerate(all_paths):
            zip_info = self._parse_zip_path(path)
            if zip_info:
                self._zip_path_map[idx] = zip_info
            else:
                regular_paths.append(path)

        # Store regular paths for filesystem lookups
        self._regular_searchpaths = regular_paths

        # Initialize parent with regular paths only (empty list is OK for our use case)
        # We override get_source anyway, so parent's searchpath is only used for list_templates
        super().__init__(regular_paths if regular_paths else [], encoding, followlinks)

        # Store all paths for reference and error messages
        self._all_searchpaths = all_paths
        self.searchpath = all_paths

    @staticmethod
    def _parse_zip_path(path: str) -> tuple[str, str] | None:
        """
        Parse a path to extract zip archive and internal path components.

        :param path: The path to parse
        :return: Tuple of (archive_path, internal_base_path) if path is a zip path,
                 None otherwise
        """
        # Check if the path itself is a zip file (no internal path)
        if path.endswith(".zip") and os.path.isfile(path) and zipfile.is_zipfile(path):
            return (path, "")

        # Check for paths inside a zip (e.g., "archive.zip/subdir")
        match = ZIP_REGEX.search(path)
        if match:
            _, archive, internal = match.groups()
            if archive and os.path.isfile(archive) and zipfile.is_zipfile(archive):
                return (archive, internal or "")

        return None

    def _read_from_zip(self, archive_path: str, internal_path: str) -> str:
        """
        Read a file from inside a zip archive.

        :param archive_path: Path to the zip file
        :param internal_path: Path to the file inside the zip
        :return: The file contents as a string
        :raises TemplateNotFound: If the file doesn't exist in the zip
        """
        try:
            with zipfile.ZipFile(archive_path, "r") as zf:
                # Normalize path separators for zip (always forward slashes)
                normalized_path = internal_path.replace(os.sep, "/")
                with zf.open(normalized_path) as f:
                    return f.read().decode(self.encoding)
        except KeyError as exc:
            raise jinja2.TemplateNotFound(internal_path) from exc
        except (OSError, zipfile.BadZipFile) as exc:
            raise jinja2.TemplateNotFound(
                f"{internal_path} (error reading from {archive_path}: {exc})"
            ) from exc

    def _get_source_from_single_zip(
        self, archive_path: str, base_internal_path: str, template: str
    ) -> tuple[str, str, Callable[[], bool]] | None:
        """
        Try to get template source from a single zip archive.

        :param archive_path: Path to the zip file
        :param base_internal_path: Base path inside the zip (may be empty)
        :param template: The name of the template to load
        :return: A tuple of (source, filename, up_to_date_func) if found, None otherwise
        """
        import posixpath

        from jinja2.loaders import split_template_path

        pieces = split_template_path(template)
        if base_internal_path:
            internal_path = posixpath.join(base_internal_path, *pieces)
        else:
            internal_path = "/".join(pieces)

        try:
            source = self._read_from_zip(archive_path, internal_path)
            filename = os.path.join(archive_path, internal_path)

            archive_mtime = os.path.getmtime(archive_path)

            def up_to_date(archive: str = archive_path, mtime: float = archive_mtime) -> bool:
                try:
                    return os.path.getmtime(archive) == mtime
                except OSError:
                    return False

            return source, filename, up_to_date
        except jinja2.TemplateNotFound:
            return None

    def _get_source_from_filesystem(
        self, searchpath: str, template: str
    ) -> tuple[str, str, Callable[[], bool]] | None:
        """
        Try to get template source from a single filesystem path.

        :param searchpath: The directory to search in
        :param template: The name of the template to load
        :return: A tuple of (source, filename, up_to_date_func) if found, None otherwise
        """
        from jinja2.loaders import split_template_path

        pieces = split_template_path(template)
        filename = os.path.join(searchpath, *pieces)

        if not os.path.isfile(filename):
            return None

        try:
            with open(filename, encoding=self.encoding) as f:
                contents = f.read()

            mtime = os.path.getmtime(filename)

            def up_to_date(filepath: str = filename, file_mtime: float = mtime) -> bool:
                try:
                    return os.path.getmtime(filepath) == file_mtime
                except OSError:
                    return False

            return contents, os.path.normpath(filename), up_to_date
        except OSError:
            return None

    def get_source(
        self, environment: jinja2.Environment, template: str
    ) -> tuple[str, str, Callable[[], bool]]:
        """
        Get the template source, filename, and reload helper for a template.

        Searches through searchpaths in order, handling both zip archives and
        regular filesystem paths according to their original order.

        :param environment: The Jinja2 environment
        :param template: The name of the template to load
        :return: A tuple of (source, filename, up_to_date_func)
        :raises TemplateNotFound: If the template cannot be found
        """
        regular_path_idx = 0

        for idx, _path in enumerate(self._all_searchpaths):
            if idx in self._zip_path_map:
                archive_path, base_internal_path = self._zip_path_map[idx]
                result = self._get_source_from_single_zip(archive_path, base_internal_path, template)
                if result:
                    return result
            else:
                if regular_path_idx < len(self._regular_searchpaths):
                    result = self._get_source_from_filesystem(
                        self._regular_searchpaths[regular_path_idx], template
                    )
                    regular_path_idx += 1
                    if result:
                        return result

        # Template not found in any searchpath
        raise jinja2.TemplateNotFound(
            f"'{template}' not found in search path: {', '.join(repr(p) for p in self._all_searchpaths)}"
        )

    def list_templates(self) -> list[str]:
        """
        Return a list of available templates.

        Combines templates from both zip archives and regular filesystem paths.

        :return: A sorted list of template names
        """
        found: set[str] = set()

        # Get templates from zip paths
        for archive_path, base_internal_path in self._zip_path_map.values():
            try:
                with zipfile.ZipFile(archive_path, "r") as zf:
                    for name in zf.namelist():
                        # Skip directories
                        if name.endswith("/"):
                            continue
                        if base_internal_path:
                            prefix = base_internal_path.replace(os.sep, "/") + "/"
                            if name.startswith(prefix):
                                relative = name[len(prefix) :]
                                found.add(relative)
                        else:
                            found.add(name)
            except (OSError, zipfile.BadZipFile):
                continue

        # Get templates from regular paths
        for searchpath in self._regular_searchpaths:
            if not os.path.isdir(searchpath):
                continue
            for dirpath, _, filenames in os.walk(searchpath, followlinks=self.followlinks):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    relative = os.path.relpath(filepath, searchpath)
                    found.add(relative.replace(os.sep, "/"))

        return sorted(found)


class Templater:
    """
    This renders the template fields of object.

    :meta private:
    """

    # For derived classes to define which fields will get jinjaified.
    template_fields: Collection[str]
    # Defines which files extensions to look for in the templated fields.
    template_ext: Sequence[str]

    def get_template_env(self, dag: DAG | None = None) -> jinja2.Environment:
        """Fetch a Jinja template environment from the Dag or instantiate empty environment if no Dag."""
        # This is imported locally since Jinja2 is heavy and we don't need it
        # for most of the functionalities. It is imported by get_template_env()
        # though, so we don't need to put this after the 'if dag' check.

        if dag:
            return dag.get_template_env(force_sandboxed=False)
        return SandboxedEnvironment(cache_size=0)

    def prepare_template(self) -> None:
        """
        Execute after the templated fields get replaced by their content.

        If you need your object to alter the content of the file before the
        template is rendered, it should override this method to do so.
        """

    def resolve_template_files(self) -> None:
        """Get the content of files for template_field / template_ext."""
        if self.template_ext:
            for field in self.template_fields:
                content = getattr(self, field, None)
                if isinstance(content, str) and content.endswith(tuple(self.template_ext)):
                    env = self.get_template_env()
                    try:
                        setattr(self, field, env.loader.get_source(env, content)[0])  # type: ignore
                    except Exception:
                        log.exception("Failed to resolve template field %r", field)
                elif isinstance(content, list):
                    env = self.get_template_env()
                    for i, item in enumerate(content):
                        if isinstance(item, str) and item.endswith(tuple(self.template_ext)):
                            try:
                                content[i] = env.loader.get_source(env, item)[0]  # type: ignore
                            except Exception:
                                log.exception("Failed to get source %s", item)
        self.prepare_template()

    def _do_render_template_fields(
        self,
        parent: Any,
        template_fields: Iterable[str],
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set[int],
    ) -> None:
        for attr_name in template_fields:
            value = getattr(parent, attr_name)
            rendered_content = self.render_template(
                value,
                context,
                jinja_env,
                seen_oids,
            )
            if rendered_content:
                setattr(parent, attr_name, rendered_content)

    def _should_render_native(self, dag: DAG | None = None) -> bool:
        # Operator explicitly set? Use that value, otherwise inherit from DAG
        render_op_template_as_native_obj = getattr(self, "render_template_as_native_obj", None)
        if render_op_template_as_native_obj is not None:
            return render_op_template_as_native_obj

        return dag.render_template_as_native_obj if dag else False

    def _render(self, template, context, dag=None) -> Any:
        if self._should_render_native(dag):
            return render_template_as_native(template, context)
        return render_template_to_string(template, context)

    def render_template(
        self,
        content: Any,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
        seen_oids: set[int] | None = None,
    ) -> Any:
        """
        Render a templated string.

        If *content* is a collection holding multiple templated strings, strings
        in the collection will be templated recursively.

        :param content: Content to template. Only strings can be templated (may
            be inside a collection).
        :param context: Dict with values to apply on templated content
        :param jinja_env: Jinja environment. Can be provided to avoid
            re-creating Jinja environments during recursion.
        :param seen_oids: template fields already rendered (to avoid
            *RecursionError* on circular dependencies)
        :return: Templated content
        """
        # "content" is a bad name, but we're stuck to it being public API.
        value = content
        del content

        if seen_oids is not None:
            oids = seen_oids
        else:
            oids = set()

        if id(value) in oids:
            return value

        if not jinja_env:
            jinja_env = self.get_template_env()

        if isinstance(value, str):
            if value.endswith(tuple(self.template_ext)):  # A filepath.
                template = jinja_env.get_template(value)
            else:
                template = jinja_env.from_string(value)
            return self._render(template, context)
        if isinstance(value, ObjectStoragePath):
            return self._render_object_storage_path(value, context, jinja_env)

        if resolve := getattr(value, "resolve", None):
            return resolve(context)

        # Fast path for common built-in collections.
        if value.__class__ is tuple:
            return tuple(self.render_template(element, context, jinja_env, oids) for element in value)
        if isinstance(value, tuple):  # Special case for named tuples.
            return value.__class__(*(self.render_template(el, context, jinja_env, oids) for el in value))
        if isinstance(value, list):
            return [self.render_template(element, context, jinja_env, oids) for element in value]
        if isinstance(value, dict):
            return {k: self.render_template(v, context, jinja_env, oids) for k, v in value.items()}
        if isinstance(value, set):
            return {self.render_template(element, context, jinja_env, oids) for element in value}

        # More complex collections.
        self._render_nested_template_fields(value, context, jinja_env, oids)
        return value

    def _render_object_storage_path(
        self, value: ObjectStoragePath, context: Context, jinja_env: jinja2.Environment
    ) -> ObjectStoragePath:
        serialized_path = value.serialize()
        path_version = value.__version__
        serialized_path["path"] = self._render(jinja_env.from_string(serialized_path["path"]), context)
        return value.deserialize(data=serialized_path, version=path_version)

    def _render_nested_template_fields(
        self,
        value: Any,
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set[int],
    ) -> None:
        if id(value) in seen_oids:
            return
        seen_oids.add(id(value))
        try:
            nested_template_fields = value.template_fields
        except AttributeError:
            # content has no inner template fields
            return
        self._do_render_template_fields(value, nested_template_fields, context, jinja_env, seen_oids)


class _AirflowEnvironmentMixin:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.filters.update(FILTERS)

    def is_safe_attribute(self, obj, attr, value):
        """
        Allow access to ``_`` prefix vars (but not ``__``).

        Unlike the stock SandboxedEnvironment, we allow access to "private" attributes (ones starting with
        ``_``) whilst still blocking internal or truly private attributes (``__`` prefixed ones).
        """
        return not jinja2.sandbox.is_internal_attribute(obj, attr)


class NativeEnvironment(_AirflowEnvironmentMixin, jinja2.nativetypes.NativeEnvironment):
    """NativeEnvironment for Airflow task templates."""


class SandboxedEnvironment(_AirflowEnvironmentMixin, jinja2.sandbox.SandboxedEnvironment):
    """SandboxedEnvironment for Airflow task templates."""


def ds_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Date filter."""
    if value is None:
        return None
    return value.strftime("%Y-%m-%d")


def ds_nodash_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Date filter without dashes."""
    if value is None:
        return None
    return value.strftime("%Y%m%d")


def ts_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Timestamp filter."""
    if value is None:
        return None
    return value.isoformat()


def ts_nodash_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Timestamp filter without dashes."""
    if value is None:
        return None
    return value.strftime("%Y%m%dT%H%M%S")


def ts_nodash_with_tz_filter(value: datetime.date | datetime.time | None) -> str | None:
    """Timestamp filter with timezone."""
    if value is None:
        return None
    return value.isoformat().replace("-", "").replace(":", "")


FILTERS = {
    "ds": ds_filter,
    "ds_nodash": ds_nodash_filter,
    "ts": ts_filter,
    "ts_nodash": ts_nodash_filter,
    "ts_nodash_with_tz": ts_nodash_with_tz_filter,
}


def create_template_env(
    *,
    native: bool = False,
    searchpath: list[str] | None = None,
    template_undefined: type[jinja2.StrictUndefined] = jinja2.StrictUndefined,
    jinja_environment_kwargs: dict | None = None,
    user_defined_macros: dict | None = None,
    user_defined_filters: dict | None = None,
) -> jinja2.Environment:
    """Create a Jinja2 environment with the given settings."""
    # Default values (for backward compatibility)
    jinja_env_options = {
        "undefined": template_undefined,
        "extensions": ["jinja2.ext.do"],
        "cache_size": 0,
    }
    if searchpath:
        jinja_env_options["loader"] = ZipAwareFileSystemLoader(searchpath)
    if jinja_environment_kwargs:
        jinja_env_options.update(jinja_environment_kwargs)

    env = NativeEnvironment(**jinja_env_options) if native else SandboxedEnvironment(**jinja_env_options)

    # Add any user defined items. Safe to edit globals as long as no templates are rendered yet.
    # http://jinja.pocoo.org/docs/2.10/api/#jinja2.Environment.globals
    if user_defined_macros:
        env.globals.update(user_defined_macros)
    if user_defined_filters:
        env.filters.update(user_defined_filters)

    return env
