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
"""Shared provider discovery utilities and data structures."""

from __future__ import annotations

import contextlib
import json
import logging
import pathlib
from collections.abc import Callable, MutableMapping
from dataclasses import dataclass
from functools import wraps
from importlib.resources import files as resource_files
from time import perf_counter
from typing import Any, NamedTuple, ParamSpec

from packaging.utils import canonicalize_name

from airflow._shared.module_loading import entry_points_with_dist, import_string
from airflow.exceptions import AirflowOptionalProviderFeatureException

log = logging.getLogger(__name__)


PS = ParamSpec("PS")


KNOWN_UNHANDLED_OPTIONAL_FEATURE_ERRORS = [("apache-airflow-providers-google", "No module named 'paramiko'")]


@dataclass
class ProviderInfo:
    """
    Provider information.

    :param version: version string
    :param data: dictionary with information about the provider
    """

    version: str
    data: dict


class HookClassProvider(NamedTuple):
    """Hook class and Provider it comes from."""

    hook_class_name: str
    package_name: str


class HookInfo(NamedTuple):
    """Hook information."""

    hook_class_name: str
    connection_id_attribute_name: str
    package_name: str
    hook_name: str
    connection_type: str
    connection_testable: bool
    dialects: list[str] = []


class ConnectionFormWidgetInfo(NamedTuple):
    """Connection Form Widget information."""

    hook_class_name: str
    package_name: str
    field: Any
    field_name: str
    is_sensitive: bool


class PluginInfo(NamedTuple):
    """Plugin class, name and provider it comes from."""

    name: str
    plugin_class: str
    provider_name: str


class NotificationInfo(NamedTuple):
    """Notification class and provider it comes from."""

    notification_class_name: str
    package_name: str


class TriggerInfo(NamedTuple):
    """Trigger class and provider it comes from."""

    trigger_class_name: str
    package_name: str
    integration_name: str


class DialectInfo(NamedTuple):
    """Dialect class and Provider it comes from."""

    name: str
    dialect_class_name: str
    provider_name: str


class LazyDictWithCache(MutableMapping):
    """
    Lazy-loaded cached dictionary.

    Dictionary, which in case you set callable, executes the passed callable with `key` attribute
    at first use - and returns and caches the result.
    """

    __slots__ = ["_resolved", "_raw_dict"]

    def __init__(self, *args, **kw):
        self._resolved = set()
        self._raw_dict = dict(*args, **kw)

    def __setitem__(self, key, value):
        self._raw_dict.__setitem__(key, value)

    def __getitem__(self, key):
        value = self._raw_dict.__getitem__(key)
        if key not in self._resolved and callable(value):
            # exchange callable with result of calling it -- but only once! allow resolver to return a
            # callable itself
            value = value()
            self._resolved.add(key)
            self._raw_dict.__setitem__(key, value)
        return value

    def __delitem__(self, key):
        with contextlib.suppress(KeyError):
            self._resolved.remove(key)
        self._raw_dict.__delitem__(key)

    def __iter__(self):
        return iter(self._raw_dict)

    def __len__(self):
        return len(self._raw_dict)

    def __contains__(self, key):
        return key in self._raw_dict

    def clear(self):
        self._resolved.clear()
        self._raw_dict.clear()


def _read_schema_from_resources_or_local_file(filename: str) -> dict:
    """Read JSON schema from resources or local file."""
    try:
        with resource_files("airflow").joinpath(filename).open("rb") as f:
            schema = json.load(f)
    except (TypeError, FileNotFoundError):
        with (pathlib.Path(__file__).parent / filename).open("rb") as f:
            schema = json.load(f)
    return schema


def _create_provider_info_schema_validator():
    """Create JSON schema validator from the provider_info.schema.json."""
    import jsonschema

    schema = _read_schema_from_resources_or_local_file("provider_info.schema.json")
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema)
    return validator


def _create_customized_form_field_behaviours_schema_validator():
    """Create JSON schema validator from the customized_form_field_behaviours.schema.json."""
    import jsonschema

    schema = _read_schema_from_resources_or_local_file("customized_form_field_behaviours.schema.json")
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema)
    return validator


def _check_builtin_provider_prefix(provider_package: str, class_name: str) -> bool:
    """Check if builtin provider class has correct prefix."""
    if provider_package.startswith("apache-airflow"):
        provider_path = provider_package[len("apache-") :].replace("-", ".")
        if not class_name.startswith(provider_path):
            log.warning(
                "Coherence check failed when importing '%s' from '%s' package. It should start with '%s'",
                class_name,
                provider_package,
                provider_path,
            )
            return False
    return True


def _ensure_prefix_for_placeholders(field_behaviors: dict[str, Any], conn_type: str):
    """
    Verify the correct placeholder prefix.

    If the given field_behaviors dict contains a placeholder's node, and there
    are placeholders for extra fields (i.e. anything other than the built-in conn
    attrs), and if those extra fields are unprefixed, then add the prefix.

    The reason we need to do this is, all custom conn fields live in the same dictionary,
    so we need to namespace them with a prefix internally.  But for user convenience,
    and consistency between the `get_ui_field_behaviour` method and the extra dict itself,
    we allow users to supply the unprefixed name.
    """
    conn_attrs = {"host", "schema", "login", "password", "port", "extra"}

    def ensure_prefix(field):
        if field not in conn_attrs and not field.startswith("extra__"):
            return f"extra__{conn_type}__{field}"
        return field

    if "placeholders" in field_behaviors:
        placeholders = field_behaviors["placeholders"]
        field_behaviors["placeholders"] = {ensure_prefix(k): v for k, v in placeholders.items()}

    return field_behaviors


def log_optional_feature_disabled(class_name, e, provider_package):
    """Log optional feature disabled."""
    log.debug(
        "Optional feature disabled on exception when importing '%s' from '%s' package",
        class_name,
        provider_package,
        exc_info=e,
    )
    log.info(
        "Optional provider feature disabled when importing '%s' from '%s' package",
        class_name,
        provider_package,
    )


def log_import_warning(class_name, e, provider_package):
    """Log import warning."""
    log.warning(
        "Exception when importing '%s' from '%s' package",
        class_name,
        provider_package,
        exc_info=e,
    )


def _correctness_check(provider_package: str, class_name: str, provider_info: ProviderInfo) -> Any:
    """
    Perform coherence check on provider classes.

    For apache-airflow providers - it checks if it starts with appropriate package. For all providers
    it tries to import the provider - checking that there are no exceptions during importing.
    It logs appropriate warning in case it detects any problems.

    :param provider_package: name of the provider package
    :param class_name: name of the class to import

    :return the class if the class is OK, None otherwise.
    """
    if not _check_builtin_provider_prefix(provider_package, class_name):
        return None
    try:
        imported_class = import_string(class_name)
    except AirflowOptionalProviderFeatureException as e:
        # When the provider class raises AirflowOptionalProviderFeatureException
        # this is an expected case when only some classes in provider are
        # available. We just log debug level here and print info message in logs so that
        # the user is aware of it
        log_optional_feature_disabled(class_name, e, provider_package)
        return None
    except ImportError as e:
        if "No module named 'airflow.providers." in e.msg:
            # handle cases where another provider is missing. This can only happen if
            # there is an optional feature, so we log debug and print information about it
            log_optional_feature_disabled(class_name, e, provider_package)
            return None
        for known_error in KNOWN_UNHANDLED_OPTIONAL_FEATURE_ERRORS:
            # Until we convert all providers to use AirflowOptionalProviderFeatureException
            # we assume any problem with importing another "provider" is because this is an
            # optional feature, so we log debug and print information about it
            if known_error[0] == provider_package and known_error[1] in e.msg:
                log_optional_feature_disabled(class_name, e, provider_package)
                return None
        # But when we have no idea - we print warning to logs
        log_import_warning(class_name, e, provider_package)
        return None
    except Exception as e:
        log_import_warning(class_name, e, provider_package)
        return None
    return imported_class


def provider_info_cache(cache_name: str) -> Callable[[Callable[PS, None]], Callable[PS, None]]:
    """
    Decorate and cache provider info.

    Decorator factory that create decorator that caches initialization of provider's parameters
    :param cache_name: Name of the cache
    """

    def provider_info_cache_decorator(func: Callable[PS, None]) -> Callable[PS, None]:
        @wraps(func)
        def wrapped_function(*args: PS.args, **kwargs: PS.kwargs) -> None:
            instance = args[0]

            if cache_name in instance._initialized_cache:
                return
            start_time = perf_counter()
            log.debug("Initializing Provider Manager[%s]", cache_name)
            func(*args, **kwargs)
            instance._initialized_cache[cache_name] = True
            log.debug(
                "Initialization of Provider Manager[%s] took %.2f seconds",
                cache_name,
                perf_counter() - start_time,
            )

        return wrapped_function

    return provider_info_cache_decorator


def discover_all_providers_from_packages(
    provider_dict: dict[str, ProviderInfo],
    provider_schema_validator,
) -> None:
    """
    Discover all providers by scanning packages installed.

    The list of providers should be returned via the 'apache_airflow_provider'
    entrypoint as a dictionary conforming to the 'airflow/provider_info.schema.json'
    schema. Note that the schema is different at runtime than provider.yaml.schema.json.
    The development version of provider schema is more strict and changes together with
    the code. The runtime version is more relaxed (allows for additional properties)
    and verifies only the subset of fields that are needed at runtime.

    :param provider_dict: Dictionary to populate with discovered providers
    :param provider_schema_validator: JSON schema validator for provider info
    """
    for entry_point, dist in entry_points_with_dist("apache_airflow_provider"):
        if not dist.metadata:
            continue
        package_name = canonicalize_name(dist.metadata["name"])
        if package_name in provider_dict:
            continue
        log.debug("Loading %s from package %s", entry_point, package_name)
        version = dist.version
        provider_info = entry_point.load()()
        provider_schema_validator.validate(provider_info)
        provider_info_package_name = provider_info["package-name"]
        if package_name != provider_info_package_name:
            raise ValueError(
                f"The package '{package_name}' from packaging information "
                f"{provider_info_package_name} do not match. Please make sure they are aligned"
            )

        # issue-59576: Retrieve the project.urls.documentation from dist.metadata
        project_urls = dist.metadata.get_all("Project-URL")
        documentation_url: str | None = None

        if project_urls:
            for entry in project_urls:
                if "," in entry:
                    name, url = entry.split(",")
                    if name.strip().lower() == "documentation":
                        documentation_url = url
                        break

        provider_info["documentation-url"] = documentation_url

        if package_name not in provider_dict:
            provider_dict[package_name] = ProviderInfo(version, provider_info)
        else:
            log.warning(
                "The provider for package '%s' could not be registered from because providers for that "
                "package name have already been registered",
                package_name,
            )
