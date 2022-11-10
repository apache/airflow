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
"""Manages all providers."""
from __future__ import annotations

import fnmatch
import functools
import json
import logging
import os
import sys
import warnings
from collections import OrderedDict
from dataclasses import dataclass
from functools import wraps
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, MutableMapping, NamedTuple, TypeVar, cast

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.typing_compat import Literal
from airflow.utils import yaml
from airflow.utils.entry_points import entry_points_with_dist
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)

if sys.version_info >= (3, 9):
    from importlib.resources import files as resource_files
else:
    from importlib_resources import files as resource_files

MIN_PROVIDER_VERSIONS = {
    "apache-airflow-providers-celery": "2.1.0",
}


def _ensure_prefix_for_placeholders(field_behaviors: dict[str, Any], conn_type: str):
    """
    If the given field_behaviors dict contains a placeholders node, and there
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
        else:
            return field

    if "placeholders" in field_behaviors:
        placeholders = field_behaviors["placeholders"]
        field_behaviors["placeholders"] = {ensure_prefix(k): v for k, v in placeholders.items()}

    return field_behaviors


if TYPE_CHECKING:
    from airflow.decorators.base import TaskDecorator
    from airflow.hooks.base import BaseHook


class LazyDictWithCache(MutableMapping):
    """
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
            if value:
                self._raw_dict.__setitem__(key, value)
        return value

    def __delitem__(self, key):
        self._raw_dict.__delitem__(key)
        try:
            self._resolved.remove(key)
        except KeyError:
            pass

    def __iter__(self):
        return iter(self._raw_dict)

    def __len__(self):
        return len(self._raw_dict)

    def __contains__(self, key):
        return key in self._raw_dict


def _create_provider_info_schema_validator():
    """Creates JSON schema validator from the provider_info.schema.json"""
    import jsonschema

    with resource_files("airflow").joinpath("provider_info.schema.json").open("rb") as f:
        schema = json.load(f)
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema)
    return validator


def _create_customized_form_field_behaviours_schema_validator():
    """Creates JSON schema validator from the customized_form_field_behaviours.schema.json"""
    import jsonschema

    with resource_files("airflow").joinpath("customized_form_field_behaviours.schema.json").open("rb") as f:
        schema = json.load(f)
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema)
    return validator


def _check_builtin_provider_prefix(provider_package: str, class_name: str) -> bool:
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


@dataclass
class ProviderInfo:
    """
    Provider information

    :param version: version string
    :param data: dictionary with information about the provider
    :param source_or_package: whether the provider is source files or PyPI package. When installed from
        sources we suppress provider import errors.
    """

    version: str
    data: dict
    package_or_source: Literal["source"] | Literal["package"]

    def __post_init__(self):
        if self.package_or_source not in ("source", "package"):
            raise ValueError(
                f"Received {self.package_or_source!r} for `package_or_source`. "
                "Must be either 'package' or 'source'."
            )
        self.is_source = self.package_or_source == "source"


class HookClassProvider(NamedTuple):
    """Hook class and Provider it comes from"""

    hook_class_name: str
    package_name: str


class HookInfo(NamedTuple):
    """Hook information"""

    hook_class_name: str
    connection_id_attribute_name: str
    package_name: str
    hook_name: str
    connection_type: str
    connection_testable: bool


class ConnectionFormWidgetInfo(NamedTuple):
    """Connection Form Widget information"""

    hook_class_name: str
    package_name: str
    field: Any
    field_name: str


T = TypeVar("T", bound=Callable)

logger = logging.getLogger(__name__)


def log_debug_import_from_sources(class_name, e, provider_package):
    log.debug(
        "Optional feature disabled on exception when importing '%s' from '%s' package",
        class_name,
        provider_package,
        exc_info=e,
    )


def log_optional_feature_disabled(class_name, e, provider_package):
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
    log.warning(
        "Exception when importing '%s' from '%s' package",
        class_name,
        provider_package,
        exc_info=e,
    )


# This is a temporary measure until all community providers will add AirflowOptionalProviderFeatureException
# where they have optional features. We are going to add tests in our CI to catch all such cases and will
# fix them, but until now all "known unhandled optional feature errors" from community providers
# should be added here
KNOWN_UNHANDLED_OPTIONAL_FEATURE_ERRORS = [("apache-airflow-providers-google", "No module named 'paramiko'")]


def _sanity_check(
    provider_package: str, class_name: str, provider_info: ProviderInfo
) -> type[BaseHook] | None:
    """
    Performs coherence check on provider classes.
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
        if provider_info.is_source:
            # When we have providers from sources, then we just turn all import logs to debug logs
            # As this is pretty expected that you have a number of dependencies not installed
            # (we always have all providers from sources until we split providers to separate repo)
            log_debug_import_from_sources(class_name, e, provider_package)
            return None
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


# We want to have better control over initialization of parameters and be able to debug and test it
# So we add our own decorator
def provider_info_cache(cache_name: str) -> Callable[[T], T]:
    """
    Decorator factory that create decorator that caches initialization of provider's parameters
    :param cache_name: Name of the cache
    """

    def provider_info_cache_decorator(func: T):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            providers_manager_instance = args[0]
            if cache_name in providers_manager_instance._initialized_cache:
                return
            start_time = perf_counter()
            logger.debug("Initializing Providers Manager[%s]", cache_name)
            func(*args, **kwargs)
            providers_manager_instance._initialized_cache[cache_name] = True
            logger.debug(
                "Initialization of Providers Manager[%s] took %.2f seconds",
                cache_name,
                perf_counter() - start_time,
            )

        return cast(T, wrapped_function)

    return provider_info_cache_decorator


class ProvidersManager(LoggingMixin):
    """
    Manages all provider packages. This is a Singleton class. The first time it is
    instantiated, it discovers all available providers in installed packages and
    local source folders (if airflow is run from sources).
    """

    _instance = None
    resource_version = "0"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initializes the manager."""
        super().__init__()
        self._initialized_cache: dict[str, bool] = {}
        # Keeps dict of providers keyed by module name
        self._provider_dict: dict[str, ProviderInfo] = {}
        # Keeps dict of hooks keyed by connection type
        self._hooks_dict: dict[str, HookInfo] = {}

        self._taskflow_decorators: dict[str, Callable] = LazyDictWithCache()
        # keeps mapping between connection_types and hook class, package they come from
        self._hook_provider_dict: dict[str, HookClassProvider] = {}
        # Keeps dict of hooks keyed by connection type. They are lazy evaluated at access time
        self._hooks_lazy_dict: LazyDictWithCache[str, HookInfo | Callable] = LazyDictWithCache()
        # Keeps methods that should be used to add custom widgets tuple of keyed by name of the extra field
        self._connection_form_widgets: dict[str, ConnectionFormWidgetInfo] = {}
        # Customizations for javascript fields are kept here
        self._field_behaviours: dict[str, dict] = {}
        self._extra_link_class_name_set: set[str] = set()
        self._logging_class_name_set: set[str] = set()
        self._secrets_backend_class_name_set: set[str] = set()
        self._api_auth_backend_module_names: set[str] = set()
        self._provider_schema_validator = _create_provider_info_schema_validator()
        self._customized_form_fields_schema_validator = (
            _create_customized_form_field_behaviours_schema_validator()
        )

    @provider_info_cache("list")
    def initialize_providers_list(self):
        """Lazy initialization of providers list."""
        # Local source folders are loaded first. They should take precedence over the package ones for
        # Development purpose. In production provider.yaml files are not present in the 'airflow" directory
        # So there is no risk we are going to override package provider accidentally. This can only happen
        # in case of local development
        self._discover_all_airflow_builtin_providers_from_local_sources()
        self._discover_all_providers_from_packages()
        self._verify_all_providers_all_compatible()
        self._provider_dict = OrderedDict(sorted(self._provider_dict.items()))

    def _verify_all_providers_all_compatible(self):
        from packaging import version as packaging_version

        for provider_id, info in self._provider_dict.items():
            min_version = MIN_PROVIDER_VERSIONS.get(provider_id)
            if min_version:
                if packaging_version.parse(min_version) > packaging_version.parse(info.version):
                    log.warning(
                        "The package %s is not compatible with this version of Airflow. "
                        "The package has version %s but the minimum supported version "
                        "of the package is %s",
                        provider_id,
                        info.version,
                        min_version,
                    )

    @provider_info_cache("hooks")
    def initialize_providers_hooks(self):
        """Lazy initialization of providers hooks."""
        self.initialize_providers_list()
        self._discover_hooks()
        self._hook_provider_dict = OrderedDict(sorted(self._hook_provider_dict.items()))

    @provider_info_cache("taskflow_decorators")
    def initialize_providers_taskflow_decorator(self):
        """Lazy initialization of providers hooks."""
        self.initialize_providers_list()
        self._discover_taskflow_decorators()

    @provider_info_cache("extra_links")
    def initialize_providers_extra_links(self):
        """Lazy initialization of providers extra links."""
        self.initialize_providers_list()
        self._discover_extra_links()

    @provider_info_cache("logging")
    def initialize_providers_logging(self):
        """Lazy initialization of providers logging information."""
        self.initialize_providers_list()
        self._discover_logging()

    @provider_info_cache("secrets_backends")
    def initialize_providers_secrets_backends(self):
        """Lazy initialization of providers secrets_backends information."""
        self.initialize_providers_list()
        self._discover_secrets_backends()

    @provider_info_cache("auth_backends")
    def initialize_providers_auth_backends(self):
        """Lazy initialization of providers API auth_backends information."""
        self.initialize_providers_list()
        self._discover_auth_backends()

    def _discover_all_providers_from_packages(self) -> None:
        """
        Discovers all providers by scanning packages installed. The list of providers should be returned
        via the 'apache_airflow_provider' entrypoint as a dictionary conforming to the
        'airflow/provider_info.schema.json' schema. Note that the schema is different at runtime
        than provider.yaml.schema.json. The development version of provider schema is more strict and changes
        together with the code. The runtime version is more relaxed (allows for additional properties)
        and verifies only the subset of fields that are needed at runtime.
        """
        for entry_point, dist in entry_points_with_dist("apache_airflow_provider"):
            package_name = dist.metadata["name"]
            if self._provider_dict.get(package_name) is not None:
                continue
            log.debug("Loading %s from package %s", entry_point, package_name)
            version = dist.version
            provider_info = entry_point.load()()
            self._provider_schema_validator.validate(provider_info)
            provider_info_package_name = provider_info["package-name"]
            if package_name != provider_info_package_name:
                raise Exception(
                    f"The package '{package_name}' from setuptools and "
                    f"{provider_info_package_name} do not match. Please make sure they are aligned"
                )
            if package_name not in self._provider_dict:
                self._provider_dict[package_name] = ProviderInfo(version, provider_info, "package")
            else:
                log.warning(
                    "The provider for package '%s' could not be registered from because providers for that "
                    "package name have already been registered",
                    package_name,
                )

    def _discover_all_airflow_builtin_providers_from_local_sources(self) -> None:
        """
        Finds all built-in airflow providers if airflow is run from the local sources.
        It finds `provider.yaml` files for all such providers and registers the providers using those.

        This 'provider.yaml' scanning takes precedence over scanning packages installed
        in case you have both sources and packages installed, the providers will be loaded from
        the "airflow" sources rather than from the packages.
        """
        try:
            import airflow.providers
        except ImportError:
            log.info("You have no providers installed.")
            return
        try:
            for path in airflow.providers.__path__:  # type: ignore[attr-defined]
                self._add_provider_info_from_local_source_files_on_path(path)
        except Exception as e:
            log.warning("Error when loading 'provider.yaml' files from airflow sources: %s", e)

    def _add_provider_info_from_local_source_files_on_path(self, path) -> None:
        """
        Finds all the provider.yaml files in the directory specified.

        :param path: path where to look for provider.yaml files
        """
        root_path = path
        for folder, subdirs, files in os.walk(path, topdown=True):
            for filename in fnmatch.filter(files, "provider.yaml"):
                package_name = "apache-airflow-providers" + folder[len(root_path) :].replace(os.sep, "-")
                self._add_provider_info_from_local_source_file(os.path.join(folder, filename), package_name)
                subdirs[:] = []

    def _add_provider_info_from_local_source_file(self, path, package_name) -> None:
        """
        Parses found provider.yaml file and adds found provider to the dictionary.

        :param path: full file path of the provider.yaml file
        :param package_name: name of the package
        """
        try:
            log.debug("Loading %s from %s", package_name, path)
            with open(path) as provider_yaml_file:
                provider_info = yaml.safe_load(provider_yaml_file)
            self._provider_schema_validator.validate(provider_info)

            version = provider_info["versions"][0]
            if package_name not in self._provider_dict:
                self._provider_dict[package_name] = ProviderInfo(version, provider_info, "source")
            else:
                log.warning(
                    "The providers for package '%s' could not be registered because providers for that "
                    "package name have already been registered",
                    package_name,
                )
        except Exception as e:
            log.warning("Error when loading '%s'", path, exc_info=e)

    def _discover_hooks_from_connection_types(
        self,
        hook_class_names_registered: set[str],
        already_registered_warning_connection_types: set[str],
        package_name: str,
        provider: ProviderInfo,
    ):
        """
        Discover  hooks from the "connection-types" property. This is new, better method that replaces
        discovery from hook-class-names as it allows to lazy import individual Hook classes when they
        are accessed. The "connection-types" keeps information about both - connection type and class
        name so we can discover all connection-types without importing the classes.
        :param hook_class_names_registered: set of registered hook class names for this provider
        :param already_registered_warning_connection_types: set of connections for which warning should be
            printed in logs as they were already registered before
        :param package_name:
        :param provider:
        :return:
        """
        provider_uses_connection_types = False
        connection_types = provider.data.get("connection-types")
        if connection_types:
            for connection_type_dict in connection_types:
                connection_type = connection_type_dict["connection-type"]
                hook_class_name = connection_type_dict["hook-class-name"]
                hook_class_names_registered.add(hook_class_name)
                already_registered = self._hook_provider_dict.get(connection_type)
                if already_registered:
                    if already_registered.package_name != package_name:
                        already_registered_warning_connection_types.add(connection_type)
                    else:
                        log.warning(
                            "The connection type '%s' is already registered in the"
                            " package '%s' with different class names: '%s' and '%s'. ",
                            connection_type,
                            package_name,
                            already_registered.hook_class_name,
                            hook_class_name,
                        )
                else:
                    self._hook_provider_dict[connection_type] = HookClassProvider(
                        hook_class_name=hook_class_name, package_name=package_name
                    )
                    # Defer importing hook to access time by setting import hook method as dict value
                    self._hooks_lazy_dict[connection_type] = functools.partial(
                        self._import_hook,
                        connection_type=connection_type,
                        provider_info=provider,
                    )
            provider_uses_connection_types = True
        return provider_uses_connection_types

    def _discover_hooks_from_hook_class_names(
        self,
        hook_class_names_registered: set[str],
        already_registered_warning_connection_types: set[str],
        package_name: str,
        provider: ProviderInfo,
        provider_uses_connection_types: bool,
    ):
        """
        Discovers hooks from "hook-class-names' property. This property is deprecated but we should
        support it in Airflow 2. The hook-class-names array contained just Hook names without connection
        type, therefore we need to import all those classes immediately to know which connection types
        are supported. This makes it impossible to selectively only import those hooks that are used.
        :param already_registered_warning_connection_types: list of connection hooks that we should warn
            about when finished discovery
        :param package_name: name of the provider package
        :param provider: class that keeps information about version and details of the provider
        :param provider_uses_connection_types: determines whether the provider uses "connection-types" new
           form of passing connection types
        :return:
        """
        hook_class_names = provider.data.get("hook-class-names")
        if hook_class_names:
            for hook_class_name in hook_class_names:
                if hook_class_name in hook_class_names_registered:
                    # Silently ignore the hook class - it's already marked for lazy-import by
                    # connection-types discovery
                    continue
                hook_info = self._import_hook(
                    connection_type=None,
                    provider_info=provider,
                    hook_class_name=hook_class_name,
                    package_name=package_name,
                )
                if not hook_info:
                    # Problem why importing class - we ignore it. Log is written at import time
                    continue
                already_registered = self._hook_provider_dict.get(hook_info.connection_type)
                if already_registered:
                    if already_registered.package_name != package_name:
                        already_registered_warning_connection_types.add(hook_info.connection_type)
                    else:
                        if already_registered.hook_class_name != hook_class_name:
                            log.warning(
                                "The hook connection type '%s' is registered twice in the"
                                " package '%s' with different class names: '%s' and '%s'. "
                                " Please fix it!",
                                hook_info.connection_type,
                                package_name,
                                already_registered.hook_class_name,
                                hook_class_name,
                            )
                else:
                    self._hook_provider_dict[hook_info.connection_type] = HookClassProvider(
                        hook_class_name=hook_class_name, package_name=package_name
                    )
                    self._hooks_lazy_dict[hook_info.connection_type] = hook_info

            if not provider_uses_connection_types:
                warnings.warn(
                    f"The provider {package_name} uses `hook-class-names` "
                    "property in provider-info and has no `connection-types` one. "
                    "The 'hook-class-names' property has been deprecated in favour "
                    "of 'connection-types' in Airflow 2.2. Use **both** in case you want to "
                    "have backwards compatibility with Airflow < 2.2",
                    DeprecationWarning,
                )
        for already_registered_connection_type in already_registered_warning_connection_types:
            log.warning(
                "The connection_type '%s' has been already registered by provider '%s.'",
                already_registered_connection_type,
                self._hook_provider_dict[already_registered_connection_type].package_name,
            )

    def _discover_hooks(self) -> None:
        """Retrieves all connections defined in the providers via Hooks"""
        for package_name, provider in self._provider_dict.items():
            duplicated_connection_types: set[str] = set()
            hook_class_names_registered: set[str] = set()
            provider_uses_connection_types = self._discover_hooks_from_connection_types(
                hook_class_names_registered, duplicated_connection_types, package_name, provider
            )
            self._discover_hooks_from_hook_class_names(
                hook_class_names_registered,
                duplicated_connection_types,
                package_name,
                provider,
                provider_uses_connection_types,
            )
        self._hook_provider_dict = OrderedDict(sorted(self._hook_provider_dict.items()))

    @provider_info_cache("import_all_hooks")
    def _import_info_from_all_hooks(self):
        """Force-import all hooks and initialize the connections/fields"""
        # Retrieve all hooks to make sure that all of them are imported
        _ = list(self._hooks_lazy_dict.values())
        self._field_behaviours = OrderedDict(sorted(self._field_behaviours.items()))

        # Widgets for connection forms are currently used in two places:
        # 1. In the UI Connections, expected same order that it defined in Hook.
        # 2. cli command - `airflow providers widgets` and expected that it in alphabetical order.
        # It is not possible to recover original ordering after sorting,
        # that the main reason why original sorting moved to cli part:
        # self._connection_form_widgets = OrderedDict(sorted(self._connection_form_widgets.items()))

    def _discover_taskflow_decorators(self) -> None:
        for name, info in self._provider_dict.items():
            for taskflow_decorator in info.data.get("task-decorators", []):
                self._add_taskflow_decorator(
                    taskflow_decorator["name"], taskflow_decorator["class-name"], name
                )

    def _add_taskflow_decorator(self, name, decorator_class_name: str, provider_package: str) -> None:
        if not _check_builtin_provider_prefix(provider_package, decorator_class_name):
            return

        if name in self._taskflow_decorators:
            try:
                existing = self._taskflow_decorators[name]
                other_name = f"{existing.__module__}.{existing.__name__}"
            except Exception:
                # If problem importing, then get the value from the functools.partial
                other_name = self._taskflow_decorators._raw_dict[name].args[0]  # type: ignore[attr-defined]

            log.warning(
                "The taskflow decorator '%s' has been already registered (by %s).",
                name,
                other_name,
            )
            return

        self._taskflow_decorators[name] = functools.partial(import_string, decorator_class_name)

    @staticmethod
    def _get_attr(obj: Any, attr_name: str):
        """Retrieves attributes of an object, or warns if not found"""
        if not hasattr(obj, attr_name):
            log.warning("The object '%s' is missing %s attribute and cannot be registered", obj, attr_name)
            return None
        return getattr(obj, attr_name)

    def _import_hook(
        self,
        connection_type: str | None,
        provider_info: ProviderInfo,
        hook_class_name: str | None = None,
        package_name: str | None = None,
    ) -> HookInfo | None:
        """
        Imports hook and retrieves hook information. Either connection_type (for lazy loading)
        or hook_class_name must be set - but not both). Only needs package_name if hook_class_name is
        passed (for lazy loading, package_name is retrieved from _connection_type_class_provider_dict
        together with hook_class_name).

        :param connection_type: type of the connection
        :param hook_class_name: name of the hook class
        :param package_name: provider package - only needed in case connection_type is missing
        : return
        """
        from wtforms import BooleanField, IntegerField, PasswordField, StringField

        if connection_type is None and hook_class_name is None:
            raise ValueError("Either connection_type or hook_class_name must be set")
        if connection_type is not None and hook_class_name is not None:
            raise ValueError(
                f"Both connection_type ({connection_type} and "
                f"hook_class_name {hook_class_name} are set. Only one should be set!"
            )
        if connection_type is not None:
            class_provider = self._hook_provider_dict[connection_type]
            package_name = class_provider.package_name
            hook_class_name = class_provider.hook_class_name
        else:
            if not hook_class_name:
                raise ValueError("Either connection_type or hook_class_name must be set")
            if not package_name:
                raise ValueError(
                    f"Provider package name is not set when hook_class_name ({hook_class_name}) is used"
                )
        allowed_field_classes = [IntegerField, PasswordField, StringField, BooleanField]
        hook_class = _sanity_check(package_name, hook_class_name, provider_info)
        if hook_class is None:
            return None
        try:
            module, class_name = hook_class_name.rsplit(".", maxsplit=1)
            # Do not use attr here. We want to check only direct class fields not those
            # inherited from parent hook. This way we add form fields only once for the whole
            # hierarchy and we add it only from the parent hook that provides those!
            if "get_connection_form_widgets" in hook_class.__dict__:
                widgets = hook_class.get_connection_form_widgets()

                if widgets:
                    for widget in widgets.values():
                        if widget.field_class not in allowed_field_classes:
                            log.warning(
                                "The hook_class '%s' uses field of unsupported class '%s'. "
                                "Only '%s' field classes are supported",
                                hook_class_name,
                                widget.field_class,
                                allowed_field_classes,
                            )
                            return None
                    self._add_widgets(package_name, hook_class, widgets)
            if "get_ui_field_behaviour" in hook_class.__dict__:
                field_behaviours = hook_class.get_ui_field_behaviour()
                if field_behaviours:
                    self._add_customized_fields(package_name, hook_class, field_behaviours)
        except Exception as e:
            log.warning(
                "Exception when importing '%s' from '%s' package: %s",
                hook_class_name,
                package_name,
                e,
            )
            return None
        hook_connection_type = self._get_attr(hook_class, "conn_type")
        if connection_type:
            if hook_connection_type != connection_type:
                log.warning(
                    "Inconsistency! The hook class '%s' declares connection type '%s'"
                    " but it is added by provider '%s' as connection_type '%s' in provider info. "
                    "This should be fixed!",
                    hook_class,
                    hook_connection_type,
                    package_name,
                    connection_type,
                )
        connection_type = hook_connection_type
        connection_id_attribute_name: str = self._get_attr(hook_class, "conn_name_attr")
        hook_name: str = self._get_attr(hook_class, "hook_name")

        if not connection_type or not connection_id_attribute_name or not hook_name:
            log.warning(
                "The hook misses one of the key attributes: "
                "conn_type: %s, conn_id_attribute_name: %s, hook_name: %s",
                connection_type,
                connection_id_attribute_name,
                hook_name,
            )
            return None

        return HookInfo(
            hook_class_name=hook_class_name,
            connection_id_attribute_name=connection_id_attribute_name,
            package_name=package_name,
            hook_name=hook_name,
            connection_type=connection_type,
            connection_testable=hasattr(hook_class, "test_connection"),
        )

    def _add_widgets(self, package_name: str, hook_class: type, widgets: dict[str, Any]):
        conn_type = hook_class.conn_type  # type: ignore
        for field_identifier, field in widgets.items():
            if field_identifier.startswith("extra__"):
                prefixed_field_name = field_identifier
            else:
                prefixed_field_name = f"extra__{conn_type}__{field_identifier}"
            if prefixed_field_name in self._connection_form_widgets:
                log.warning(
                    "The field %s from class %s has already been added by another provider. Ignoring it.",
                    field_identifier,
                    hook_class.__name__,
                )
                # In case of inherited hooks this might be happening several times
                continue
            self._connection_form_widgets[prefixed_field_name] = ConnectionFormWidgetInfo(
                hook_class.__name__, package_name, field, field_identifier
            )

    def _add_customized_fields(self, package_name: str, hook_class: type, customized_fields: dict):
        try:
            connection_type = getattr(hook_class, "conn_type")

            self._customized_form_fields_schema_validator.validate(customized_fields)

            if connection_type:
                customized_fields = _ensure_prefix_for_placeholders(customized_fields, connection_type)

            if connection_type in self._field_behaviours:
                log.warning(
                    "The connection_type %s from package %s and class %s has already been added "
                    "by another provider. Ignoring it.",
                    connection_type,
                    package_name,
                    hook_class.__name__,
                )
                return
            self._field_behaviours[connection_type] = customized_fields
        except Exception as e:
            log.warning(
                "Error when loading customized fields from package '%s' hook class '%s': %s",
                package_name,
                hook_class.__name__,
                e,
            )

    def _discover_extra_links(self) -> None:
        """Retrieves all extra links defined in the providers"""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("extra-links"):
                for extra_link_class_name in provider.data["extra-links"]:
                    if _sanity_check(provider_package, extra_link_class_name, provider):
                        self._extra_link_class_name_set.add(extra_link_class_name)

    def _discover_logging(self) -> None:
        """Retrieves all logging defined in the providers"""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("logging"):
                for logging_class_name in provider.data["logging"]:
                    if _sanity_check(provider_package, logging_class_name, provider):
                        self._logging_class_name_set.add(logging_class_name)

    def _discover_secrets_backends(self) -> None:
        """Retrieves all secrets backends defined in the providers"""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("secrets-backends"):
                for secrets_backends_class_name in provider.data["secrets-backends"]:
                    if _sanity_check(provider_package, secrets_backends_class_name, provider):
                        self._secrets_backend_class_name_set.add(secrets_backends_class_name)

    def _discover_auth_backends(self) -> None:
        """Retrieves all API auth backends defined in the providers"""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("auth-backends"):
                for auth_backend_module_name in provider.data["auth-backends"]:
                    if _sanity_check(provider_package, auth_backend_module_name + ".init_app", provider):
                        self._api_auth_backend_module_names.add(auth_backend_module_name)

    @property
    def providers(self) -> dict[str, ProviderInfo]:
        """Returns information about available providers."""
        self.initialize_providers_list()
        return self._provider_dict

    @property
    def hooks(self) -> MutableMapping[str, HookInfo | None]:
        """
        Returns dictionary of connection_type-to-hook mapping. Note that the dict can contain
        None values if a hook discovered cannot be imported!
        """
        self.initialize_providers_hooks()
        # When we return hooks here it will only be used to retrieve hook information
        return self._hooks_lazy_dict

    @property
    def taskflow_decorators(self) -> dict[str, TaskDecorator]:
        self.initialize_providers_taskflow_decorator()
        return self._taskflow_decorators

    @property
    def extra_links_class_names(self) -> list[str]:
        """Returns set of extra link class names."""
        self.initialize_providers_extra_links()
        return sorted(self._extra_link_class_name_set)

    @property
    def connection_form_widgets(self) -> dict[str, ConnectionFormWidgetInfo]:
        """
        Returns widgets for connection forms.
        Dictionary keys in the same order that it defined in Hook.
        """
        self.initialize_providers_hooks()
        self._import_info_from_all_hooks()
        return self._connection_form_widgets

    @property
    def field_behaviours(self) -> dict[str, dict]:
        """Returns dictionary with field behaviours for connection types."""
        self.initialize_providers_hooks()
        self._import_info_from_all_hooks()
        return self._field_behaviours

    @property
    def logging_class_names(self) -> list[str]:
        """Returns set of log task handlers class names."""
        self.initialize_providers_logging()
        return sorted(self._logging_class_name_set)

    @property
    def secrets_backend_class_names(self) -> list[str]:
        """Returns set of secret backend class names."""
        self.initialize_providers_secrets_backends()
        return sorted(self._secrets_backend_class_name_set)

    @property
    def auth_backend_module_names(self) -> list[str]:
        """Returns set of API auth backend class names."""
        self.initialize_providers_auth_backends()
        return sorted(self._api_auth_backend_module_names)
