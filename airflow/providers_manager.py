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
import inspect
import json
import logging
import os
import sys
import traceback
import warnings
from dataclasses import dataclass
from functools import wraps
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, MutableMapping, NamedTuple, NoReturn, TypeVar

from packaging.utils import canonicalize_name

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.hooks.filesystem import FSHook
from airflow.hooks.package_index import PackageIndexHook
from airflow.typing_compat import ParamSpec
from airflow.utils import yaml
from airflow.utils.entry_points import entry_points_with_dist
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.singleton import Singleton

log = logging.getLogger(__name__)

if sys.version_info >= (3, 9):
    from importlib.resources import files as resource_files
else:
    from importlib_resources import files as resource_files

PS = ParamSpec("PS")
RT = TypeVar("RT")

MIN_PROVIDER_VERSIONS = {
    "apache-airflow-providers-celery": "2.1.0",
}


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
        else:
            return field

    if "placeholders" in field_behaviors:
        placeholders = field_behaviors["placeholders"]
        field_behaviors["placeholders"] = {ensure_prefix(k): v for k, v in placeholders.items()}

    return field_behaviors


if TYPE_CHECKING:
    from urllib.parse import SplitResult

    from airflow.assets import Asset
    from airflow.decorators.base import TaskDecorator
    from airflow.hooks.base import BaseHook
    from airflow.typing_compat import Literal


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
        try:
            self._resolved.remove(key)
        except KeyError:
            pass
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
    try:
        with resource_files("airflow").joinpath(filename).open("rb") as f:
            schema = json.load(f)
    except (TypeError, FileNotFoundError):
        import pathlib

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
    Provider information.

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
    """Hook class and Provider it comes from."""

    hook_class_name: str
    package_name: str


class TriggerInfo(NamedTuple):
    """Trigger class and provider it comes from."""

    trigger_class_name: str
    package_name: str
    integration_name: str


class NotificationInfo(NamedTuple):
    """Notification class and provider it comes from."""

    notification_class_name: str
    package_name: str


class PluginInfo(NamedTuple):
    """Plugin class, name and provider it comes from."""

    name: str
    plugin_class: str
    provider_name: str


class HookInfo(NamedTuple):
    """Hook information."""

    hook_class_name: str
    connection_id_attribute_name: str
    package_name: str
    hook_name: str
    connection_type: str
    connection_testable: bool


class ConnectionFormWidgetInfo(NamedTuple):
    """Connection Form Widget information."""

    hook_class_name: str
    package_name: str
    field: Any
    field_name: str
    is_sensitive: bool


def log_debug_import_from_sources(class_name, e, provider_package):
    """Log debug imports from sources."""
    log.debug(
        "Optional feature disabled on exception when importing '%s' from '%s' package",
        class_name,
        provider_package,
        exc_info=e,
    )


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


# This is a temporary measure until all community providers will add AirflowOptionalProviderFeatureException
# where they have optional features. We are going to add tests in our CI to catch all such cases and will
# fix them, but until now all "known unhandled optional feature errors" from community providers
# should be added here
KNOWN_UNHANDLED_OPTIONAL_FEATURE_ERRORS = [("apache-airflow-providers-google", "No module named 'paramiko'")]


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
def provider_info_cache(cache_name: str) -> Callable[[Callable[PS, NoReturn]], Callable[PS, None]]:
    """
    Decorate and cache provider info.

    Decorator factory that create decorator that caches initialization of provider's parameters
    :param cache_name: Name of the cache
    """

    def provider_info_cache_decorator(func: Callable[PS, NoReturn]) -> Callable[PS, None]:
        @wraps(func)
        def wrapped_function(*args: PS.args, **kwargs: PS.kwargs) -> None:
            providers_manager_instance = args[0]
            if TYPE_CHECKING:
                assert isinstance(providers_manager_instance, ProvidersManager)

            if cache_name in providers_manager_instance._initialized_cache:
                return
            start_time = perf_counter()
            log.debug("Initializing Providers Manager[%s]", cache_name)
            func(*args, **kwargs)
            providers_manager_instance._initialized_cache[cache_name] = True
            log.debug(
                "Initialization of Providers Manager[%s] took %.2f seconds",
                cache_name,
                perf_counter() - start_time,
            )

        return wrapped_function

    return provider_info_cache_decorator


class ProvidersManager(LoggingMixin, metaclass=Singleton):
    """
    Manages all provider packages.

    This is a Singleton class. The first time it is
    instantiated, it discovers all available providers in installed packages and
    local source folders (if airflow is run from sources).
    """

    resource_version = "0"
    _initialized: bool = False
    _initialization_stack_trace = None

    @staticmethod
    def initialized() -> bool:
        return ProvidersManager._initialized

    @staticmethod
    def initialization_stack_trace() -> str | None:
        return ProvidersManager._initialization_stack_trace

    def __init__(self):
        """Initialize the manager."""
        super().__init__()
        ProvidersManager._initialized = True
        ProvidersManager._initialization_stack_trace = "".join(traceback.format_stack(inspect.currentframe()))
        self._initialized_cache: dict[str, bool] = {}
        # Keeps dict of providers keyed by module name
        self._provider_dict: dict[str, ProviderInfo] = {}
        # Keeps dict of hooks keyed by connection type
        self._hooks_dict: dict[str, HookInfo] = {}
        self._fs_set: set[str] = set()
        self._asset_uri_handlers: dict[str, Callable[[SplitResult], SplitResult]] = {}
        self._asset_factories: dict[str, Callable[..., Asset]] = {}
        self._asset_to_openlineage_converters: dict[str, Callable] = {}
        self._taskflow_decorators: dict[str, Callable] = LazyDictWithCache()  # type: ignore[assignment]
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
        self._auth_manager_class_name_set: set[str] = set()
        self._secrets_backend_class_name_set: set[str] = set()
        self._executor_class_name_set: set[str] = set()
        self._provider_configs: dict[str, dict[str, Any]] = {}
        self._api_auth_backend_module_names: set[str] = set()
        self._trigger_info_set: set[TriggerInfo] = set()
        self._notification_info_set: set[NotificationInfo] = set()
        self._provider_schema_validator = _create_provider_info_schema_validator()
        self._customized_form_fields_schema_validator = (
            _create_customized_form_field_behaviours_schema_validator()
        )
        # Set of plugins contained in providers
        self._plugins_set: set[PluginInfo] = set()
        self._init_airflow_core_hooks()

    def _init_airflow_core_hooks(self):
        """Initialize the hooks dict with default hooks from Airflow core."""
        core_dummy_hooks = {
            "generic": "Generic",
            "email": "Email",
        }
        for key, display in core_dummy_hooks.items():
            self._hooks_lazy_dict[key] = HookInfo(
                hook_class_name=None,
                connection_id_attribute_name=None,
                package_name=None,
                hook_name=display,
                connection_type=None,
                connection_testable=False,
            )
        for cls in [FSHook, PackageIndexHook]:
            package_name = cls.__module__
            hook_class_name = f"{cls.__module__}.{cls.__name__}"
            hook_info = self._import_hook(
                connection_type=None,
                provider_info=None,
                hook_class_name=hook_class_name,
                package_name=package_name,
            )
            self._hook_provider_dict[hook_info.connection_type] = HookClassProvider(
                hook_class_name=hook_class_name, package_name=package_name
            )
            self._hooks_lazy_dict[hook_info.connection_type] = hook_info

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
        self._provider_dict = dict(sorted(self._provider_dict.items()))

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
        self._hook_provider_dict = dict(sorted(self._hook_provider_dict.items()))

    @provider_info_cache("filesystems")
    def initialize_providers_filesystems(self):
        """Lazy initialization of providers filesystems."""
        self.initialize_providers_list()
        self._discover_filesystems()

    @provider_info_cache("dataset_uris")
    def initialize_providers_dataset_uri_resources(self):
        """Lazy initialization of provider dataset URI handlers, factories, converters etc."""
        self.initialize_providers_list()
        self._discover_dataset_uri_resources()

    @provider_info_cache("hook_lineage_writers")
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

    @provider_info_cache("executors")
    def initialize_providers_executors(self):
        """Lazy initialization of providers executors information."""
        self.initialize_providers_list()
        self._discover_executors()

    @provider_info_cache("notifications")
    def initialize_providers_notifications(self):
        """Lazy initialization of providers notifications information."""
        self.initialize_providers_list()
        self._discover_notifications()

    @provider_info_cache("auth_managers")
    def initialize_providers_auth_managers(self):
        """Lazy initialization of providers notifications information."""
        self.initialize_providers_list()
        self._discover_auth_managers()

    @provider_info_cache("config")
    def initialize_providers_configuration(self):
        """Lazy initialization of providers configuration information."""
        self._initialize_providers_configuration()

    def _initialize_providers_configuration(self):
        """
        Initialize providers configuration information.

        Should be used if we do not want to trigger caching for ``initialize_providers_configuration`` method.
        In some cases we might want to make sure that the configuration is initialized, but we do not want
        to cache the initialization method - for example when we just want to write configuration with
        providers, but it is used in the context where no providers are loaded yet we will eventually
        restore the original configuration and we want the subsequent ``initialize_providers_configuration``
        method to be run in order to load the configuration for providers again.
        """
        self.initialize_providers_list()
        self._discover_config()
        # Now update conf with the new provider configuration from providers
        from airflow.configuration import conf

        conf.load_providers_configuration()

    @provider_info_cache("auth_backends")
    def initialize_providers_auth_backends(self):
        """Lazy initialization of providers API auth_backends information."""
        self.initialize_providers_list()
        self._discover_auth_backends()

    @provider_info_cache("plugins")
    def initialize_providers_plugins(self):
        self.initialize_providers_list()
        self._discover_plugins()

    def _discover_all_providers_from_packages(self) -> None:
        """
        Discover all providers by scanning packages installed.

        The list of providers should be returned via the 'apache_airflow_provider'
        entrypoint as a dictionary conforming to the 'airflow/provider_info.schema.json'
        schema. Note that the schema is different at runtime than provider.yaml.schema.json.
        The development version of provider schema is more strict and changes together with
        the code. The runtime version is more relaxed (allows for additional properties)
        and verifies only the subset of fields that are needed at runtime.
        """
        for entry_point, dist in entry_points_with_dist("apache_airflow_provider"):
            package_name = canonicalize_name(dist.metadata["name"])
            if package_name in self._provider_dict:
                continue
            log.debug("Loading %s from package %s", entry_point, package_name)
            version = dist.version
            provider_info = entry_point.load()()
            self._provider_schema_validator.validate(provider_info)
            provider_info_package_name = provider_info["package-name"]
            if package_name != provider_info_package_name:
                raise ValueError(
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
        Find all built-in airflow providers if airflow is run from the local sources.

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

        seen = set()
        for path in airflow.providers.__path__:  # type: ignore[attr-defined]
            try:
                # The same path can appear in the __path__ twice, under non-normalized paths (ie.
                # /path/to/repo/airflow/providers and /path/to/repo/./airflow/providers)
                path = os.path.realpath(path)
                if path not in seen:
                    seen.add(path)
                    self._add_provider_info_from_local_source_files_on_path(path)
            except Exception as e:
                log.warning("Error when loading 'provider.yaml' files from %s airflow sources: %s", path, e)

    def _add_provider_info_from_local_source_files_on_path(self, path) -> None:
        """
        Find all the provider.yaml files in the directory specified.

        :param path: path where to look for provider.yaml files
        """
        root_path = path
        for folder, subdirs, files in os.walk(path, topdown=True):
            for filename in fnmatch.filter(files, "provider.yaml"):
                try:
                    package_name = "apache-airflow-providers" + folder[len(root_path) :].replace(os.sep, "-")
                    self._add_provider_info_from_local_source_file(
                        os.path.join(folder, filename), package_name
                    )
                    subdirs[:] = []
                except Exception as e:
                    log.warning("Error when loading 'provider.yaml' file from %s %e", folder, e)

    def _add_provider_info_from_local_source_file(self, path, package_name) -> None:
        """
        Parse found provider.yaml file and adds found provider to the dictionary.

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
        Discover hooks from the "connection-types" property.

        This is new, better method that replaces discovery from hook-class-names as it
        allows to lazy import individual Hook classes when they are accessed.
        The "connection-types" keeps information about both - connection type and class
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
        Discover hooks from "hook-class-names' property.

        This property is deprecated but we should support it in Airflow 2.
        The hook-class-names array contained just Hook names without connection type,
        therefore we need to import all those classes immediately to know which connection types
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
                    stacklevel=1,
                )
        for already_registered_connection_type in already_registered_warning_connection_types:
            log.warning(
                "The connection_type '%s' has been already registered by provider '%s.'",
                already_registered_connection_type,
                self._hook_provider_dict[already_registered_connection_type].package_name,
            )

    def _discover_hooks(self) -> None:
        """Retrieve all connections defined in the providers via Hooks."""
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
        self._hook_provider_dict = dict(sorted(self._hook_provider_dict.items()))

    @provider_info_cache("import_all_hooks")
    def _import_info_from_all_hooks(self):
        """Force-import all hooks and initialize the connections/fields."""
        # Retrieve all hooks to make sure that all of them are imported
        _ = list(self._hooks_lazy_dict.values())
        self._field_behaviours = dict(sorted(self._field_behaviours.items()))

        # Widgets for connection forms are currently used in two places:
        # 1. In the UI Connections, expected same order that it defined in Hook.
        # 2. cli command - `airflow providers widgets` and expected that it in alphabetical order.
        # It is not possible to recover original ordering after sorting,
        # that the main reason why original sorting moved to cli part:
        # self._connection_form_widgets = dict(sorted(self._connection_form_widgets.items()))

    def _discover_filesystems(self) -> None:
        """Retrieve all filesystems defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            for fs_module_name in provider.data.get("filesystems", []):
                if _correctness_check(provider_package, f"{fs_module_name}.get_fs", provider):
                    self._fs_set.add(fs_module_name)
        self._fs_set = set(sorted(self._fs_set))

    def _discover_dataset_uri_resources(self) -> None:
        """Discovers and registers dataset URI handlers, factories, and converters for all providers."""
        from airflow.assets import normalize_noop

        def _safe_register_resource(
            provider_package_name: str,
            schemes_list: list[str],
            resource_path: str | None,
            resource_registry: dict,
            default_resource: Any = None,
        ):
            """
            Register a specific resource (handler, factory, or converter) for the given schemes.

            If the resolved resource (either from the path or the default) is valid, it updates
            the resource registry with the appropriate resource for each scheme.
            """
            resource = (
                _correctness_check(provider_package_name, resource_path, provider)
                if resource_path is not None
                else default_resource
            )
            if resource:
                resource_registry.update((scheme, resource) for scheme in schemes_list)

        for provider_name, provider in self._provider_dict.items():
            for uri_info in provider.data.get("dataset-uris", []):
                if "schemes" not in uri_info or "handler" not in uri_info:
                    continue  # Both schemas and handler must be explicitly set, handler can be set to null
                common_args = {"schemes_list": uri_info["schemes"], "provider_package_name": provider_name}
                _safe_register_resource(
                    resource_path=uri_info["handler"],
                    resource_registry=self._asset_uri_handlers,
                    default_resource=normalize_noop,
                    **common_args,
                )
                _safe_register_resource(
                    resource_path=uri_info.get("factory"),
                    resource_registry=self._asset_factories,
                    **common_args,
                )
                _safe_register_resource(
                    resource_path=uri_info.get("to_openlineage_converter"),
                    resource_registry=self._asset_to_openlineage_converters,
                    **common_args,
                )

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
        """Retrieve attributes of an object, or warn if not found."""
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
        Import hook and retrieve hook information.

        Either connection_type (for lazy loading) or hook_class_name must be set - but not both).
        Only needs package_name if hook_class_name is passed (for lazy loading, package_name
        is retrieved from _connection_type_class_provider_dict together with hook_class_name).

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
        hook_class: type[BaseHook] | None = _correctness_check(package_name, hook_class_name, provider_info)
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
        except ImportError as e:
            if "No module named 'flask_appbuilder'" in e.msg:
                log.warning(
                    "The hook_class '%s' is not fully initialized (UI widgets will be missing), because "
                    "the 'flask_appbuilder' package is not installed, however it is not required for "
                    "Airflow components to work",
                    hook_class_name,
                )
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
            else:
                self._connection_form_widgets[prefixed_field_name] = ConnectionFormWidgetInfo(
                    hook_class.__name__,
                    package_name,
                    field,
                    field_identifier,
                    hasattr(field.field_class.widget, "input_type")
                    and field.field_class.widget.input_type == "password",
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

    def _discover_auth_managers(self) -> None:
        """Retrieve all auth managers defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("auth-managers"):
                for auth_manager_class_name in provider.data["auth-managers"]:
                    if _correctness_check(provider_package, auth_manager_class_name, provider):
                        self._auth_manager_class_name_set.add(auth_manager_class_name)

    def _discover_notifications(self) -> None:
        """Retrieve all notifications defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("notifications"):
                for notification_class_name in provider.data["notifications"]:
                    if _correctness_check(provider_package, notification_class_name, provider):
                        self._notification_info_set.add(notification_class_name)

    def _discover_extra_links(self) -> None:
        """Retrieve all extra links defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("extra-links"):
                for extra_link_class_name in provider.data["extra-links"]:
                    if _correctness_check(provider_package, extra_link_class_name, provider):
                        self._extra_link_class_name_set.add(extra_link_class_name)

    def _discover_logging(self) -> None:
        """Retrieve all logging defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("logging"):
                for logging_class_name in provider.data["logging"]:
                    if _correctness_check(provider_package, logging_class_name, provider):
                        self._logging_class_name_set.add(logging_class_name)

    def _discover_secrets_backends(self) -> None:
        """Retrieve all secrets backends defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("secrets-backends"):
                for secrets_backends_class_name in provider.data["secrets-backends"]:
                    if _correctness_check(provider_package, secrets_backends_class_name, provider):
                        self._secrets_backend_class_name_set.add(secrets_backends_class_name)

    def _discover_auth_backends(self) -> None:
        """Retrieve all API auth backends defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("auth-backends"):
                for auth_backend_module_name in provider.data["auth-backends"]:
                    if _correctness_check(provider_package, auth_backend_module_name + ".init_app", provider):
                        self._api_auth_backend_module_names.add(auth_backend_module_name)

    def _discover_executors(self) -> None:
        """Retrieve all executors defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("executors"):
                for executors_class_name in provider.data["executors"]:
                    if _correctness_check(provider_package, executors_class_name, provider):
                        self._executor_class_name_set.add(executors_class_name)

    def _discover_config(self) -> None:
        """Retrieve all configs defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            if provider.data.get("config"):
                self._provider_configs[provider_package] = provider.data.get("config")  # type: ignore[assignment]

    def _discover_plugins(self) -> None:
        """Retrieve all plugins defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            for plugin_dict in provider.data.get("plugins", ()):
                if not _correctness_check(provider_package, plugin_dict["plugin-class"], provider):
                    log.warning("Plugin not loaded due to above correctness check problem.")
                    continue
                self._plugins_set.add(
                    PluginInfo(
                        name=plugin_dict["name"],
                        plugin_class=plugin_dict["plugin-class"],
                        provider_name=provider_package,
                    )
                )

    @provider_info_cache("triggers")
    def initialize_providers_triggers(self):
        """Initialize providers triggers."""
        self.initialize_providers_list()
        for provider_package, provider in self._provider_dict.items():
            for trigger in provider.data.get("triggers", []):
                for trigger_class_name in trigger.get("python-modules"):
                    self._trigger_info_set.add(
                        TriggerInfo(
                            package_name=provider_package,
                            trigger_class_name=trigger_class_name,
                            integration_name=trigger.get("integration-name", ""),
                        )
                    )

    @property
    def auth_managers(self) -> list[str]:
        """Returns information about available providers notifications class."""
        self.initialize_providers_auth_managers()
        return sorted(self._auth_manager_class_name_set)

    @property
    def notification(self) -> list[NotificationInfo]:
        """Returns information about available providers notifications class."""
        self.initialize_providers_notifications()
        return sorted(self._notification_info_set)

    @property
    def trigger(self) -> list[TriggerInfo]:
        """Returns information about available providers trigger class."""
        self.initialize_providers_triggers()
        return sorted(self._trigger_info_set, key=lambda x: x.package_name)

    @property
    def providers(self) -> dict[str, ProviderInfo]:
        """Returns information about available providers."""
        self.initialize_providers_list()
        return self._provider_dict

    @property
    def hooks(self) -> MutableMapping[str, HookInfo | None]:
        """
        Return dictionary of connection_type-to-hook mapping.

        Note that the dict can contain None values if a hook discovered cannot be imported!
        """
        self.initialize_providers_hooks()
        # When we return hooks here it will only be used to retrieve hook information
        return self._hooks_lazy_dict

    @property
    def plugins(self) -> list[PluginInfo]:
        """Returns information about plugins available in providers."""
        self.initialize_providers_plugins()
        return sorted(self._plugins_set, key=lambda x: x.plugin_class)

    @property
    def taskflow_decorators(self) -> dict[str, TaskDecorator]:
        self.initialize_providers_taskflow_decorator()
        return self._taskflow_decorators  # type: ignore[return-value]

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

    @property
    def executor_class_names(self) -> list[str]:
        self.initialize_providers_executors()
        return sorted(self._executor_class_name_set)

    @property
    def filesystem_module_names(self) -> list[str]:
        self.initialize_providers_filesystems()
        return sorted(self._fs_set)

    @property
    def asset_factories(self) -> dict[str, Callable[..., Asset]]:
        self.initialize_providers_dataset_uri_resources()
        return self._asset_factories

    @property
    def asset_uri_handlers(self) -> dict[str, Callable[[SplitResult], SplitResult]]:
        self.initialize_providers_dataset_uri_resources()
        return self._asset_uri_handlers

    @property
    def asset_to_openlineage_converters(
        self,
    ) -> dict[str, Callable]:
        self.initialize_providers_dataset_uri_resources()
        return self._asset_to_openlineage_converters

    @property
    def provider_configs(self) -> list[tuple[str, dict[str, Any]]]:
        self.initialize_providers_configuration()
        return sorted(self._provider_configs.items(), key=lambda x: x[0])

    @property
    def already_initialized_provider_configs(self) -> list[tuple[str, dict[str, Any]]]:
        return sorted(self._provider_configs.items(), key=lambda x: x[0])

    def _cleanup(self):
        self._initialized_cache.clear()
        self._provider_dict.clear()
        self._hooks_dict.clear()
        self._fs_set.clear()
        self._taskflow_decorators.clear()
        self._hook_provider_dict.clear()
        self._hooks_lazy_dict.clear()
        self._connection_form_widgets.clear()
        self._field_behaviours.clear()
        self._extra_link_class_name_set.clear()
        self._logging_class_name_set.clear()
        self._auth_manager_class_name_set.clear()
        self._secrets_backend_class_name_set.clear()
        self._executor_class_name_set.clear()
        self._provider_configs.clear()
        self._api_auth_backend_module_names.clear()
        self._trigger_info_set.clear()
        self._notification_info_set.clear()
        self._plugins_set.clear()
        self._initialized = False
        self._initialization_stack_trace = None
