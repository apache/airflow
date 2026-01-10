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
"""Manages runtime provider resources for task execution."""

from __future__ import annotations

import functools
import inspect
import logging
import traceback
from collections.abc import Callable, MutableMapping
from typing import TYPE_CHECKING, Any
from urllib.parse import SplitResult

from airflow._shared.module_loading import import_string
from airflow._shared.providers_discovery import (
    HookClassProvider,
    HookInfo,
    LazyDictWithCache,
    ProviderInfo,
    _correctness_check,
    _create_provider_info_schema_validator,
    discover_all_providers_from_packages,
    provider_info_cache,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.singleton import Singleton

if TYPE_CHECKING:
    from airflow.sdk import BaseHook
    from airflow.sdk.bases.decorator import TaskDecorator
    from airflow.sdk.definitions.asset import Asset

log = logging.getLogger(__name__)


class ProvidersManagerRuntime(LoggingMixin, metaclass=Singleton):
    """
    Manages runtime provider resources for task execution.

    This is a Singleton class. The first time it is instantiated, it discovers all available
    runtime provider resources (hooks, taskflow decorators, filesystems, asset handlers).
    """

    resource_version = "0"
    _initialized: bool = False
    _initialization_stack_trace = None

    @staticmethod
    def initialized() -> bool:
        return ProvidersManagerRuntime._initialized

    @staticmethod
    def initialization_stack_trace() -> str | None:
        return ProvidersManagerRuntime._initialization_stack_trace

    def __init__(self):
        """Initialize the runtime manager."""
        super().__init__()
        ProvidersManagerRuntime._initialized = True
        ProvidersManagerRuntime._initialization_stack_trace = "".join(
            traceback.format_stack(inspect.currentframe())
        )
        self._initialized_cache: dict[str, bool] = {}
        # Keeps dict of providers keyed by module name
        self._provider_dict: dict[str, ProviderInfo] = {}
        self._fs_set: set[str] = set()
        self._asset_uri_handlers: dict[str, Callable[[SplitResult], SplitResult]] = {}
        self._asset_factories: dict[str, Callable[..., Asset]] = {}
        self._asset_to_openlineage_converters: dict[str, Callable] = {}
        self._taskflow_decorators: dict[str, Callable] = LazyDictWithCache()
        # keeps mapping between connection_types and hook class, package they come from
        self._hook_provider_dict: dict[str, HookClassProvider] = {}
        # Keeps dict of hooks keyed by connection type. They are lazy evaluated at access time
        self._hooks_lazy_dict: LazyDictWithCache[str, HookInfo | Callable] = LazyDictWithCache()
        self._provider_schema_validator = _create_provider_info_schema_validator()
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
        for conn_type, class_name in (
            ("fs", "airflow.providers.standard.hooks.filesystem.FSHook"),
            ("package_index", "airflow.providers.standard.hooks.package_index.PackageIndexHook"),
        ):
            self._hooks_lazy_dict[conn_type] = functools.partial(
                self._import_hook,
                connection_type=None,
                package_name="apache-airflow-providers-standard",
                hook_class_name=class_name,
                provider_info=None,
            )

    @provider_info_cache("list")
    def initialize_providers_list(self):
        """Lazy initialization of providers list."""
        discover_all_providers_from_packages(self._provider_dict, self._provider_schema_validator)
        self._provider_dict = dict(sorted(self._provider_dict.items()))

    @provider_info_cache("hooks")
    def initialize_providers_hooks(self):
        """Lazy initialization of providers hooks."""
        self._init_airflow_core_hooks()
        self.initialize_providers_list()
        self._discover_hooks()
        self._hook_provider_dict = dict(sorted(self._hook_provider_dict.items()))

    @provider_info_cache("filesystems")
    def initialize_providers_filesystems(self):
        """Lazy initialization of providers filesystems."""
        self.initialize_providers_list()
        self._discover_filesystems()

    @provider_info_cache("asset_uris")
    def initialize_providers_asset_uri_resources(self):
        """Lazy initialization of provider asset URI handlers, factories, converters etc."""
        self.initialize_providers_list()
        self._discover_asset_uri_resources()

    @provider_info_cache("taskflow_decorators")
    def initialize_providers_taskflow_decorator(self):
        """Lazy initialization of providers taskflow decorators."""
        self.initialize_providers_list()
        self._discover_taskflow_decorators()

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

    def _discover_hooks(self) -> None:
        """Retrieve all connections defined in the providers via Hooks."""
        for package_name, provider in self._provider_dict.items():
            duplicated_connection_types: set[str] = set()
            hook_class_names_registered: set[str] = set()
            self._discover_hooks_from_connection_types(
                hook_class_names_registered, duplicated_connection_types, package_name, provider
            )
        self._hook_provider_dict = dict(sorted(self._hook_provider_dict.items()))

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
        hook_class: type[BaseHook] | None = _correctness_check(package_name, hook_class_name, provider_info)
        if hook_class is None:
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

    def _discover_filesystems(self) -> None:
        """Retrieve all filesystems defined in the providers."""
        for provider_package, provider in self._provider_dict.items():
            for fs_module_name in provider.data.get("filesystems", []):
                if _correctness_check(provider_package, f"{fs_module_name}.get_fs", provider):
                    self._fs_set.add(fs_module_name)
        self._fs_set = set(sorted(self._fs_set))

    def _discover_asset_uri_resources(self) -> None:
        """Discovers and registers asset URI handlers, factories, and converters for all providers."""
        from airflow.sdk.definitions.asset import normalize_noop

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
            for uri_info in provider.data.get("asset-uris", []):
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
        from airflow._shared.providers_discovery import _check_builtin_provider_prefix

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
        return self._hooks_lazy_dict

    @property
    def taskflow_decorators(self) -> dict[str, TaskDecorator]:
        self.initialize_providers_taskflow_decorator()
        return self._taskflow_decorators  # type: ignore[return-value]

    @property
    def filesystem_module_names(self) -> list[str]:
        self.initialize_providers_filesystems()
        return sorted(self._fs_set)

    @property
    def asset_factories(self) -> dict[str, Callable[..., Asset]]:
        self.initialize_providers_asset_uri_resources()
        return self._asset_factories

    @property
    def asset_uri_handlers(self) -> dict[str, Callable[[SplitResult], SplitResult]]:
        self.initialize_providers_asset_uri_resources()
        return self._asset_uri_handlers

    @property
    def asset_to_openlineage_converters(
        self,
    ) -> dict[str, Callable]:
        self.initialize_providers_asset_uri_resources()
        return self._asset_to_openlineage_converters

    def _cleanup(self):
        self._initialized_cache.clear()
        self._provider_dict.clear()
        self._fs_set.clear()
        self._taskflow_decorators.clear()
        self._hook_provider_dict.clear()
        self._hooks_lazy_dict.clear()
        self._asset_uri_handlers.clear()
        self._asset_factories.clear()
        self._asset_to_openlineage_converters.clear()

        self._initialized = False
        self._initialization_stack_trace = None
