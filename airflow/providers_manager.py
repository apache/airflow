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
import importlib
import json
import logging
import pkgutil
import traceback
from functools import lru_cache
from typing import Dict, Tuple

import jsonschema
import yaml

try:
    import importlib.resources as importlib_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources  # noqa


def _create_validator():
    schema = json.loads(importlib_resources.read_text('airflow', 'provider.yaml.schema.json'))
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema)
    return validator


class ProvidersManager:
    """Manages all provider packages."""

    def __init__(self):
        self.__log = logging.getLogger(__name__)
        self._provider_directory = {}
        try:
            from airflow import providers
        except ImportError as e:
            self.__log.warning("No providers are present or error when importing them! :%s", e)
            return
        self.__connections: Dict[str, Tuple[str, str]] = {}
        self.__validator = _create_validator()
        self.__find_all_providers(providers.__path__)
        self.__retrieve_connections()

    @staticmethod
    def get_full_class_name(provider_package: str, class_path: str):
        """
        Return full class name - if the class starts with ., it adds package_name in front.
        :param provider_package: Package name
        :param class_path: class path (might be relative if starts with .)
        :return: full class name
        """
        if class_path.startswith('.'):
            return f"{provider_package}{class_path}"
        return class_path

    def __find_all_providers(self, paths: str) -> None:
        """
        Finds all providers installed and stores then in the directory of the providers.
        :param paths: Path where to look for providers
        """
        import warnings

        old_warn = warnings.warn

        def warn(*args, **kwargs):
            if "deprecated" not in args[0]:
                old_warn(*args, **kwargs)

        warnings.warn = warn
        try:

            def onerror(_):
                exception_string = traceback.format_exc()
                self.__log.warning(exception_string)

            for module_info in pkgutil.walk_packages(paths, prefix="airflow.providers.", onerror=onerror):
                try:
                    imported_module = importlib.import_module(module_info.name)
                except Exception as e:  # noqa pylint: disable=broad-except
                    self.__log.warning("Error when importing %s:%s", module_info.name, e)
                    continue
                self.__read_provider_metadata(imported_module, module_info)
        finally:
            warnings.warn = old_warn

    def __read_provider_metadata(self, imported_module, module_info):
        try:
            provider = importlib_resources.read_text(imported_module, 'provider.yaml')
            provider_info = yaml.safe_load(provider)
            self.__validator.validate(provider_info)
            package_name = provider_info['package-name']
            provider_package = provider_info['provider-package']
            if provider_package.startswith("apache.airflow"):
                expected_package_name = provider_package.replace('.', '.')
                if expected_package_name != package_name:
                    raise Exception(
                        f"Expected package name: {expected_package_name}" f" but it was {package_name}"
                    )
            self._provider_directory[package_name] = provider_info
        except FileNotFoundError:
            # This is OK - this is not a provider package
            pass
        except TypeError as e:
            if "is not a package" not in str(e):
                self.__log.warning("Error when loading 'provider.yaml' file from %s:%s}", module_info.name, e)
            # Otherwise this is OK - this is likely a module
        except Exception as e:  # noqa pylint: disable=broad-except
            self.__log.warning("Error when loading 'provider.yaml' file from %s:%s", module_info.name, e)

    def __retrieve_connections(self) -> None:
        """Retrieves all connections defined in the providers"""
        for provider in self._provider_directory.values():
            provider_package = provider['provider-package']
            if provider.get("connections"):
                for connection in provider["connections"]:
                    self.__add_connection_to_table(connection, provider_package)

    def __add_connection_to_table(self, connection, provider_package):
        hook_class_name = connection['hook-class']
        if not hook_class_name.startswith('.'):
            self.__log.warning(
                "The %s in provider.yaml when trying %s "
                "provider should start with '.' being relative to the package",
                hook_class_name,
                provider_package,
            )
        else:
            full_class_name = f"{provider_package}{hook_class_name}"
            try:
                module, class_name = full_class_name.rsplit('.', maxsplit=1)
                getattr(importlib.import_module(module), class_name)
            except Exception as e:  # noqa pylint: disable=broad-except
                self.__log.warning(
                    "The %s in provider.yaml when trying %s " "could not be imported: %s",
                    full_class_name,
                    provider_package,
                    e,
                )
                return
            self.__connections[connection['connection-type']] = (
                full_class_name,
                connection['connection-id-parameter-name'],
            )

    @property
    def providers(self):
        """Returns information about available providers."""
        return self._provider_directory

    @property
    def connections(self):
        """Returns dictionary of connection-to-hook mapping"""
        return self.__connections


cache = lru_cache(maxsize=None)


@cache
def get_providers_manager() -> ProvidersManager:  # noqa
    """Returns singleton instance of ProvidersManager"""
    return ProvidersManager()
