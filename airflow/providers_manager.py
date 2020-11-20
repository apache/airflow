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
import fnmatch
import json
import logging
import os
from collections import OrderedDict
from typing import Dict, Tuple

import jsonschema
import pkg_resources
import yaml

try:
    import importlib.resources as importlib_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources


log = logging.getLogger(__name__)


def _create_validator():
    """Creates JSON schema validator from the provider.yaml.schema.json"""
    schema = json.loads(importlib_resources.read_text('airflow', 'provider.yaml.schema.json'))
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema)
    return validator


class ProvidersManager:
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
        # Keeps list of providers keyed by module name and value is Tuple: version, provider_info
        self._provider_dict: Dict[str, Tuple[str, Dict]] = {}
        self._validator = _create_validator()
        # Local source folders are loaded first. They should take precedence over the package ones for
        # Development purpose. In production provider.yaml files are not present in the 'airflow" directory
        # So there is no risk we are going to override package provider accidentally. This can only happen
        # in case of local development
        self._discover_all_airflow_builtin_providers_from_local_sources()
        self._discover_all_providers_from_packages()
        self._sort_provider_dictionary()

    def _sort_provider_dictionary(self):
        """
        Sort provider_dictionary using OrderedDict.

        The dictionary gets sorted so that when you iterate through it, the providers are by
        default returned in alphabetical order.
        """
        sorted_dict = OrderedDict()
        for provider_name in sorted(self._provider_dict.keys()):
            sorted_dict[provider_name] = self._provider_dict[provider_name]
        self._provider_dict = sorted_dict

    def _discover_all_providers_from_packages(self) -> None:
        """
        Discovers all providers by scanning packages installed. The list of providers should be returned
        via the 'apache_airflow_provider' entrypoint as a dictionary conforming to the
        'airflow/provider.yaml.schema.json' schema.
        """
        for entry_point in pkg_resources.iter_entry_points('apache_airflow_provider'):
            package_name = entry_point.dist.project_name
            log.debug("Loading %s from package %s", entry_point, package_name)
            version = entry_point.dist.version
            try:
                provider_info = entry_point.load()()
            except pkg_resources.VersionConflict as e:
                log.warning(
                    "The provider package %s could not be registered because of version conflict : %s",
                    package_name,
                    e,
                )
                continue
            self._validator.validate(provider_info)
            provider_info_package_name = provider_info['package-name']
            if package_name != provider_info_package_name:
                raise Exception(
                    f"The package '{package_name}' from setuptools and "
                    f"{provider_info_package_name} do not match. Please make sure they are"
                    f"aligned"
                )
            if package_name not in self._provider_dict:
                self._provider_dict[package_name] = (version, provider_info)
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
            for path in airflow.providers.__path__:
                self._add_provider_info_from_local_source_files_on_path(path)
        except Exception as e:  # noqa pylint: disable=broad-except
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
            self._validator.validate(provider_info)

            version = provider_info['versions'][0]
            if package_name not in self._provider_dict:
                self._provider_dict[package_name] = (version, provider_info)
            else:
                log.warning(
                    "The providers for package '%s' could not be registered because providers for that "
                    "package name have already been registered",
                    package_name,
                )
        except Exception as e:  # noqa pylint: disable=broad-except
            log.warning("Error when loading '%s': %s", path, e)

    @property
    def providers(self):
        """Returns information about available providers."""
        return self._provider_dict
