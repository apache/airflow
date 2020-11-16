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
from typing import Dict

import jsonschema
import yaml

try:
    import importlib.resources as importlib_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources


log = logging.getLogger(__name__)


def _load_schema() -> Dict:
    return json.loads(importlib_resources.read_text('airflow', 'provider.yaml.schema.json'))


class ProvidersManager:
    """Manages all provider packages."""

    def __init__(self):
        self._provider_directory = {}
        try:
            from airflow import providers
        except ImportError as e:
            log.warning("No providers are present or error when importing them! :%s", e)
            return
        self._schema = _load_schema()
        self.__find_all_providers(providers.__path__)

    def __find_all_providers(self, paths: str):
        def onerror(_):
            exception_string = traceback.format_exc()
            log.warning(exception_string)

        for module_info in pkgutil.walk_packages(paths, prefix="airflow.providers.", onerror=onerror):
            try:
                imported_module = importlib.import_module(module_info.name)
            except Exception as e:  # noqa pylint: disable=broad-except
                log.warning("Error when importing %s:%s", module_info.name, e)
                continue
            try:
                provider = importlib_resources.read_text(imported_module, 'provider.yaml')
                provider_info = yaml.safe_load(provider)
                jsonschema.validate(provider_info, schema=self._schema)
                self._provider_directory[provider_info['package-name']] = provider_info
            except FileNotFoundError:
                # This is OK - this is not a provider package
                pass
            except TypeError as e:
                if "is not a package" not in str(e):
                    log.warning("Error when loading 'provider.yaml' file from %s:%s}", module_info.name, e)
                # Otherwise this is OK - this is likely a module
            except Exception as e:  # noqa pylint: disable=broad-except
                log.warning("Error when loading 'provider.yaml' file from %s:%s", module_info.name, e)

    @property
    def providers(self):
        """Returns information about available providers."""
        return self._provider_directory
