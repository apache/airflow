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
"""DAG importer loading utilities."""

from __future__ import annotations

from typing import Any

from ..configuration.exceptions import AirflowConfigException

__all__ = ["load_dag_importers"]


def load_dag_importers(
    configs: list[dict[str, Any]],
    context: str = "importer configuration",
) -> list[tuple[Any, list[str]]]:
    """
    Dynamically load custom DAG importers from configuration dictionaries.

    :param configs: List of dictionaries with importer configuration.
    :param context: Context description for error reporting.
    :return: List of (importer, extensions) tuples.
    """
    from . import import_string

    if not isinstance(configs, list):
        raise AirflowConfigException(
            f"Invalid importer configuration for {context}: each entry must be a dictionary."
        )

    importers: list[tuple[Any, list[str]]] = []
    for importer_cfg in configs:
        if not isinstance(importer_cfg, dict):
            raise AirflowConfigException(
                f"Invalid importer configuration for {context}: each entry must be a dictionary."
            )
        classpath = importer_cfg.get("classpath")
        if not classpath:
            raise AirflowConfigException(
                f"Missing required 'classpath' in importer configuration for {context}."
            )
        kwargs = importer_cfg.get("kwargs", {})
        if not isinstance(kwargs, dict):
            raise AirflowConfigException(
                f"Field 'kwargs' must be a dictionary in importer configuration for {context}."
            )
        try:
            importer_class = import_string(classpath)
            importer = importer_class(**kwargs)
        except Exception as err:
            raise AirflowConfigException(
                f"Failed to load DAG importer '{classpath}' for {context}: {err}"
            ) from err

        extensions = importer_cfg.get("extensions")
        if extensions is not None:
            if not isinstance(extensions, list) or any(not isinstance(ext, str) for ext in extensions):
                raise AirflowConfigException(
                    f"Field 'extensions' must be a list of strings in importer configuration for {context}."
                )
            raw_extensions = extensions
        else:
            ext_attr = getattr(importer, "supported_extensions", None)
            raw_extensions = ext_attr() if callable(ext_attr) else (ext_attr or [])

        normalized_extensions = [
            ext.lower() if ext.startswith(".") else f".{ext.lower()}" for ext in raw_extensions
        ]
        importers.append((importer, normalized_extensions))

    return importers
