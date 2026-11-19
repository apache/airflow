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
"""URI schemes a provider registers, collected from its provider.yaml.

Airflow resolves three kinds of provider handler by URI scheme at runtime:

- ``filesystems``: modules exposing ``get_fs``, which ``airflow.sdk.io.fs``
  maps to ``ObjectStoragePath`` schemes. The schemes are not in provider.yaml;
  each module declares them in a module-level ``schemes`` list, read here with AST.
- ``asset-uris``: URI normalizers, Asset factories and OpenLineage converters.
- ``remote-logging``: ``RemoteLogIO`` classes claiming a scheme of
  ``[logging] remote_base_log_folder``.

Shared by extract_metadata.py (reads sources from the working tree) and
extract_versions.py (reads sources from a release tag).
"""

from __future__ import annotations

import ast
from collections.abc import Callable
from typing import Any

ReadModuleSource = Callable[[str], str | None]


def read_filesystem_schemes(source: str) -> list[str]:
    """Return the module-level ``schemes`` list declared by a filesystem module."""
    try:
        for node in ast.parse(source).body:
            if isinstance(node, ast.Assign):
                targets, value = node.targets, node.value
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                targets, value = [node.target], node.value
            else:
                continue
            if any(isinstance(target, ast.Name) and target.id == "schemes" for target in targets):
                return list(ast.literal_eval(value))
    except (SyntaxError, ValueError) as e:
        print(f"  Warning: could not read filesystem schemes: {e}")
    return []


def collect_uri_schemes(
    provider_yaml: dict[str, Any], read_module_source: ReadModuleSource
) -> list[dict[str, Any]]:
    """
    Return one entry per URI scheme the provider registers, sorted by scheme.

    Each entry has a ``scheme`` key plus whichever of ``filesystem`` (module
    path), ``asset`` (handler, factory and OpenLineage converter paths) and
    ``remote_logging`` (``RemoteLogIO`` class path) the provider registers for it.

    :param provider_yaml: Parsed provider.yaml.
    :param read_module_source: Returns the source of a dotted module path, or
        ``None`` when the file is missing.
    """
    by_scheme: dict[str, dict[str, Any]] = {}

    def entry(scheme: str) -> dict[str, Any]:
        return by_scheme.setdefault(scheme, {"scheme": scheme})

    for module_path in provider_yaml.get("filesystems", []):
        if (source := read_module_source(module_path)) is None:
            continue
        for scheme in read_filesystem_schemes(source):
            entry(scheme)["filesystem"] = module_path

    for spec in provider_yaml.get("asset-uris", []):
        # ProvidersManager skips entries without a handler key; an explicit null
        # handler still registers the scheme, with a no-op normalizer.
        if "handler" not in spec:
            continue
        for scheme in spec.get("schemes", []):
            entry(scheme)["asset"] = {
                "handler": spec["handler"],
                "factory": spec.get("factory"),
                "to_openlineage_converter": spec.get("to_openlineage_converter"),
            }

    for spec in provider_yaml.get("remote-logging", []):
        entry(spec["scheme"])["remote_logging"] = spec["classpath"]

    return [by_scheme[scheme] for scheme in sorted(by_scheme)]
