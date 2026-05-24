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
"""
Resolve unresolved ``airflow.providers.common.compat.sdk.<Name>`` cross-references
to the symbol's canonical location (typically ``airflow.sdk.<Name>``).

``airflow.providers.common.compat.sdk`` dispatches symbols at runtime through
``__getattr__``, so static documentation tooling (autoapi) cannot see the
re-exports. Without this extension, any documentation that names a compat.sdk
symbol — most visibly the "Bases:" line on provider operator/sensor/hook
pages — renders as plain text rather than a link.

The handler consults the same ``_IMPORT_MAP`` / ``_RENAME_MAP`` / ``_MODULE_MAP``
that the runtime dispatcher uses, tries the canonical (Airflow 3) target first
followed by the legacy fallbacks, and re-runs Sphinx's intersphinx lookup against
each rewritten target until one resolves.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING

from sphinx.ext.intersphinx import resolve_reference_any_inventory

if TYPE_CHECKING:
    from docutils.nodes import Element, TextElement
    from sphinx.addnodes import pending_xref
    from sphinx.application import Sphinx
    from sphinx.environment import BuildEnvironment

PREFIX = "airflow.providers.common.compat.sdk."


def _canonical_targets(name: str) -> list[str]:
    """Return the ordered list of intersphinx reftarget candidates for a short name."""
    from airflow.providers.common.compat.sdk import (
        _IMPORT_MAP,
        _MODULE_MAP,
        _RENAME_MAP,
    )

    targets: list[str] = []

    # Mirrors runtime __getattr__ in providers.common.compat._compat_utils:
    # rename → module → import, with every path in each tuple tried in order so
    # legacy (Airflow 2.x) locations stay valid fallbacks when the canonical
    # target isn't present in any intersphinx inventory.
    if name in _RENAME_MAP:
        new_path, old_path, old_name = _RENAME_MAP[name]
        targets.append(f"{new_path}.{name}")
        targets.append(f"{old_path}.{old_name}")

    if name in _MODULE_MAP:
        paths = _MODULE_MAP[name]
        targets.extend(paths if isinstance(paths, tuple) else (paths,))

    if name in _IMPORT_MAP:
        paths = _IMPORT_MAP[name]
        path_tuple = paths if isinstance(paths, tuple) else (paths,)
        targets.extend(f"{path}.{name}" for path in path_tuple)

    return targets


def resolve_compat_sdk_xref(
    app: Sphinx,
    env: BuildEnvironment,
    node: pending_xref,
    contnode: TextElement,
) -> Element | None:
    """Sphinx ``missing-reference`` handler that aliases compat.sdk symbols."""
    if node.get("refdomain") != "py":
        return None

    target = node.get("reftarget", "")
    if not target.startswith(PREFIX):
        return None

    name = target[len(PREFIX) :]
    # __getattr__ only dispatches top-level attributes; nested (e.g. timezone.utcnow) is out of scope.
    if "." in name:
        return None

    try:
        candidates = _canonical_targets(name)
    except (ImportError, ModuleNotFoundError, AttributeError):
        # Narrow on purpose — other exceptions are real bugs and must surface.
        return None

    if not candidates:
        return None

    for candidate in candidates:
        probe = copy.copy(node)
        probe["reftarget"] = candidate
        resolved = resolve_reference_any_inventory(env, False, probe, contnode)
        if resolved is not None:
            return resolved

    return None


def setup(app: Sphinx) -> dict[str, object]:
    app.connect("missing-reference", resolve_compat_sdk_xref)
    return {
        "version": "1.0",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
