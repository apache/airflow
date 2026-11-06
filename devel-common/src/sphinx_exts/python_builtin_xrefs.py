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
Keep builtin names in annotations from resolving to same-named project attributes.

Sphinx 9 falls back from a ``py:class`` lookup to a fuzzy ``py:data``/``py:attr`` search, so a
builtin such as ``type`` or ``object`` in an annotation matches every documented attribute with
that name and fails the build with "more than one target found". Leaving the builtin unresolved
lets intersphinx link it to the Python docs, as Sphinx 8 did.

Remove once https://github.com/sphinx-doc/sphinx/issues/14223 is fixed in every Sphinx version
the docs build uses; tracked at https://github.com/apache/airflow/issues/74167
"""

from __future__ import annotations

import builtins
from typing import TYPE_CHECKING, Any

from sphinx.domains.python import PythonDomain

if TYPE_CHECKING:
    from docutils.nodes import Element
    from sphinx.addnodes import pending_xref
    from sphinx.application import Sphinx
    from sphinx.builders import Builder
    from sphinx.environment import BuildEnvironment

_BUILTIN_NAMES = frozenset(dir(builtins))


class _PythonDomainWithBuiltinXrefs(PythonDomain):
    def resolve_xref(
        self,
        env: BuildEnvironment,
        fromdocname: str,
        builder: Builder,
        type: str,
        target: str,
        node: pending_xref,
        contnode: Element,
    ) -> Any:  # Sphinx 8 returns ``Element | None`` here, Sphinx 9 ``reference | None``.
        if type == "class" and target in _BUILTIN_NAMES:
            searchmode = 1 if node.hasattr("refspecific") else 0
            if not self.find_obj(env, node.get("py:module"), node.get("py:class"), target, type, searchmode):
                return None
        return super().resolve_xref(env, fromdocname, builder, type, target, node, contnode)


def setup(app: Sphinx) -> dict[str, Any]:
    app.add_domain(_PythonDomainWithBuiltinXrefs, override=True)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
