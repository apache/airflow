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
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from docutils import nodes
from docutils.parsers.rst import Directive, directives
from docutils.parsers.rst.roles import code_role
from sphinx.transforms import SphinxTransform
from sphinx.transforms.post_transforms.code import HighlightLanguageTransform

if TYPE_CHECKING:
    from sphinx.application import Sphinx

LOGGER = logging.getLogger(__name__)

OriginalCodeBlock: Directive = directives._directives["code-block"]  # type: ignore[attr-defined]

_SUBSTITUTION_OPTION_NAME = "substitutions"


class SubstitutionCodeBlock(OriginalCodeBlock):  # type: ignore
    """Similar to CodeBlock but replaces placeholders with variables."""

    option_spec = OriginalCodeBlock.option_spec.copy()  # type: ignore[union-attr]
    option_spec[_SUBSTITUTION_OPTION_NAME] = directives.flag

    def run(self) -> list:
        """Decorate code block so that SubstitutionCodeBlockTransform will notice it"""
        [node] = super().run()

        if _SUBSTITUTION_OPTION_NAME in self.options:
            node.attributes["substitutions"] = True
        return [node]


class SubstitutionCodeBlockTransform(SphinxTransform):
    """Substitute ``|variables|`` in code and code-block nodes"""

    # Run before we highlight the code!
    default_priority = HighlightLanguageTransform.default_priority - 1

    def apply(self, **kwargs: Any) -> None:
        def condition(node):
            return isinstance(node, (nodes.literal_block, nodes.literal))

        for node in self.document.traverse(condition):
            # Guard: Only process Element nodes with a truthy substitution option
            if not (isinstance(node, nodes.Element) and node.attributes.get(_SUBSTITUTION_OPTION_NAME)):
                continue

            # Some nodes don't have a direct document property, so walk up until we find it
            document = node.document
            parent = node.parent
            while document is None:
                parent = parent.parent
                document = parent.document

            substitution_defs = document.substitution_defs
            for child in node.children:
                old_child = child
                # Only substitute for Text nodes
                if isinstance(child, nodes.Text):
                    new_text = str(child)
                    for name, value in substitution_defs.items():
                        replacement = value.astext()
                        new_text = new_text.replace(f"|{name}|", replacement)
                    # Only replace if the text actually changed
                    if new_text != str(child):
                        child_new = nodes.Text(new_text)
                        node.replace(old_child, child_new)
                # For non-Text nodes, do not replace

            # The highlighter checks this -- without this, it will refuse to apply highlighting
            node.rawsource = node.astext()


def substitution_code_role(*args, **kwargs) -> tuple[list, list[Any]]:
    """Decorate an inline code so that SubstitutionCodeBlockTransform will notice it"""
    [node], system_messages = code_role(*args, **kwargs)
    node[_SUBSTITUTION_OPTION_NAME] = True  # type: ignore[index]

    return [node], system_messages


substitution_code_role.options = {  # type: ignore
    "class": directives.class_option,
    "language": directives.unchanged,
}


class AddSpacepadSubstReference(SphinxTransform):
    """
    Add a custom ``|version-spacepad|`` replacement definition

    Since this desired replacement text is all just whitespace, we can't use
    the normal RST to define this, we instead of to create this definition
    manually after docutils has parsed the source files.
    """

    # Run as early as possible
    default_priority = 1

    def apply(self, **kwargs: Any) -> None:
        substitution_defs = self.document.substitution_defs
        version = substitution_defs["version"].astext()
        pad = " " * len(version)
        substitution_defs["version-spacepad"] = nodes.substitution_definition(version, pad)
        ...


def setup(app: Sphinx) -> dict:
    """Setup plugin"""
    app.add_config_value("substitutions", [], "html")
    directives.register_directive("code-block", SubstitutionCodeBlock)
    app.add_role("subst-code", substitution_code_role)
    app.add_post_transform(SubstitutionCodeBlockTransform)
    app.add_post_transform(AddSpacepadSubstReference)
    return {"parallel_write_safe": True, "parallel_read_safe": True}
