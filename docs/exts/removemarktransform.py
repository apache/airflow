# Disable Flake8 because of all the sphinx imports
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
"""Remove Transform Mark for Sphinx"""
from __future__ import annotations

import re

from docutils import nodes
from pygments.lexers import Python3Lexer, PythonLexer, guess_lexer
from sphinx.transforms import SphinxTransform
from sphinx.transforms.post_transforms.code import TrimDoctestFlagsTransform

docmark_re = re.compile(r"\s*#\s*\[(START|END)\s*[a-z_A-Z]+].*$", re.MULTILINE)


class TrimDocMarkerFlagsTransform(SphinxTransform):
    """
    Trim doc marker like ``# [START howto_concept]` from python code-blocks.

    Based on:
    https://github.com/sphinx-doc/sphinx/blob/master/sphinx/transforms/post_transforms/code.py
    class TrimDoctestFlagsTransform
    """

    default_priority = TrimDoctestFlagsTransform.default_priority + 1

    def apply(self, **kwargs):
        for node in self.document.traverse(nodes.literal_block):
            if self.is_pycode(node):
                source = node.rawsource
                source = docmark_re.sub("", source)
                node.rawsource = source
                node[:] = [nodes.Text(source)]

    @staticmethod
    def is_pycode(node: nodes.literal_block) -> bool:
        """Checks if the node is literal block of python"""
        if node.rawsource != node.astext():
            return False  # skip parsed-literal node

        language = node.get("language")
        if language in ("py", "py3", "python", "python3", "default"):
            return True
        elif language == "guess":
            try:
                lexer = guess_lexer(node.rawsource)
                return isinstance(lexer, (PythonLexer, Python3Lexer))
            except Exception:
                pass

        return False


def setup(app):
    """Sets the transform up"""
    app.add_post_transform(TrimDocMarkerFlagsTransform)

    return {"version": "builtin", "parallel_read_safe": False, "parallel_write_safe": False}
