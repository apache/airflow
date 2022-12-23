# flake8: noqa
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
from __future__ import annotations

"""Nice formatted include for examples"""
import traceback
from os import path

from docutils import nodes

# No stub exists for docutils.parsers.rst.directives. See https://github.com/python/typeshed/issues/5755.
from docutils.parsers.rst import directives  # type: ignore[attr-defined]
from sphinx.directives.code import LiteralIncludeReader
from sphinx.ext.viewcode import viewcode_anchor
from sphinx.locale import _
from sphinx.pycode import ModuleAnalyzer
from sphinx.util import logging, parselinenos
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import set_source_info

try:
    import sphinx_airflow_theme  # noqa: autoflake

    airflow_theme_is_available = True
except ImportError:
    airflow_theme_is_available = False

logger = logging.getLogger(__name__)


class ExampleHeader(nodes.reference, nodes.FixedTextElement):
    """Header for examples."""


class ExampleInclude(SphinxDirective):
    """
    Like ``.. literalinclude:: ``, but it does not support caption option.
    Adds a header with a reference to  the full source code

    Based on:
    https://raw.githubusercontent.com/sphinx-doc/sphinx/v1.8.3/sphinx/directives/code.py
    """

    has_content = False
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = True
    option_spec = {
        "dedent": int,
        "linenos": directives.flag,
        "lineno-start": int,
        "lineno-match": directives.flag,
        "tab-width": int,
        "language": directives.unchanged_required,
        "encoding": directives.encoding,
        "pyobject": directives.unchanged_required,
        "lines": directives.unchanged_required,
        "start-after": directives.unchanged_required,
        "end-before": directives.unchanged_required,
        "start-at": directives.unchanged_required,
        "end-at": directives.unchanged_required,
        "prepend": directives.unchanged_required,
        "append": directives.unchanged_required,
        "emphasize-lines": directives.unchanged_required,
        "class": directives.class_option,
        "name": directives.unchanged,
        "diff": directives.unchanged_required,
    }

    def run(self):
        document = self.state.document
        if not document.settings.file_insertion_enabled:
            return [document.reporter.warning("File insertion disabled", line=self.lineno)]
        # convert options['diff'] to absolute a_path
        if "diff" in self.options:
            _, a_path = self.env.relfn2path(self.options["diff"])
            self.options["diff"] = a_path

        try:
            location = self.state_machine.get_source_and_line(self.lineno)
            rel_filename, filename = self.env.relfn2path(self.arguments[0])
            self.env.note_dependency(rel_filename)

            reader = LiteralIncludeReader(filename, self.options, self.config)
            text, lines = reader.read(location=location)

            retnode = nodes.literal_block(text, text, source=filename)
            set_source_info(self, retnode)
            if self.options.get("diff"):  # if diff is set, set udiff
                retnode["language"] = "udiff"
            elif "language" in self.options:
                retnode["language"] = self.options["language"]
            retnode["linenos"] = (
                "linenos" in self.options or "lineno-start" in self.options or "lineno-match" in self.options
            )
            retnode["classes"] += self.options.get("class", [])
            extra_args = retnode["highlight_args"] = {}
            if "emphasize-lines" in self.options:
                hl_lines = parselinenos(self.options["emphasize-lines"], lines)
                if any(i >= lines for i in hl_lines):
                    logger.warning(
                        "line number spec is out of range(1-%d): %r", lines, self.options["emphasize-lines"]
                    )
                extra_args["hl_lines"] = [x + 1 for x in hl_lines if x < lines]
            extra_args["linenostart"] = reader.lineno_start

            container_node = nodes.compound(classes=["example-block-wrapper"])
            container_node += ExampleHeader(filename=filename)
            container_node += retnode
            retnode = container_node

            return [retnode]
        except Exception as exc:
            return [document.reporter.warning(str(exc), line=self.lineno)]


def register_source(app, env, modname):
    """
    Registers source code.

    :param app: application
    :param env: environment of the plugin
    :param modname: name of the module to load
    :return: True if the code is registered successfully, False otherwise
    """
    entry = env._viewcode_modules.get(modname, None)
    if entry is False:
        print(f"[{modname}] Entry is false for ")
        return False

    code_tags = app.emit_firstresult("viewcode-find-source", modname)
    if code_tags is None:
        try:
            analyzer = ModuleAnalyzer.for_module(modname)
        except Exception as ex:
            logger.info(
                'Module "%s" could not be loaded. Full source will not be available. "%s"', modname, ex
            )
            # We cannot use regular warnings or exception methods because those warnings are interpreted
            # by running python process and converted into "real" warnings, so we need to print the
            # traceback here at info level
            tb = traceback.format_exc()
            logger.info("%s", tb)
            env._viewcode_modules[modname] = False
            return False

        if not isinstance(analyzer.code, str):
            code = analyzer.code.decode(analyzer.encoding)
        else:
            code = analyzer.code

        analyzer.find_tags()
        tags = analyzer.tags

    else:
        code, tags = code_tags

    if entry is None or entry[0] != code:
        entry = code, tags, {}, ""
        env._viewcode_modules[modname] = entry

    return True


def create_node(env, relative_path, show_button):
    """
    Creates documentation node for example include.

    :param env: environment of the documentation
    :param relative_path: path of the code
    :param show_button: whether to show "view code" button
    :return paragraph with the node
    """
    pagename = "_modules/" + relative_path[:-3]

    header_classes = ["example-header"]
    if show_button:
        header_classes += ["example-header--with-button"]
    paragraph = nodes.paragraph(relative_path, classes=header_classes)
    paragraph += nodes.inline("", relative_path, classes=["example-title"])
    if show_button:
        pending_ref = viewcode_anchor(
            reftarget=pagename,
            refid="",
            refdoc=env.docname,
            classes=["example-header-button viewcode-button"],
        )
        pending_ref += nodes.inline("", _("View Source"))
        paragraph += pending_ref

    return paragraph


def doctree_read(app, doctree):
    """
    Reads documentation tree for the application and register sources in the generated documentation.

    :param app: application
    :param doctree: documentation tree

    :return None

    """
    env = app.builder.env
    if not hasattr(env, "_viewcode_modules"):
        env._viewcode_modules = {}

    if app.builder.name == "singlehtml":
        return

    for objnode in doctree.traverse(ExampleHeader):
        filepath = objnode.get("filename")
        relative_path = path.relpath(
            filepath, path.commonprefix([app.config.exampleinclude_sourceroot, filepath])
        )
        modname = relative_path.replace("/", ".")[:-3]
        show_button = register_source(app, env, modname)
        onlynode = create_node(env, relative_path, show_button)

        objnode.replace_self(onlynode)


def setup(app):
    """
    Sets the plugin up and returns configuration of the plugin.

    :param app: application.
    :return json description of the configuration that is needed by the plugin.
    """
    directives.register_directive("exampleinclude", ExampleInclude)
    app.connect("doctree-read", doctree_read)
    app.add_config_value("exampleinclude_sourceroot", None, "env")
    if not airflow_theme_is_available:
        # Sphinx airflow theme has its own styles.
        app.add_css_file("exampleinclude.css")
    return {"version": "builtin", "parallel_read_safe": True, "parallel_write_safe": True}
