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

"""Sphinx extension for Pagefind search integration."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sphinx.application import Sphinx

from sphinx_exts.pagefind_search.builder import build_index_finished, copy_static_files


def inject_search_html(app: Sphinx, pagename: str, templatename: str, context: dict, doctree) -> None:
    """Inject Pagefind search modal and button HTML into page context."""
    template_dir = Path(__file__).parent / "templates"

    modal_file = template_dir / "search-modal.html"
    button_file = template_dir / "search-button.html"

    if not modal_file.exists():
        return

    modal_content = modal_file.read_text()
    button_content = button_file.read_text() if button_file.exists() else ""

    from jinja2 import Template

    modal_template = Template(modal_content)
    button_template = Template(button_content)

    search_modal_html = modal_template.render(pathto=context.get("pathto"))
    search_button_html = button_template.render(pathto=context.get("pathto"))

    context["pagefind_search_modal"] = search_modal_html
    context["pagefind_search_button"] = search_button_html

    if "body" in context:
        context["body"] = search_modal_html + search_button_html + context["body"]


def setup(app: Sphinx) -> dict[str, Any]:
    """Setup the Pagefind search extension."""
    app.add_config_value("pagefind_enabled", True, "html", [bool])
    app.add_config_value("pagefind_verbose", False, "html", [bool])
    app.add_config_value("pagefind_root_selector", "main", "html", [str])
    app.add_config_value(
        "pagefind_exclude_selectors",
        [
            ".headerlink",
            ".toctree-wrapper",
            "nav",
            "footer",
            ".td-sidebar",
            ".breadcrumb",
            ".navbar",
            ".dropdown-menu",
            ".docs-version-selector",
            "[role='navigation']",
            ".d-print-none",
            ".pagefind-search-button",
        ],
        "html",
        [list],
    )
    app.add_config_value("pagefind_glob", "**/*.html", "html", [str])
    app.add_config_value("pagefind_exclude_patterns", [], "html", [list])
    app.add_config_value("pagefind_custom_records", [], "html", [list])
    app.add_config_value("pagefind_content_weighting", True, "html", [bool])
    app.add_config_value("pagefind_enable_playground", False, "html", [bool])

    app.add_css_file("css/pagefind.css")
    app.add_js_file("js/search.js")

    app.connect("html-page-context", inject_search_html)
    app.connect("build-finished", copy_static_files, priority=100)
    app.connect("build-finished", build_index_finished, priority=900)

    return {
        "version": "1.0.0",
        "parallel_read_safe": True,
        "parallel_write_safe": False,
    }
