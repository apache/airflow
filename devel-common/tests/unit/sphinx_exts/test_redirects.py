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

import os
from types import SimpleNamespace
from unittest import mock

import pytest
from sphinx.builders.html import StandaloneHTMLBuilder

from sphinx_exts import redirects


@pytest.fixture
def app(tmp_path):
    source = tmp_path / "source"
    output = tmp_path / "output"
    source.mkdir()
    output.mkdir()

    return SimpleNamespace(
        srcdir=source,
        config=SimpleNamespace(
            redirects_file="redirects.txt",
            source_suffix={".rst": "restructuredtext"},
        ),
        builder=SimpleNamespace(outdir=output),
    )


@pytest.mark.parametrize(
    ("source", "target", "expected_source", "expected_target"),
    [
        pytest.param(
            "old.rst",
            "new.rst",
            "old.html",
            "new.html",
            id="root-page",
        ),
        pytest.param(
            "hooks/index.rst",
            "concepts.rst",
            "hooks/index.html",
            "../concepts.html",
            id="nested-page",
        ),
        pytest.param(
            "guides/nested/old.rst",
            "guides/new.rst",
            "guides/nested/old.html",
            "../../guides/new.html",
            id="deeply-nested-page",
        ),
        pytest.param(
            "old.rst",
            "new.rst#section",
            "old.html",
            "new.html#section",
            id="anchor",
        ),
        pytest.param(
            "_api/airflow/old.rst",
            "providers/example/new.rst",
            "_api/airflow/old.html",
            "../../providers/example/new.html",
            id="core-api-to-provider",
        ),
        pytest.param(
            "hooks/old.rst",
            "providers/example/new.rst",
            "hooks/old.html",
            "../../providers/example/new.html",
            id="provider-destination",
        ),
        pytest.param(
            "_api/airflow/providers/old.rst",
            "providers/example/new.rst",
            "_api/airflow/providers/old.html",
            "../../../../providers/example/new.html",
            id="provider-api-destination",
        ),
    ],
)
def test_iter_redirects(app, source, target, expected_source, expected_target):
    # The existing resolver uses the platform separator for parent prefixes.
    expected_target = expected_target.replace("../", f"..{os.path.sep}")
    (app.srcdir / "redirects.txt").write_text(f"{source} {target}\n", encoding="utf-8")

    assert list(redirects.iter_redirects(app)) == [(expected_source, expected_target)]


def test_iter_redirects_skips_comments_and_empty_lines(app):
    (app.srcdir / "redirects.txt").write_text(
        "# Old documentation paths\n"
        "\n"
        "   \n"
        "first.rst new-first.rst\n"
        "# Another comment\n"
        "second.rst new-second.rst\n",
        encoding="utf-8",
    )

    assert list(redirects.iter_redirects(app)) == [
        ("first.html", "new-first.html"),
        ("second.html", "new-second.html"),
    ]


def test_iter_redirects_without_redirect_file(app):
    assert list(redirects.iter_redirects(app)) == []


def test_iter_redirects_uses_configured_source_suffix(app):
    app.config.source_suffix = {".md": "markdown"}
    (app.srcdir / "redirects.txt").write_text("old.md new.md\n", encoding="utf-8")

    assert list(redirects.iter_redirects(app)) == [("old.html", "new.html")]


def test_generate_html_redirects(app):
    output = app.builder.outdir
    # A spec'd mock passes the existing HTML-builder isinstance check.
    app.builder = mock.Mock(spec=StandaloneHTMLBuilder)
    app.builder.outdir = output
    (app.srcdir / "redirects.txt").write_text(
        "old.rst new.rst\nhooks/index.rst concepts.rst\n",
        encoding="utf-8",
    )

    redirects.generate_redirects(app)

    assert (output / "old.html").read_text(encoding="utf-8") == (
        '<html><head><meta http-equiv="refresh" content="0; url=new.html"/></head></html>'
    )
    destination = f"..{os.path.sep}concepts.html"
    assert (output / "hooks/index.html").read_text(encoding="utf-8") == (
        f'<html><head><meta http-equiv="refresh" content="0; url={destination}"/></head></html>'
    )


def test_generate_redirects_skips_non_html_builder(app):
    (app.srcdir / "redirects.txt").write_text("old.rst new.rst\n", encoding="utf-8")

    redirects.generate_redirects(app)

    assert not list(app.builder.outdir.rglob("*"))


def test_generate_redirects_without_redirect_file(app):
    output = app.builder.outdir
    app.builder = mock.Mock(spec=StandaloneHTMLBuilder)
    app.builder.outdir = output

    redirects.generate_redirects(app)

    assert not list(output.rglob("*"))
