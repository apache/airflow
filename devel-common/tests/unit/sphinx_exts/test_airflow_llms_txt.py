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

import io
from pathlib import Path
from types import SimpleNamespace

import pytest
from docutils import nodes
from sphinx import addnodes
from sphinx.application import Sphinx
from sphinx.errors import ExtensionError

from sphinx_exts import airflow_llms_txt


def make_app(tmp_path, *, found_docs=()):
    source = tmp_path / "source"
    output = tmp_path / "output"
    source.mkdir()
    output.mkdir()
    (source / "redirects.txt").write_text("", encoding="utf-8")

    return SimpleNamespace(
        srcdir=source,
        outdir=output,
        builder=SimpleNamespace(name="html"),
        env=SimpleNamespace(found_docs=set(found_docs)),
        config=SimpleNamespace(
            redirects_file="redirects.txt",
            source_suffix={".rst": "restructuredtext"},
            llms_txt_enabled=True,
            root_doc="index",
        ),
    )


@pytest.mark.parametrize(
    ("source", "target", "expected"),
    [
        pytest.param(
            "old.rst",
            "new.rst",
            "new.html.md",
            id="normal",
        ),
        pytest.param(
            "hooks/index.rst",
            "concepts.rst",
            "../concepts.html.md",
            id="nested",
        ),
        pytest.param(
            "old.rst",
            "new.rst#section",
            "new.html.md#section",
            id="anchor",
        ),
        pytest.param(
            "old.rst",
            "new.rst?view=full#section",
            "new.html.md?view=full#section",
            id="query-and-anchor",
        ),
        pytest.param(
            "old.rst",
            "../other/new.rst",
            "../other/new.html",
            id="outside-package",
        ),
        pytest.param(
            "old.rst",
            "https://example.org/new.html",
            "https://example.org/new.html",
            id="external",
        ),
    ],
)
def test_write_markdown_redirects(tmp_path, source, target, expected):
    app = make_app(tmp_path)
    (app.srcdir / "redirects.txt").write_text(f"{source} {target}\n", encoding="utf-8")

    airflow_llms_txt._write_markdown_redirects(app)

    output = app.outdir / source.replace(".rst", ".html.md")
    assert output.read_text(encoding="utf-8") == (
        f"# Page moved\n\nThis page has moved to [the new page](<{expected}>).\n"
    )


def test_redirect_preserves_actual_document(tmp_path):
    app = make_app(tmp_path, found_docs={"old"})
    (app.srcdir / "redirects.txt").write_text("old.rst new.rst\n", encoding="utf-8")
    output = app.outdir / "old.html.md"
    output.write_text("# Actual documentation\n", encoding="utf-8")

    airflow_llms_txt._write_markdown_redirects(app)

    assert output.read_text(encoding="utf-8") == "# Actual documentation\n"


def test_redirect_updates_existing_stub(tmp_path):
    app = make_app(tmp_path)
    (app.srcdir / "redirects.txt").write_text("old.rst new.rst\n", encoding="utf-8")
    output = app.outdir / "old.html.md"
    output.write_text("Old redirect destination", encoding="utf-8")

    airflow_llms_txt._write_markdown_redirects(app)

    assert "(<new.html.md>)" in output.read_text(encoding="utf-8")
    assert "Old redirect destination" not in output.read_text(encoding="utf-8")


def test_complete_markdown_coverage(tmp_path):
    for name in ("index.html", "operators/llm.html"):
        page = tmp_path / name
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text("HTML", encoding="utf-8")
        page.with_name(page.name + ".md").write_text("# Documentation", encoding="utf-8")

    airflow_llms_txt._check_markdown_coverage(tmp_path)


def test_missing_markdown_lists_all_missing_pages(tmp_path):
    for name in ("index.html", "operators/llm.html"):
        page = tmp_path / name
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text("HTML", encoding="utf-8")

    with pytest.raises(ExtensionError) as exc:
        airflow_llms_txt._check_markdown_coverage(tmp_path)

    assert str(exc.value) == ("HTML pages missing Markdown copies:\n  index.html\n  operators/llm.html")


@pytest.mark.parametrize(
    "name",
    [
        "genindex.html",
        "search.html",
        "py-modindex.html",
        "_modules/index.html",
        "_modules/airflow/providers/example.html",
    ],
)
def test_coverage_excludes_special_pages(tmp_path, name):
    page = tmp_path / name
    page.parent.mkdir(parents=True, exist_ok=True)
    page.write_text("HTML", encoding="utf-8")

    airflow_llms_txt._check_markdown_coverage(tmp_path)


def test_missing_root_markdown_fails_build(tmp_path):
    app = make_app(tmp_path, found_docs={"index"})
    (app.outdir / "index.html").write_text("HTML", encoding="utf-8")

    with pytest.raises(ExtensionError, match="index.html"):
        airflow_llms_txt._write_llms_txt(app, None)


@pytest.mark.parametrize(
    ("builder", "enabled", "exception"),
    [
        pytest.param("html", True, RuntimeError("Build failed"), id="failed-build"),
        pytest.param("html", False, None, id="disabled"),
        pytest.param(
            airflow_llms_txt.MARKDOWN_BUILDER,
            True,
            None,
            id="markdown-build",
        ),
    ],
)
def test_index_generation_skipped(tmp_path, builder, enabled, exception):
    app = make_app(tmp_path)
    app.builder.name = builder
    app.config.llms_txt_enabled = enabled
    (app.outdir / "index.html").write_text("HTML", encoding="utf-8")

    airflow_llms_txt._write_llms_txt(app, exception)

    assert not (app.outdir / "llms.txt").exists()
    assert not (app.outdir / "llms-full.txt").exists()


def test_indexes_follow_navigation_and_use_versioned_urls(tmp_path):
    names = {"index", "guide", "second", "changelog", "commits"}
    app = make_app(tmp_path, found_docs=names)
    site_url = "https://airflow.apache.org/docs/apache-airflow-providers-common-ai/0.10.0"

    root = nodes.document("", "")
    navigation = addnodes.toctree()
    navigation["caption"] = "Guides"
    navigation["entries"] = [
        ("Getting started", "guide"),
        (None, "second"),
        (None, "changelog"),
        (None, "commits"),
    ]
    root += navigation

    doctrees = {"index": root}
    for name in names - {"index"}:
        tree = nodes.document("", "")
        tree += nodes.paragraph("", f"This is the documentation description for {name}.")
        doctrees[name] = tree

    app.env.titles = {name: nodes.title("", name.capitalize()) for name in names}
    app.env.toctree_includes = {}
    app.env.get_doctree = doctrees.__getitem__

    app.config.llms_txt_site_url = site_url + "/"
    app.config.llms_txt_optional_docs = ["changelog"]
    app.config.llms_txt_skip_docs = ["commits"]
    app.config.project = "Common AI"
    app.config.release = "0.10.0"
    app.config.llms_txt_description = "Common AI documentation."
    app.config.llms_txt_intro = "Install the Common AI provider."
    app.config.llms_txt_root_description = ""

    for name in names:
        (app.outdir / f"{name}.html").write_text("HTML", encoding="utf-8")
        content = f"# {name.capitalize()}\n"
        if name == "guide":
            content += "[Next](second.html.md)\n"
        (app.outdir / f"{name}.html.md").write_text(content, encoding="utf-8")

    airflow_llms_txt._write_llms_txt(app, None)

    index = (app.outdir / "llms.txt").read_text(encoding="utf-8")
    full = (app.outdir / "llms-full.txt").read_text(encoding="utf-8")

    assert index.startswith("# Common AI 0.10.0\n")
    assert "Install the Common AI provider." in index
    assert "## Guides" in index
    assert (
        f"- [Getting started]({site_url}/guide.html.md): This is the documentation description for guide."
    ) in index
    assert index.index("/guide.html.md") < index.index("/second.html.md")

    optional = index.split("## Optional\n", 1)[1]
    assert f"- [Changelog]({site_url}/changelog.html.md)" in optional
    assert f"[llms-full.txt]({site_url}/llms-full.txt)" in optional
    assert "/commits.html.md" not in index

    assert full.index("Source: " + site_url + "/index.html.md") < full.index(
        "Source: " + site_url + "/guide.html.md"
    )
    assert full.index("Source: " + site_url + "/guide.html.md") < full.index(
        "Source: " + site_url + "/second.html.md"
    )
    assert f"[Next]({site_url}/second.html.md)" in full
    assert "Source: " + site_url + "/changelog.html.md" not in full
    assert "Source: " + site_url + "/commits.html.md" not in full


@pytest.mark.parametrize(
    ("inside_repo", "source_url", "builder"),
    [
        pytest.param(
            True, "https://github.com/apache/airflow/blob/abc", airflow_llms_txt.MARKDOWN_BUILDER, id="linked"
        ),
        pytest.param(
            False,
            "https://github.com/apache/airflow/blob/abc",
            airflow_llms_txt.MARKDOWN_BUILDER,
            id="outside-repo",
        ),
        pytest.param(True, "", airflow_llms_txt.MARKDOWN_BUILDER, id="no-source-url"),
        pytest.param(True, "https://github.com/apache/airflow/blob/abc", "html", id="html-unchanged"),
    ],
)
def test_example_source_links(tmp_path, monkeypatch, inside_repo, source_url, builder):
    monkeypatch.syspath_prepend(str(Path(airflow_llms_txt.__file__).parent))

    from exampleinclude import ExampleHeader

    repo = tmp_path / "repo"
    example = (
        repo / "providers/common/ai/tests/system/example.py"
        if inside_repo
        else tmp_path / "outside/example.py"
    )
    tree = nodes.document("", "")
    header = ExampleHeader()
    header["filename"] = str(example)
    tree += header

    app = SimpleNamespace(
        builder=SimpleNamespace(name=builder),
        config=SimpleNamespace(
            llms_txt_repo_root=str(repo),
            llms_txt_source_url=source_url + "/" if source_url else "",
        ),
    )

    airflow_llms_txt._link_example_sources(app, tree)

    if builder == "html":
        assert tree.children == [header]
    elif not inside_repo or not source_url:
        assert not tree.children
    else:
        assert not list(tree.findall(ExampleHeader))
        references = list(tree.findall(nodes.reference))
        assert len(references) == 1
        assert references[0]["refuri"] == (source_url + "/providers/common/ai/tests/system/example.py")
        assert references[0].astext() == ("providers/common/ai/tests/system/example.py")
        assert tree.astext() == ("Excerpt from providers/common/ai/tests/system/example.py:")


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param(
            "https://example.org/docs/|version|/guide",
            "https://example.org/docs/0.10.0/guide",
            id="version",
        ),
        pytest.param(
            "https://github.com/apache/airflow/blob/release-tag/example.py",
            "https://github.com/apache/airflow/blob/head-sha/example.py",
            id="github-file",
        ),
        pytest.param(
            "https://github.com/apache/airflow/tree/release-tag/examples",
            "https://github.com/apache/airflow/tree/head-sha/examples",
            id="github-directory",
        ),
        pytest.param(
            "/docs/apache-airflow/stable/index.html",
            "https://airflow.apache.org/docs/apache-airflow/stable/index.html",
            id="site-root",
        ),
        pytest.param(
            "//example.org/page",
            "//example.org/page",
            id="protocol-relative-unchanged",
        ),
        pytest.param(
            "concepts.html.md#section",
            "concepts.html.md#section",
            id="relative-unchanged",
        ),
        pytest.param(
            "https://example.org/page",
            "https://example.org/page",
            id="external-unchanged",
        ),
    ],
)
def test_clean_markdown_link_targets(url, expected):
    tree = nodes.document("", "")
    reference = nodes.reference("", "Link", refuri=url)
    tree += reference

    app = SimpleNamespace(
        builder=SimpleNamespace(name=airflow_llms_txt.MARKDOWN_BUILDER),
        config=SimpleNamespace(
            version="0.10.0",
            llms_txt_site_url=("https://airflow.apache.org/docs/common-ai/0.10.0/"),
            llms_txt_release_source_url=("https://github.com/apache/airflow/blob/release-tag/"),
            llms_txt_source_url=("https://github.com/apache/airflow/blob/head-sha/"),
        ),
    )

    airflow_llms_txt._clean_markdown_doctree(app, tree, "index")

    assert reference["refuri"] == expected


def test_clean_markdown_removes_comments_and_empty_quotes():
    tree = nodes.document("", "")
    tree += nodes.comment("", "License header")
    tree += nodes.block_quote()
    populated_quote = nodes.block_quote()
    populated_quote += nodes.paragraph("", "Keep this quotation.")
    tree += populated_quote

    app = SimpleNamespace(
        builder=SimpleNamespace(name=airflow_llms_txt.MARKDOWN_BUILDER),
        config=SimpleNamespace(
            llms_txt_site_url="https://airflow.apache.org/",
            llms_txt_release_source_url="",
            llms_txt_source_url="",
        ),
    )

    airflow_llms_txt._clean_markdown_doctree(app, tree, "index")

    assert not list(tree.findall(nodes.comment))
    assert list(tree.findall(nodes.block_quote)) == [populated_quote]
    assert tree.astext() == "Keep this quotation."


def test_clean_doctree_leaves_html_unchanged():
    tree = nodes.document("", "")
    comment = nodes.comment("", "License header")
    reference = nodes.reference("", "Link", refuri="/docs/|version|/")
    tree += comment
    tree += reference
    app = SimpleNamespace(builder=SimpleNamespace(name="html"))

    airflow_llms_txt._clean_markdown_doctree(app, tree, "index")

    assert comment in tree.children
    assert reference["refuri"] == "/docs/|version|/"


@pytest.mark.parametrize(
    ("markdown", "expected"),
    [
        pytest.param(
            "[Next](other.html.md#section)",
            "[Next](https://example.org/docs/other.html.md#section)",
            id="relative-link",
        ),
        pytest.param(
            "[Parent](../index.html.md)",
            "[Parent](https://example.org/index.html.md)",
            id="parent-link",
        ),
        pytest.param(
            "[Section](#setup)",
            "[Section](https://example.org/docs/page.html.md#setup)",
            id="same-page-link",
        ),
        pytest.param(
            "[External](https://other.example/page)",
            "[External](https://other.example/page)",
            id="external-link",
        ),
        pytest.param(
            "`[Example](other.html.md)` [Next](other.html.md)",
            "`[Example](other.html.md)` [Next](https://example.org/docs/other.html.md)",
            id="inline-code",
        ),
        pytest.param(
            "```python\n[Example](other.html.md)\n```\n[Next](other.html.md)",
            "```python\n[Example](other.html.md)\n```\n[Next](https://example.org/docs/other.html.md)",
            id="fenced-code",
        ),
    ],
)
def test_absolute_links(markdown, expected):
    assert airflow_llms_txt._absolute_links(markdown, "https://example.org/docs/page.html.md") == expected


def test_navigation_handles_nested_pages_duplicates_and_cycles():
    env = SimpleNamespace(
        toctree_includes={
            "index": ["guide", "second", "guide"],
            "guide": ["child", "second"],
            "child": ["index"],
        }
    )
    seen = set()

    assert airflow_llms_txt._toctree_docs(env, "index", seen) == [
        ("index", None),
        ("guide", None),
        ("child", None),
        ("second", None),
    ]
    assert airflow_llms_txt._toctree_docs(env, "guide", seen) == []


@pytest.mark.parametrize("markdown_build", [True, False])
def test_markdown_doctree_cache_is_separate(tmp_path, markdown_build):
    original = tmp_path / "html-doctrees"
    app = SimpleNamespace(
        outdir=tmp_path / "markdown-output",
        doctreedir=original,
        tags=SimpleNamespace(has=lambda tag: markdown_build and tag == "sphinx_llm_markdown"),
    )

    airflow_llms_txt._separate_markdown_doctrees(app, SimpleNamespace())

    expected = app.outdir / ".doctrees" if markdown_build else original
    assert app.doctreedir == expected


@pytest.fixture
def rendered_markdown(tmp_path, monkeypatch):
    source = tmp_path / "source"
    output = tmp_path / "output"
    source.mkdir()

    # The extension imports redirects and exampleinclude by bare module name.
    extension_dir = Path(airflow_llms_txt.__file__).parent
    source_root = extension_dir.parent

    (source / "conf.py").write_text(
        "import sys\n"
        f"sys.path.insert(0, {str(source_root)!r})\n"
        f"sys.path.insert(0, {str(extension_dir)!r})\n"
        "extensions = [\n"
        '    "redirects",\n'
        '    "sphinx_exts.airflow_llms_txt",\n'
        '    "sphinxcontrib.mermaid",\n'
        "]\n"
        'root_doc = "index"\n'
        'redirects_file = "redirects.txt"\n'
        "llms_txt_enabled = True\n"
        'llms_txt_suffix_mode = "append"\n'
        "llms_txt_build_parallel = False\n",
        encoding="utf-8",
    )
    (source / "redirects.txt").write_text("", encoding="utf-8")
    (source / "index.rst").write_text(
        """\
Rendering
=========

.. _setup-section:

Setup
-----

See `Setup`_.

.. note::

   Important configuration advice.

Following paragraph outside the note.

.. list-table::
   :header-rows: 1

   * - Name
     - Value
   * - Short
     - A much longer value

.. code-block:: python
   :caption: Example code

   print("hello")

.. mermaid::

   graph TD
       A --> B

.. py:function:: configure(*, model_id)

   Configure a model.

.. toctree::
   :caption: Further reading

   child
""",
        encoding="utf-8",
    )
    (source / "child.rst").write_text(
        "Child page\n==========\n\nChild documentation.\n",
        encoding="utf-8",
    )

    warnings = io.StringIO()
    app = Sphinx(
        srcdir=str(source),
        confdir=str(source),
        outdir=str(output),
        doctreedir=str(tmp_path / "doctrees"),
        buildername="html",
        status=io.StringIO(),
        warning=warnings,
        freshenv=True,
    )
    app.build(force_all=True)

    assert app.statuscode == 0, warnings.getvalue()
    pages = list(output.rglob("index*.md"))
    assert len(pages) == 1
    return pages[0].read_text(encoding="utf-8")


def test_rendered_mermaid(rendered_markdown):
    assert "```mermaid\n" in rendered_markdown
    assert "graph TD\n" in rendered_markdown
    assert "A --> B" in rendered_markdown


def test_rendered_code_caption_and_sample(rendered_markdown):
    assert "**Example code**" in rendered_markdown
    assert 'print("hello")' in rendered_markdown
    assert rendered_markdown.index("**Example code**") < (rendered_markdown.index('print("hello")'))


def test_rendered_keyword_only_marker(rendered_markdown):
    signature = next(line for line in rendered_markdown.splitlines() if "configure(" in line)
    assert "*" in signature
    assert "model_id" in signature


def test_rendered_toctree_caption(rendered_markdown):
    assert "**Further reading**" in rendered_markdown
    assert "[Child page]" in rendered_markdown
    assert "# Further reading" not in rendered_markdown


def test_rendered_admonition_has_clear_boundaries(rendered_markdown):
    lines = rendered_markdown.splitlines()
    assert any(line.startswith(">") and "**Note:**" in line for line in lines)
    assert any(line.startswith(">") and "Important configuration advice." in line for line in lines)
    following = next(line for line in lines if "Following paragraph outside the note." in line)
    assert not following.startswith(">")


def test_rendered_table_is_compact(rendered_markdown):
    table_lines = [line.strip() for line in rendered_markdown.splitlines() if line.strip().startswith("|")]
    assert "| Name | Value |" in table_lines
    assert "|---|---|" in table_lines
    assert "| Short | A much longer value |" in table_lines


def test_rendered_same_page_link(rendered_markdown):
    assert "[Setup]()" not in rendered_markdown
    assert "[Setup]" in rendered_markdown
    assert "#setup-section" in rendered_markdown or "#setup" in rendered_markdown
