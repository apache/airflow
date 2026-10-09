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
Publish Markdown copies of each page and an ``llms.txt`` index for LLMs and coding agents.

`sphinx-llm <https://github.com/NVIDIA/sphinx-llm>`__ does the heavy lifting: it runs a second
Sphinx build with a Markdown builder, writes ``page.html.md`` next to every ``page.html`` and
adds ``<link rel="alternate" type="text/markdown">`` to each HTML page. This extension adds what
Airflow docs need on top of it:

* Markdown handlers for nodes the stock translator drops or misrenders: ``mermaid`` diagrams,
  code-block captions, keyword-only ``*`` markers in API signatures and toctree captions.
* License-header comments are removed, so they do not open every page.
* ``exampleinclude`` headers link to the full example file on GitHub at the release tag, since
  the excerpts leave out imports.
* ``llms.txt`` is rewritten: sections follow the root toctree captions, every link is absolute,
  and descriptions come from each page's first paragraph rather than its first line of text.
* ``llms-full.txt`` holds the same pages in the same order, each headed by its URL, with links
  made absolute so they still work once the pages share one file.
"""

from __future__ import annotations

import posixpath
import re
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urljoin, urlsplit, urlunsplit

from docutils import nodes
from sphinx import addnodes
from sphinx.errors import ExtensionError
from sphinx.util import logging
from sphinx.util.matching import Matcher
from sphinx_llm.markdown_builder import SphinxLlmMarkdownBuilder, SphinxLlmMarkdownTranslator
from sphinx_markdown_builder.contexts import IndentContext, SubContext, SubContextParams, TableContext

from sphinx_exts.redirects import iter_redirects

if TYPE_CHECKING:
    from sphinx.application import Sphinx
    from sphinx.config import Config
    from sphinx.domains.std import StandardDomain
    from sphinx.environment import BuildEnvironment

logger = logging.getLogger(__name__)

MARKDOWN_BUILDER = SphinxLlmMarkdownBuilder.name
# Pages whose first paragraph is shorter than this get no description rather than a fragment.
MIN_DESCRIPTION_LENGTH = 20
MAX_DESCRIPTION_LENGTH = 240
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z`])")
# Paragraphs under these are not the page's own opening prose.
_NOT_PROSE = (
    nodes.Admonition,
    nodes.comment,
    nodes.compound,
    nodes.list_item,
    nodes.system_message,
    nodes.table,
    nodes.topic,
    addnodes.desc,
)
# llms.txt is built from the HTML build's doctrees, where Sphinx has already curled the quotes.
# The files are served without a charset, so keep them ASCII (see SMARTQUOTES_EXCLUDES).
_TYPOGRAPHY = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u2026": "...",
        "\u00a0": " ",
    }
)
_INLINE_CODE = re.compile(r"(`[^`]*`)")
_MARKDOWN_LINK = re.compile(r"(\]\()([^)\s]+)(\))")


class CompactTableContext(TableContext):
    """
    A pipe table without the column padding.

    ``tabulate`` pads every cell to the widest one in its column, which is pure whitespace to a
    model reading the file: about a tenth of ``llms-full.txt`` before this.
    """

    def make(self) -> str:
        ctx = SubContext()
        prefix = self.internal_context.make()
        if prefix:
            ctx.add(prefix)
        rows = [self.make_row(row) for row in [*self.headers, *self.body]]
        if rows:
            width = max(len(row) for row in rows)
            lines = [
                "| "
                + " | ".join(cell.replace("|", r"\|") for cell in [*row, *[""] * (width - len(row))])
                + " |"
                for row in rows
            ]
            lines.insert(1, "|" + "---|" * width)
            ctx.add("\n".join(lines), prefix_eol=2)
        return ctx.make()


class AirflowMarkdownTranslator(SphinxLlmMarkdownTranslator):
    """Render the Sphinx nodes Airflow docs use that sphinx-markdown-builder skips or misrenders."""

    def _push_box(self, title: str) -> None:
        # Admonitions become a quoted block that starts with their label, rather than an
        # ``#### NOTE`` heading that breaks the heading order and never says where the note ends.
        self._push_context(IndentContext("> ", empty=True, params=SubContextParams(2, 2)))
        self.add(f"**{title.capitalize()}:**", suffix_eol=2)

    def _fetch_ref_uri(self, node: nodes.reference) -> str:
        # A link to a section on the same page (```Confidence`_``) has a refid but no refuri and
        # no ``internal`` flag, so the base translators leave its target empty.
        if node.get("refid") and not node.get("refuri"):
            return self.builder.link_token(self.builder.current_doc_name, node["refid"])
        return super()._fetch_ref_uri(node)

    def visit_meta(self, node: nodes.Element) -> None:
        raise nodes.SkipNode

    def visit_toctree(self, node: addnodes.toctree) -> None:
        # Hidden toctrees can reach the writer unresolved; like HTML, show nothing for them.
        # Visible ones are resolved into link lists before this, so any left are unexpected.
        if not node.get("hidden"):
            super().unknown_visit(node)
        raise nodes.SkipNode

    def visit_table(self, node: nodes.Element) -> None:
        self._push_context(CompactTableContext(params=SubContextParams(2, 1)))

    def depart_table(self, node: nodes.Element) -> None:
        self._pop_context(node)

    def visit_mermaid(self, node: nodes.Element) -> None:
        self.add(f"```mermaid\n{node['code'].strip()}\n```", prefix_eol=2, suffix_eol=2)
        raise nodes.SkipNode

    def visit_caption(self, node: nodes.Element) -> None:
        self.add(f"**{node.astext()}**", prefix_eol=2, suffix_eol=1)
        raise nodes.SkipNode

    # ``abbreviation`` wraps the bare ``*`` that marks keyword-only parameters in signatures.
    def visit_abbreviation(self, node: nodes.Element) -> None:
        pass

    def depart_abbreviation(self, node: nodes.Element) -> None:
        pass

    def visit_title(self, node: nodes.Element) -> None:
        # Render navigation captions in bold rather than as section headings.
        parent = node.parent
        if (isinstance(parent, addnodes.compact_paragraph) and parent.get("toctree")) or (
            isinstance(parent, nodes.compound) and "toctree-wrapper" in parent.get("classes", [])
        ):
            self.add(f"**{node.astext()}**", prefix_eol=2, suffix_eol=1)
            raise nodes.SkipNode

        super().visit_title(node)

    def depart_title(self, node: nodes.Element) -> None:
        self._pop_context(node)


def _escape_link_text(text: str) -> str:
    return text.replace("[", r"\[").replace("]", r"\]")


def _inline_markdown(node: nodes.Node, env: BuildEnvironment) -> str:
    """Flatten a title or paragraph to one line of Markdown, keeping inline code as code."""
    parts: list[str] = []
    for child in node.children:
        if isinstance(child, addnodes.pending_xref) and not child.get("refexplicit"):
            parts.append(_xref_title(child, env) or _inline_markdown(child, env))
        elif isinstance(child, nodes.literal):
            parts.append(f"`{child.astext()}`")
        elif isinstance(child, nodes.Text):
            parts.append(child.astext())
        elif isinstance(child, nodes.Element):
            parts.append(_inline_markdown(child, env))
    return re.sub(r"\s+", " ", "".join(parts)).strip().translate(_TYPOGRAPHY)


def _xref_title(xref: addnodes.pending_xref, env: BuildEnvironment) -> str | None:
    """
    Return the title a ``:doc:`` or ``:ref:`` without explicit text resolves to.

    Doctrees read back from the environment are unresolved, so such a reference still holds its
    raw target (``operators/llm``) rather than the page or section title the HTML shows.
    """
    target = xref.get("reftarget", "")
    if xref.get("reftype") == "doc":
        refdoc = xref.get("refdoc", "")
        docname = (
            target.lstrip("/")
            if target.startswith("/")
            else posixpath.join(posixpath.dirname(refdoc), target)
        )
        title = env.titles.get(posixpath.normpath(docname))
        return _inline_markdown(title, env) if title else None
    if xref.get("reftype") == "ref":
        domain = cast("StandardDomain", env.get_domain("std"))
        label = domain.labels.get(target.lower())
        return label[2] if label else None
    return None


def _separate_markdown_doctrees(app: Sphinx, config: Config) -> None:
    """Give the Markdown build its own doctree cache."""
    # sphinx-llm's sequential build shares the HTML cache, which can reuse
    # incompatible doctrees or overwrite them. Set a separate cache before
    # Sphinx creates the environment.

    if app.tags.has("sphinx_llm_markdown"):
        app.doctreedir = Path(app.outdir) / ".doctrees"


def _is_markdown_build(app: Sphinx) -> bool:
    return app.builder is not None and app.builder.name == MARKDOWN_BUILDER


def _link_example_sources(app: Sphinx, doctree: nodes.document) -> None:
    """
    Replace each ``exampleinclude`` header with a link to the whole example file.

    Runs before ``exampleinclude``'s own ``doctree-read`` handler, which would otherwise turn the
    header into a bare module path plus a "View Source" button that has no Markdown equivalent.
    """
    if not _is_markdown_build(app):
        return
    # Imported by bare name, as conf.py loads it: another import path gives a different class.
    from exampleinclude import ExampleHeader

    repo_root = Path(app.config.llms_txt_repo_root).resolve()
    source_url = app.config.llms_txt_source_url.rstrip("/")
    for header in list(doctree.findall(ExampleHeader)):
        path = Path(header["filename"]).resolve()
        if not source_url or not path.is_relative_to(repo_root):
            header.parent.remove(header)
            continue
        repo_path = path.relative_to(repo_root).as_posix()
        paragraph = nodes.paragraph()
        paragraph += nodes.Text("Excerpt from ")
        link = nodes.reference("", "", refuri=f"{source_url}/{repo_path}", internal=False)
        link += nodes.literal(repo_path, repo_path)
        paragraph += link
        paragraph += nodes.Text(":")
        header.replace_self(paragraph)


def _clean_markdown_doctree(app: Sphinx, doctree: nodes.document, docname: str) -> None:
    """Remove RST comments and normalize version, GitHub source and site-root links for Markdown."""

    if not _is_markdown_build(app):
        return
    site_root = urljoin(app.config.llms_txt_site_url, "/")
    release_url = app.config.llms_txt_release_source_url
    source_url = app.config.llms_txt_source_url
    for reference in doctree.findall(nodes.reference):
        refuri = reference.get("refuri", "")
        if "|version|" in refuri:
            refuri = reference["refuri"] = refuri.replace("|version|", app.config.version)
        if release_url and source_url and release_url != source_url:
            for kind in ("blob", "tree"):
                prefix = release_url.replace("/blob/", f"/{kind}/")
                if refuri.startswith(prefix):
                    refuri = reference["refuri"] = (
                        source_url.replace("/blob/", f"/{kind}/") + refuri[len(prefix) :]
                    )
        # Links into other packages' docs are site-root relative, which means nothing to a model
        # that got the page without its URL.
        if refuri.startswith("/") and not refuri.startswith("//"):
            reference["refuri"] = urljoin(site_root, refuri)
    for comment in list(doctree.findall(nodes.comment)):
        comment.parent.remove(comment)
    for quote in list(doctree.findall(nodes.block_quote)):
        if not quote.children:
            quote.parent.remove(quote)


def _page_description(env: BuildEnvironment, docname: str) -> str:
    """Return the page's meta description or the opening sentences of its first paragraph."""
    doctree = env.get_doctree(docname)
    for meta in doctree.findall(nodes.meta):
        if meta.get("name") == "description" and meta.get("content"):
            return meta["content"]
    for paragraph in doctree.findall(nodes.paragraph):
        # ``example-header`` is the file path ``exampleinclude`` puts above a code excerpt.
        if "example-header" in paragraph["classes"] or any(
            isinstance(ancestor, _NOT_PROSE) for ancestor in _ancestors(paragraph)
        ):
            continue
        text = _inline_markdown(paragraph, env)
        if len(text) >= MIN_DESCRIPTION_LENGTH:
            return _summarize(text)
    return ""


def _summarize(text: str) -> str:
    """Return whole sentences of ``text`` up to the length cap, cutting the first one if it is longer."""
    description = ""
    for sentence in _SENTENCE_END.split(text):
        if description and len(description) + len(sentence) + 1 > MAX_DESCRIPTION_LENGTH:
            break
        description = f"{description} {sentence}".strip()
    if len(description) > MAX_DESCRIPTION_LENGTH:
        description = description[:MAX_DESCRIPTION_LENGTH].rsplit(" ", 1)[0] + " ..."
    # A paragraph that introduces a list or code block ends on a colon.
    return re.sub(r":$", ".", description)


def _ancestors(node: nodes.Node) -> Iterator[nodes.Element]:
    parent = node.parent
    while parent is not None:
        yield parent
        parent = parent.parent


def _page_title(env: BuildEnvironment, docname: str, explicit_title: str | None) -> str:
    if explicit_title:
        return explicit_title
    title_node = env.titles.get(docname)
    return _inline_markdown(title_node, env) if title_node else docname


def _toctree_docs(env: BuildEnvironment, docname: str, seen: set[str]) -> list[tuple[str, str | None]]:
    """Return ``docname`` and everything below it in toctree order, each once."""
    if docname in seen:
        return []
    seen.add(docname)
    docs: list[tuple[str, str | None]] = [(docname, None)]
    for child in env.toctree_includes.get(docname, []):
        docs.extend(_toctree_docs(env, child, seen))
    return docs


def _write_markdown_redirects(app: Sphinx) -> None:
    """Write Markdown stubs for redirected documentation pages."""
    outdir = Path(app.outdir)

    for from_path, to_path in iter_redirects(app):
        # Preserve Markdown belonging to an actual source document.
        docname = from_path.removesuffix(".html")
        if docname in app.env.found_docs:
            continue

        target = urlsplit(to_path)
        resolved_path = posixpath.normpath(posixpath.join(posixpath.dirname(from_path), target.path))

        # Only change links within this package to Markdown.
        # Other packages and external sites might only publish HTML.
        if (
            not target.scheme
            and not target.netloc
            and not target.path.startswith("/")
            and resolved_path != ".."
            and not resolved_path.startswith("../")
            and target.path.endswith(".html")
        ):
            target = target._replace(path=f"{target.path}.md")

        destination = urlunsplit(target)
        output = outdir / f"{from_path}.md"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            f"# Page moved\n\nThis page has moved to [the new page](<{destination}>).\n",
            encoding="utf-8",
        )


def _check_markdown_coverage(outdir: Path) -> None:
    """Fail if an eligible HTML page has no Markdown copy."""
    excluded = {"genindex.html", "search.html", "py-modindex.html"}
    missing = []

    for page in sorted(outdir.rglob("*.html")):
        relative = page.relative_to(outdir)
        if "_modules" in relative.parts or page.name in excluded:
            continue
        if not page.with_name(page.name + ".md").is_file():
            missing.append(relative.as_posix())

    if missing:
        raise ExtensionError(
            "HTML pages missing Markdown copies:\n" + "\n".join(f"  {page}" for page in missing)
        )


def _write_llms_txt(app: Sphinx, exception: Exception | None) -> None:
    """
    Replace sphinx-llm's flat ``llms.txt`` with one grouped by the root toctree captions.

    Runs after sphinx-llm has copied the Markdown pages into the HTML output (priority 101).
    """
    if exception is not None or app.builder.name != "html" or not app.config.llms_txt_enabled:
        return
    env = app.env
    outdir = Path(app.outdir)

    _write_markdown_redirects(app)
    _check_markdown_coverage(outdir)

    site_url = app.config.llms_txt_site_url.rstrip("/")
    optional = Matcher(app.config.llms_txt_optional_docs)
    skipped = Matcher(app.config.llms_txt_skip_docs)

    def md_url(docname: str) -> str:
        return f"{site_url}/{docname}.html.md"

    def entry(docname: str, title: str | None = None, *, describe: bool = True) -> str:
        line = f"- [{_escape_link_text(_page_title(env, docname, title))}]({md_url(docname)})"
        description = _page_description(env, docname) if describe else ""
        return f"{line}: {description}" if description else line

    root_doc = app.config.root_doc
    root = env.get_doctree(root_doc)
    # A caption's own pages come first; each page with pages under it gets a section of its own.
    # Each section is its caption and its (docname, llms.txt line) pairs.
    sections: list[tuple[str, list[tuple[str, str]]]] = []
    optional_entries: list[str] = []
    seen: set[str] = {root_doc}
    for toctree in root.findall(addnodes.toctree):
        leaves: list[tuple[str, str]] = []
        sections.append((toctree.get("caption") or "Pages", leaves))
        for title, ref in toctree["entries"]:
            if ref == "self":
                line = f"- [{_escape_link_text(title or 'Home')}]({md_url(root_doc)})"
                if app.config.llms_txt_root_description:
                    line += f": {app.config.llms_txt_root_description}"
                leaves.append((root_doc, line))
                continue
            if "://" in ref:
                continue
            docs = [
                doc
                for doc, _ in _toctree_docs(env, ref, seen)
                if not skipped(doc) and (outdir / f"{doc}.html.md").is_file()
            ]
            if optional(ref):
                if ref in docs:
                    optional_entries.append(entry(ref, title, describe=False))
                continue
            lines = [(doc, entry(doc, title if doc == ref else None)) for doc in docs if not optional(doc)]
            if len(lines) > 1:
                sections.append((_page_title(env, ref, title), lines))
            else:
                leaves.extend(lines)
    sections = [(caption, lines) for caption, lines in sections if lines]
    ordered_docs = [doc for _, lines in sections for doc, _ in lines]
    if root_doc not in ordered_docs:
        ordered_docs.insert(0, root_doc)

    title = f"{app.config.project} {app.config.release}"
    summary = app.config.llms_txt_description or _page_description(env, app.config.root_doc)
    pages = [
        f"---\n\nSource: {md_url(doc)}\n\n"
        + _absolute_links((outdir / f"{doc}.html.md").read_text(encoding="utf-8").strip(), md_url(doc))
        for doc in ordered_docs
    ]
    (outdir / "llms-full.txt").write_text(
        "\n\n".join([f"# {title}", f"> {summary}", *pages]) + "\n", encoding="utf-8"
    )
    optional_entries.append(
        f"- [llms-full.txt]({site_url}/llms-full.txt): The pages in the sections above, "
        "in the same order, in one file."
    )

    out = [f"# {title}", "", f"> {summary}", ""]
    if app.config.llms_txt_intro:
        out += [app.config.llms_txt_intro.strip(), ""]
    for caption, lines in sections:
        out += [f"## {caption}", "", *(line for _, line in lines), ""]
    if optional_entries:
        out += ["## Optional", "", *optional_entries, ""]
    (outdir / "llms.txt").write_text("\n".join(out), encoding="utf-8")
    logger.info("Wrote grouped llms.txt with %d sections", len(sections))


def _absolute_links(markdown: str, page_url: str) -> str:
    """Resolve the page's relative Markdown links against its URL, leaving code blocks alone."""
    lines = []
    in_code = False
    for line in markdown.splitlines():
        fence = line.lstrip().startswith("```")
        in_code ^= fence
        if in_code or fence:
            lines.append(line)
            continue
        # Odd-numbered parts are inline code spans, which stay as written.
        parts = _INLINE_CODE.split(line)
        for i in range(0, len(parts), 2):
            parts[i] = _MARKDOWN_LINK.sub(lambda m: f"{m[1]}{urljoin(page_url, m[2])}{m[3]}", parts[i])
        lines.append("".join(parts))
    return "\n".join(lines)


def setup(app: Sphinx) -> dict[str, Any]:
    app.setup_extension("sphinx_llm.txt")
    app.set_translator(MARKDOWN_BUILDER, AirflowMarkdownTranslator, override=True)
    app.add_config_value("llms_txt_site_url", "", "env")
    app.add_config_value("llms_txt_source_url", "", "env")
    app.add_config_value("llms_txt_release_source_url", "", "env")
    app.add_config_value("llms_txt_repo_root", "", "env")
    app.add_config_value("llms_txt_intro", "", "env")
    app.add_config_value("llms_txt_root_description", "", "env")
    app.add_config_value("llms_txt_optional_docs", [], "env")
    app.add_config_value("llms_txt_skip_docs", [], "env")
    app.connect("config-inited", _separate_markdown_doctrees)
    app.connect("doctree-read", _link_example_sources, priority=400)
    app.connect("doctree-resolved", _clean_markdown_doctree)
    app.connect("build-finished", _write_llms_txt, priority=102)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
