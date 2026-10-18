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
"""Map a provider's classes to the how-to guide sections that document them.

A module's ``docs_url`` points at generated API reference, which tells a reader
what the arguments are but not how the thing is meant to be used. The prose
guides carry that, and they already mark it: a how-to guide documents one class
(or a class and its task-flow decorator) per section, titled with the name(s)
either at the start (``HookToolset``, ``SQLToolset``, ``AgentOperator`` &
``@task.agent``) or after a colon at the very end, as in a section titled
"Airflow hooks as tools: ``HookToolset``".

So the mapping is read back out of the guides rather than curated anywhere: a
hand-maintained name-to-guide table would rot silently every time a guide is
split, renamed, or a class is dropped, and a rotten link is worse than none.
Callers supply the reST they can see (a git tag, or the working tree) and get
back only the anchors those sources actually contain.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any

# reST underlines an (optionally overlined) section title with a run of one
# punctuation character, at least as long as the title itself.
_ADORNMENT_CHARS = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"

_SKIPPED_PAGE_NAMES = frozenset({"changelog.rst", "commits.rst"})


def is_guide_page(relative_path: str) -> bool:
    """Whether a path relative to a provider's docs directory is a how-to guide page.

    Callers hand every ``.rst`` they can see to this before it ever reaches
    ``collect_guide_anchors``. Two kinds of real, built pages must not go
    further:

    - Anything under a ``_``-prefixed path segment, at any depth
      (``_api/hook/index.rst``, ``operators/_partials/foo.rst``, top-level
      ``_partials/foo.rst``): Sphinx/autoapi output and partials are directive
      markup, not the hand-written, reST-underlined titles this module's
      inline-literal title convention parses.
    - ``changelog.rst`` and ``commits.rst``: real release-note pages, not
      how-to guides, that can carry inline-literal-formatted headings by
      coincidence.
    """
    path = PurePosixPath(relative_path)
    if any(part.startswith("_") for part in path.parts):
        return False
    return path.name not in _SKIPPED_PAGE_NAMES


# A single inline-literal name: a class (``HookToolset``) or a task-flow
# decorator (``@task.llm_file_analysis``) -- narrow enough that it still can't
# match arbitrary prose wrapped in backticks.
_INLINE_LITERAL_NAME = r"``(@?[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)``"
_INLINE_LITERAL_NAME_RE = re.compile(_INLINE_LITERAL_NAME)

# Only titles that consist solely of, or end a colon-led clause with, a run of
# inline-literal names are treated as documenting them, so prose headings
# ("Bounded query results") never produce a link. A run is one or more names
# joined by "&", ",", "/" or "and" -- how guides write a section that covers both
# an operator and its decorator (``AgentOperator`` & ``@task.agent``).
_NAME_SEPARATOR = r"(?:\s*[&,/]\s*|\s+and\s+)"
_NAME_RUN = rf"{_INLINE_LITERAL_NAME}(?:{_NAME_SEPARATOR}{_INLINE_LITERAL_NAME})*"

# Shape one (what older release tags' docs use):
# the title is nothing but the name run. Anchoring to "$" keeps a title that
# merely opens with a literal and continues in prose ("``SandboxToolset``
# parameters") from claiming to document that class.
_LEADING_LITERAL_NAME_RUN = re.compile(rf"^{_NAME_RUN}\s*$")
# Shape two (what current docs use): a prose lead-in, a colon, then the name run runs to
# the very end of the title. Anchoring to "$" is what keeps a colon earlier in
# the title, with prose after it, from being mistaken for this shape.
_TRAILING_LITERAL_NAME_RUN = re.compile(rf":\s+{_NAME_RUN}\s*$")


def slugify_section_anchor(title: str) -> str:
    """Return the HTML id Sphinx gives a section with this title.

    Mirrors docutils' ``make_id``: lower-case, every run of non-alphanumeric
    characters becomes a single hyphen, and leading/trailing hyphens are
    dropped -- e.g. the section titled ``HookToolset`` is served at
    ``#hooktoolset``.
    """
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


def _extract_names_from_title(title: str) -> list[str]:
    """Return the names a section title documents, or [] if it names prose.

    A guide marks a section as being *about* one or more names by titling it
    with them as inline literals, either as the whole title (``HookToolset``, or
    ``AgentOperator`` & ``@task.agent`` where one section covers the operator
    and its decorator) or after a colon at the very end ("Airflow hooks as
    tools: ``HookToolset``"). Requiring that markup is what keeps a single-word
    prose heading ("Guidelines") -- or a literal appearing elsewhere in a prose
    title -- from claiming to document a class of the same name, and it is a
    convention the guides already follow rather than one imposed on them.
    """
    match = _LEADING_LITERAL_NAME_RUN.match(title) or _TRAILING_LITERAL_NAME_RUN.search(title)
    return _INLINE_LITERAL_NAME_RE.findall(match.group(0)) if match else []


def _is_adornment(line: str) -> bool:
    """Whether a line is a reST title overline/underline rather than a title."""
    return bool(line) and len(set(line)) == 1 and line[0] in _ADORNMENT_CHARS


def _extract_section_titles(text: str) -> list[str]:
    """Return every section title in a reST document, in document order."""
    titles = []
    lines = text.splitlines()
    for index, line in enumerate(lines[:-1]):
        title = line.strip()
        # Guides title these sections with an inline literal (``HookToolset``),
        # so a title can legitimately start with an adornment character; only a
        # line that is *entirely* one repeated character is an adornment.
        if not title or _is_adornment(title):
            continue
        underline = lines[index + 1].strip()
        if len(underline) >= len(title) and _is_adornment(underline):
            titles.append(title)
    return titles


def collect_guide_anchors(docs: Mapping[str, str]) -> dict[str, str]:
    """Map name -> ``<page>.html#<anchor>`` for every documented class or decorator.

    ``docs`` maps a page path relative to the provider's docs directory (e.g.
    ``toolsets.rst``) to its reST source. A title can name more than one name
    (``AgentOperator`` & ``@task.agent``), in which case every one gets the
    same anchor. When two pages document the same name: a page's own title
    (its first section) beats a subsection found on any other page, since that
    page is the one dedicated to the class; among two page titles -- or two
    subsections neither page titles -- the first page in sorted order wins, so
    a rebuild of the same sources always produces the same link. "Page title"
    is simply the first title _extract_section_titles finds, not a checked
    top-level adornment, so a heading-shaped block earlier on the page (say,
    inside a directive) would take that role.
    """
    found: dict[str, tuple[bool, str]] = {}  # name -> (from a page title?, anchor)
    for page in sorted(docs):
        page_url = re.sub(r"\.rst$", ".html", page)
        for index, title in enumerate(_extract_section_titles(docs[page])):
            is_page_title = index == 0
            for name in _extract_names_from_title(title):
                current = found.get(name)
                # A page title replaces a subsection found earlier; nothing else
                # replaces what was found first, so rebuilds stay deterministic.
                if current is not None and (current[0] or not is_page_title):
                    continue
                found[name] = (is_page_title, f"{page_url}#{slugify_section_anchor(title)}")
    return {name: anchor for name, (_, anchor) in found.items()}


def attach_guide_urls(modules: list[dict[str, Any]], anchors: Mapping[str, str], base_docs_url: str) -> int:
    """Set ``guide_url`` on every module a guide section documents.

    Mutates ``modules`` in place; returns how many got a link.
    """
    attached = 0
    for module in modules:
        anchor = anchors.get(module["name"])
        if not anchor:
            continue
        module["guide_url"] = f"{base_docs_url.rstrip('/')}/{anchor}"
        attached += 1
    return attached
