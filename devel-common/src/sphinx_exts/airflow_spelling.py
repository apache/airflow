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
Spell check every document while the html builder writes it.

``sphinxcontrib.spelling`` ships a separate ``spelling`` builder, which means building every package
twice. This extension runs the same checks - its tokenizer, filters, dictionary, word lists and the
``spelling`` domain's directive and roles - on each resolved doctree of the html build instead, and
writes what it finds to the JSON file named by the ``airflow_spelling_output`` config value. Leaving
that value empty disables the check.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import enchant
from docutils import nodes
from docutils.utils import get_source_line
from docutils.utils.smartquotes import smartchars
from enchant.tokenize import EmailFilter, WikiWordFilter, get_tokenizer
from sphinx.util.matching import Matcher
from sphinxcontrib.spelling import filters
from sphinxcontrib.spelling.builder import SpellingBuilder

if TYPE_CHECKING:
    from docutils.parsers.rst.states import Inliner
    from sphinx.application import Sphinx
    from sphinx.config import Config
    from sphinx.environment import BuildEnvironment

# Enchant's tokenizer starts by splitting on whitespace, so a whitespace-delimited chunk is checked the
# same way on its own as within its paragraph.
_CHUNK = re.compile(r"\S+")


# SmartQuotes swaps typed quotes, dashes and "..." for characters the tokenizer and filters do not understand
# ("didn’t" splits into "didn" and "t", "isinstance…" escapes PythonBuiltinsFilter), so text is checked as typed.
# The docs are English, whose quotes are single characters; some languages' are not and str.maketrans would refuse.
def _build_unsmarten_table() -> dict[int, str]:
    double_open, double_close, single_open, single_close = tuple(smartchars.quotes["en"])
    return str.maketrans(
        {
            double_open: '"',
            double_close: '"',
            single_open: "'",
            single_close: "'",
            smartchars.apostrophe: "'",
            smartchars.endash: "--",
            smartchars.emdash: "---",
            smartchars.ellipsis: "...",
        }
    )


_UNSMARTEN = _build_unsmarten_table()
_RUN_ATTRIBUTE = "_airflow_spelling_run"


class SpellChecker:
    """Finds misspelled words the way ``sphinxcontrib.spelling``'s builder does, with caching."""

    def __init__(self, config: Config, srcdir: str | os.PathLike) -> None:
        cheap_filters: list[Any] = [filters.ContractionFilter, EmailFilter]
        if config.spelling_ignore_wiki_words:
            cheap_filters.append(WikiWordFilter)
        if config.spelling_ignore_acronyms:
            cheap_filters.append(filters.AcronymFilter)
        if config.spelling_ignore_python_builtins:
            cheap_filters.append(filters.PythonBuiltinsFilter)
        cheap_filters.extend(config.spelling_filters)
        # Both only ever drop tokens and both are slow (find_spec over sys.path, git log), so they are
        # applied only to chunks that are still misspelled after the cheap filters.
        slow_filters: list[Any] = []
        if config.spelling_ignore_importable_modules:
            slow_filters.append(filters.ImportableModuleFilter)
        if config.spelling_ignore_contributor_names:
            slow_filters.append(filters.ContributorFilter)

        word_lists = config.spelling_word_list_filename or ["spelling_wordlist.txt"]
        if isinstance(word_lists, str):
            word_lists = word_lists.split(",")
        word_lists = [os.path.join(srcdir, word_list) for word_list in word_lists]

        self._tokenizer_lang = config.tokenizer_lang
        self._all_filters = [*cheap_filters, *slow_filters]
        self._slow_filters = slow_filters
        self._slow_tokenizer: Any = None
        self._tokenizer: Any = get_tokenizer(config.tokenizer_lang, filters=cheap_filters)
        self._dictionary = enchant.DictWithPWL(config.spelling_lang, _combine_word_lists(word_lists))
        self._cache: dict[str, tuple[tuple[str, int], ...]] = {}

    def find_misspellings(self, text: str) -> Iterator[tuple[str, int]]:
        """Yield each misspelled word in ``text`` with its offset."""
        for match in _CHUNK.finditer(text):
            for word, offset in self._check_chunk(match[0]):
                yield word, match.start() + offset

    def _check_chunk(self, chunk: str) -> tuple[tuple[str, int], ...]:
        result = self._cache.get(chunk)
        if result is not None:
            return result
        check = self._dictionary.check
        # Filters only ever drop tokens, so a single word the dictionary knows needs no tokenizing.
        if chunk.isalpha() and chunk.isascii() and check(chunk):
            result = ()
        else:
            result = tuple((word, pos) for word, pos in self._tokenizer(chunk) if not check(word))
            if result and self._slow_filters:
                if self._slow_tokenizer is None:
                    self._slow_tokenizer = get_tokenizer(self._tokenizer_lang, filters=self._all_filters)
                result = tuple((word, pos) for word, pos in self._slow_tokenizer(chunk) if not check(word))
        self._cache[chunk] = result
        return result


def _combine_word_lists(word_lists: list[str]) -> str:
    if len(word_lists) == 1:
        return word_lists[0]
    fd, combined = tempfile.mkstemp(prefix="spelling_wordlist_", suffix=".txt")
    with os.fdopen(fd, "w", encoding="utf-8") as out:
        for word_list in word_lists:
            out.write(Path(word_list).read_text(encoding="utf-8").rstrip("\n") + "\n")
    return combined


class _SpellingRun:
    def __init__(self, app: Sphinx) -> None:
        self.checker = SpellChecker(app.config, app.srcdir)
        self.excluded = Matcher(app.config.spelling_exclude_patterns)
        self.output = Path(app.config.airflow_spelling_output)
        self.findings: list[dict[str, Any]] = []

    def check_document(self, env: BuildEnvironment, docname: str, doctree: nodes.document) -> None:
        if self.excluded(str(env.doc2path(docname, False))):
            return
        good_words = set(getattr(env, "spelling_document_words", {}).get(docname, ()))
        for node in doctree.findall(nodes.Text):
            parent = node.parent
            if parent is not None and parent.tagname not in SpellingBuilder.TEXT_NODES:
                continue
            text = node.astext().translate(_UNSMARTEN)
            for word, offset in self.checker.find_misspellings(text):
                if word not in good_words:
                    self.findings.append(_describe(node, text, word, offset))


def _describe(node: nodes.Text, text: str, word: str, offset: int) -> dict[str, Any]:
    line_start = text.rfind("\n", 0, offset) + 1
    line_end = text.find("\n", offset)
    context = text[line_start : len(text) if line_end == -1 else line_end].strip()
    source, node_line = get_source_line(node)
    line = None if node_line is None else node_line + text.count("\n", 0, offset)
    return {"file": source, "line": line, "word": word, "context": context}


def _start(app: Sphinx) -> None:
    if app.config.airflow_spelling_output:
        setattr(app, _RUN_ATTRIBUTE, _SpellingRun(app))


def _check(app: Sphinx, doctree: nodes.document, docname: str) -> None:
    run: _SpellingRun | None = getattr(app, _RUN_ATTRIBUTE, None)
    if run is not None:
        run.check_document(app.env, docname, doctree)


def _write_findings(app: Sphinx, exception: Exception | None) -> None:
    run: _SpellingRun | None = getattr(app, _RUN_ATTRIBUTE, None)
    if run is None or exception is not None:
        return
    run.output.parent.mkdir(parents=True, exist_ok=True)
    run.output.write_text(json.dumps(run.findings, indent=2), encoding="utf-8")


def spelling_ignore_role(
    name: str,
    rawtext: str,
    text: str,
    lineno: int,
    inliner: Inliner,
    options: dict[str, Any] | None = None,
    content: Sequence[str] = (),
) -> tuple[list[nodes.Node], list[nodes.system_message]]:
    # sphinxcontrib.spelling flags the Text node itself, but SmartQuotes replaces Text nodes during an
    # html build, losing the flag; text inside an inline node is never checked.
    return [nodes.inline(rawtext, text, classes=["spelling-ignore"])], []


def setup(app: Sphinx) -> dict[str, Any]:
    app.setup_extension("sphinxcontrib.spelling")
    app.add_role_to_domain("spelling", "ignore", spelling_ignore_role, override=True)
    app.add_config_value("airflow_spelling_output", "", "", types=frozenset({str}))
    app.connect("builder-inited", _start)
    app.connect("doctree-resolved", _check)
    app.connect("build-finished", _write_findings)
    return {"version": "builtin", "parallel_read_safe": True, "parallel_write_safe": True}
