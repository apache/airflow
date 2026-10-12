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

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import enchant
import pytest
from docutils import nodes
from docutils.utils import new_document
from docutils.utils.smartquotes import smartchars, smartyPants
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment
from sphinx.errors import SphinxError
from sphinxcontrib.spelling import filters

SPHINX_EXTS_PATH = Path(__file__).parents[3] / "src" / "sphinx_exts"
if SPHINX_EXTS_PATH.as_posix() not in sys.path:
    # The extensions are loaded by Sphinx from this directory and import each other by bare name.
    sys.path.append(SPHINX_EXTS_PATH.as_posix())

from sphinx_exts import airflow_spelling  # noqa: E402

KNOWN_WORDS = {"the", "word", "is", "spelled", "do", "Dag", "text", "quoted", "a"}


def _config(**overrides):
    values = {
        "spelling_ignore_wiki_words": True,
        "spelling_ignore_acronyms": True,
        "spelling_ignore_python_builtins": True,
        "spelling_ignore_importable_modules": True,
        "spelling_ignore_contributor_names": False,
        "spelling_filters": [],
        "spelling_word_list_filename": ["wordlist.txt"],
        "spelling_lang": "en_US",
        "tokenizer_lang": "en_US",
        "spelling_exclude_patterns": [],
        "airflow_spelling_output": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.fixture(autouse=True)
def dictionary_factory(monkeypatch):
    pwl = mock.create_autospec(enchant.DictWithPWL, instance=True)
    pwl.check.side_effect = KNOWN_WORDS.__contains__
    factory = mock.create_autospec(enchant.DictWithPWL, return_value=pwl)
    monkeypatch.setattr(airflow_spelling.enchant, "DictWithPWL", factory)
    return factory


@pytest.fixture
def dictionary(dictionary_factory):
    return dictionary_factory.return_value


@pytest.fixture
def checker(tmp_path):
    return airflow_spelling.SpellChecker(_config(), tmp_path)


class TestSpellChecker:
    def test_reports_unknown_words_with_their_offset_in_the_text(self, checker):
        text = "the zorp is\nspelled (badly) Dag's"

        assert list(checker.find_misspellings(text)) == [("zorp", 4), ("badly", 21)]

    def test_skips_what_the_spelling_builder_filters_skip(self, checker):
        text = "API URLs WikiWord len someone@example.com sys don't"

        assert list(checker.find_misspellings(text)) == []

    def test_each_distinct_chunk_is_checked_once(self, checker, dictionary):
        list(checker.find_misspellings("zorp"))
        checks_for_first_sight = dictionary.check.call_count

        assert list(checker.find_misspellings("zorp zorp")) == [("zorp", 0), ("zorp", 5)]
        assert dictionary.check.call_count == checks_for_first_sight

    @mock.patch.object(filters.ImportableModuleFilter, "_skip", autospec=True, return_value=False)
    def test_importable_module_lookup_only_runs_for_words_still_misspelled(self, mock_skip, checker):
        list(checker.find_misspellings("(the) zorp"))

        assert [c.args[1] for c in mock_skip.call_args_list] == ["zorp"]

    def test_combines_several_word_lists(self, tmp_path, dictionary_factory):
        (tmp_path / "a.txt").write_text("alpha\n")
        (tmp_path / "b.txt").write_text("beta")

        airflow_spelling.SpellChecker(_config(spelling_word_list_filename=["a.txt", "b.txt"]), tmp_path)

        combined = dictionary_factory.call_args.args[1]
        assert Path(combined).read_text() == "alpha\nbeta\n"


def test_undoes_every_character_smartquotes_produces():
    produced = {
        value
        for name, value in vars(smartchars).items()
        if isinstance(value, str) and not name.startswith("_")
    }
    produced.update(smartchars.quotes["en"])

    assert {character for character in produced if ord(character) not in airflow_spelling._UNSMARTEN} == set()


@pytest.mark.parametrize(
    "typed",
    ["the \"double\" and 'single' quotes", "don't, the '80s", "an -- en and --- em dash", "and so on..."],
)
def test_undoing_smartquotes_gives_back_the_text_as_typed(typed):
    assert smartyPants(typed, "qDe", "en").translate(airflow_spelling._UNSMARTEN) == typed


def _env(docname_path: str = "index.rst", good_words=None):
    env = mock.create_autospec(BuildEnvironment, instance=True)
    env.doc2path.return_value = Path(docname_path)
    env.spelling_document_words = {"index": good_words or []}
    return env


def _paragraph(*children: nodes.Node, source: str, line: int) -> nodes.document:
    document = new_document(source)
    paragraph = nodes.paragraph("", "", *children)
    paragraph.source = source
    paragraph.line = line
    document += paragraph
    return document


class TestCheckDocument:
    def _run(self, tmp_path, **config):
        app = mock.create_autospec(Sphinx, instance=True)
        app.config = _config(airflow_spelling_output=(tmp_path / "out.json").as_posix(), **config)
        app.srcdir = tmp_path
        return airflow_spelling._SpellingRun(app)

    def test_checks_smart_quoted_paragraph_text_but_not_literals_ignored_text_or_document_words(
        self, tmp_path
    ):
        run = self._run(tmp_path)
        doctree = _paragraph(
            nodes.Text("the zorp\ndon’t “quoted” ‘WikiWord’ “ZorpQuux” isinstance… mispeled localword "),
            nodes.literal("", "notchecked"),
            *airflow_spelling.spelling_ignore_role("", "", "ignoredword", 1, mock.Mock())[0],
            source="/docs/index.rst",
            line=3,
        )

        run.check_document(_env(good_words=["localword"]), "index", doctree)

        assert run.findings == [
            {"file": "/docs/index.rst", "line": 3, "word": "zorp", "context": "the zorp"},
            {
                "file": "/docs/index.rst",
                "line": 4,
                "word": "mispeled",
                "context": """don't "quoted" 'WikiWord' "ZorpQuux" isinstance... mispeled localword""",
            },
        ]

    def test_skips_excluded_documents(self, tmp_path):
        run = self._run(tmp_path, spelling_exclude_patterns=["changelog.rst"])
        doctree = _paragraph(nodes.Text("zorp"), source="/docs/changelog.rst", line=1)

        run.check_document(_env("changelog.rst"), "changelog", doctree)

        assert run.findings == []


class TestWriteFindings:
    def _app(self, tmp_path, findings):
        app = mock.create_autospec(Sphinx, instance=True)
        run = mock.create_autospec(airflow_spelling._SpellingRun, instance=True)
        run.output = tmp_path / "nested" / "out.json"
        run.findings = findings
        setattr(app, airflow_spelling._RUN_ATTRIBUTE, run)
        return app, run.output

    def test_writes_findings_as_json(self, tmp_path):
        findings = [{"file": "/docs/index.rst", "line": 1, "word": "zorp", "context": "zorp"}]
        app, output = self._app(tmp_path, findings)

        airflow_spelling._write_findings(app, None)

        assert json.loads(output.read_text()) == findings

    def test_writes_nothing_when_the_build_failed(self, tmp_path):
        app, output = self._app(tmp_path, [])

        airflow_spelling._write_findings(app, SphinxError("boom"))

        assert not output.exists()
