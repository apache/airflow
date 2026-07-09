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
"""Unit tests for ``scripts/ci/prek/check_common_ai_examples_referenced.py``."""

from __future__ import annotations

import check_common_ai_examples_referenced as checker
import pytest


@pytest.fixture
def docs_tree(tmp_path, monkeypatch):
    """Build a fake docs/example_dags tree and point the module's path constants at it."""
    example_dags_path = tmp_path / "example_dags"
    example_dags_path.mkdir()
    docs_path = tmp_path / "docs"
    docs_path.mkdir()
    examples_rst_path = docs_path / "examples.rst"

    for name in ("example_direct.py", "example_via_doc.py", "example_via_ref.py", "example_missing.py"):
        (example_dags_path / name).write_text("# placeholder\n")

    examples_rst_path.write_text(
        "Examples\n"
        "========\n\n"
        "Source: `example_direct.py <https://example.invalid/example_direct.py>`__.\n\n"
        "See :doc:`guide_a` and :ref:`Some label <some-label>`.\n"
    )
    (docs_path / "guide_a.rst").write_text(
        "Guide A\n=======\n\n.. exampleinclude:: /example_dags/example_via_doc.py\n    :language: python\n"
    )
    (docs_path / "guide_b.rst").write_text(
        "Guide B\n"
        "=======\n\n"
        ".. _some-label:\n\n"
        ".. exampleinclude:: /example_dags/example_via_ref.py\n"
        "    :language: python\n"
    )

    monkeypatch.setattr(checker, "DOCS_PATH", docs_path)
    monkeypatch.setattr(checker, "EXAMPLES_RST_PATH", examples_rst_path)
    monkeypatch.setattr(checker, "EXAMPLE_DAGS_PATH", example_dags_path)
    return tmp_path


class TestGetExampleDagFilenames:
    def test_lists_example_files_only(self, docs_tree):
        assert checker.get_example_dag_filenames() == {
            "example_direct.py",
            "example_via_doc.py",
            "example_via_ref.py",
            "example_missing.py",
        }


class TestResolveDocTarget:
    def test_relative_target_resolves_next_to_referring_file(self, docs_tree):
        referring_file = docs_tree / "docs" / "examples.rst"
        resolved = checker.resolve_doc_target("guide_a", referring_file)
        assert resolved == docs_tree / "docs" / "guide_a.rst"

    def test_absolute_target_resolves_from_docs_root(self, docs_tree):
        referring_file = docs_tree / "docs" / "examples.rst"
        resolved = checker.resolve_doc_target("/guide_a", referring_file)
        assert resolved == docs_tree / "docs" / "guide_a.rst"

    def test_missing_target_returns_none(self, docs_tree):
        referring_file = docs_tree / "docs" / "examples.rst"
        assert checker.resolve_doc_target("does_not_exist", referring_file) is None


class TestResolveRefTarget:
    def test_finds_file_defining_label(self, docs_tree):
        assert checker.resolve_ref_target("some-label") == docs_tree / "docs" / "guide_b.rst"

    def test_missing_label_returns_none(self, docs_tree):
        assert checker.resolve_ref_target("no-such-label") is None


class TestFindReferencedExampleFilenames:
    def test_follows_doc_and_ref_links_from_examples_rst(self, docs_tree):
        assert checker.find_referenced_example_filenames() == {
            "example_direct.py",
            "example_via_doc.py",
            "example_via_ref.py",
        }


class TestMain:
    def test_fails_when_an_example_is_unreferenced(self, docs_tree):
        assert checker.main() == 1

    def test_passes_when_every_example_is_reachable(self, docs_tree):
        (docs_tree / "example_dags" / "example_missing.py").unlink()
        assert checker.main() == 0
