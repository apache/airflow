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

import random
import string
from pathlib import Path

import pytest

from airflow_breeze.prepare_providers.provider_documentation import (
    VERSION_MAJOR_INDEX,
    VERSION_MINOR_INDEX,
    VERSION_PATCHLEVEL_INDEX,
    Change,
    TypeOfChange,
    _convert_git_changes_to_table,
    _find_insertion_index_for_version,
    _get_change_from_line,
    _get_changes_classified,
    _get_git_log_command,
    classification_result,
    get_most_impactful_change,
    get_version_tag,
)

CHANGELOG_CONTENT = """
Changelog
---------

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

The ``offset`` parameter has been deprecated from ``list_jobs`` in favor of faster pagination with ``page_token`` similarly to `Databricks API <https://docs.databricks.com/api/workspace/jobs/list>`_.

* ``Remove offset-based pagination from 'list_jobs' function in 'DatabricksHook' (#34926)``

4.7.0
.....

Features
~~~~~~~~

* ``Add operator to create jobs in Databricks (#35156)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``D401 Support - Providers: DaskExecutor to Github (Inclusive) (#34935)``

4.6.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

"""


def test_find_insertion_index_append_to_found_changelog():
    index, append = _find_insertion_index_for_version(CHANGELOG_CONTENT.splitlines(), "5.0.0")
    assert append
    assert index == 13


def test_find_insertion_index_insert_new_changelog():
    index, append = _find_insertion_index_for_version(CHANGELOG_CONTENT.splitlines(), "5.0.1")
    assert not append
    assert index == 3


@pytest.mark.parametrize(
    ("version", "provider_id", "suffix", "tag"),
    [
        ("1.0.1", "asana", "", "providers-asana/1.0.1"),
        ("1.0.1", "asana", "rc1", "providers-asana/1.0.1rc1"),
        ("1.0.1", "apache.hdfs", "beta1", "providers-apache-hdfs/1.0.1beta1"),
    ],
)
def test_get_version_tag(version: str, provider_id: str, suffix: str, tag: str):
    assert get_version_tag(version, provider_id, suffix) == tag


@pytest.mark.parametrize(
    ("folder_paths", "from_commit", "to_commit", "git_command"),
    [
        (None, None, None, ["git", "log", "--pretty=format:%H %h %cd %s", "--date=short", "--", "."]),
        (
            None,
            "from_tag",
            None,
            ["git", "log", "--pretty=format:%H %h %cd %s", "--date=short", "from_tag", "--", "."],
        ),
        (
            None,
            "from_tag",
            "to_tag",
            ["git", "log", "--pretty=format:%H %h %cd %s", "--date=short", "from_tag...to_tag", "--", "."],
        ),
        (
            [Path("a"), Path("b")],
            "from_tag",
            "to_tag",
            [
                "git",
                "log",
                "--pretty=format:%H %h %cd %s",
                "--date=short",
                "from_tag...to_tag",
                "--",
                "a",
                "b",
            ],
        ),
    ],
)
def test_get_git_log_command(
    folder_paths: list[str] | None, from_commit: str | None, to_commit: str | None, git_command: list[str]
):
    assert _get_git_log_command(folder_paths, from_commit, to_commit) == git_command


def test_get_git_log_command_wrong():
    with pytest.raises(ValueError, match=r"to_commit without from_commit"):
        _get_git_log_command(None, None, "to_commit")


@pytest.mark.parametrize(
    ("line", "version", "change"),
    [
        (
            "LONG_HASH_123144 SHORT_HASH 2023-01-01 Description `with` no pr",
            "1.0.1",
            Change(
                full_hash="LONG_HASH_123144",
                short_hash="SHORT_HASH",
                date="2023-01-01",
                version="1.0.1",
                message="Description `with` no pr",
                message_without_backticks="Description 'with' no pr",
                pr=None,
            ),
        ),
        (
            "LONG_HASH_123144 SHORT_HASH 2023-01-01 Description `with` pr (#12345)",
            "1.0.1",
            Change(
                full_hash="LONG_HASH_123144",
                short_hash="SHORT_HASH",
                date="2023-01-01",
                version="1.0.1",
                message="Description `with` pr (#12345)",
                message_without_backticks="Description 'with' pr (#12345)",
                pr="12345",
            ),
        ),
    ],
)
def test_get_change_from_line(line: str, version: str, change: Change):
    assert _get_change_from_line(line, version) == change


@pytest.mark.parametrize(
    ("input", "output", "markdown", "changes_len"),
    [
        (
            """
LONG_HASH_123144 SHORT_HASH 2023-01-01 Description `with` no pr
LONG_HASH_123144 SHORT_HASH 2023-01-01 Description `with` pr (#12345)

LONG_HASH_123144 SHORT_HASH 2023-01-01 Description `with` pr (#12346)

""",
            """
1.0.1
.....

Latest change: 2023-01-01

=============================================  ===========  ==================================
Commit                                          Committed   Subject
=============================================  ===========  ==================================
`SHORT_HASH <https://url/LONG_HASH_123144>`__  2023-01-01   ``Description 'with' no pr``
`SHORT_HASH <https://url/LONG_HASH_123144>`__  2023-01-01   ``Description 'with' pr (#12345)``
`SHORT_HASH <https://url/LONG_HASH_123144>`__  2023-01-01   ``Description 'with' pr (#12346)``
=============================================  ===========  ==================================""",
            False,
            3,
        ),
        (
            """
LONG_HASH_123144 SHORT_HASH 2023-01-01 Description `with` no pr
LONG_HASH_123144 SHORT_HASH 2023-01-01 Description `with` pr (#12345)

LONG_HASH_123144 SHORT_HASH 2023-01-01 Description `with` pr (#12346)

""",
            """
| Commit                                     |  Committed  | Subject                          |
|:-------------------------------------------|:-----------:|:---------------------------------|
| [SHORT_HASH](https://url/LONG_HASH_123144) | 2023-01-01  | `Description 'with' no pr`       |
| [SHORT_HASH](https://url/LONG_HASH_123144) | 2023-01-01  | `Description 'with' pr (#12345)` |
| [SHORT_HASH](https://url/LONG_HASH_123144) | 2023-01-01  | `Description 'with' pr (#12346)` |
""",
            True,
            3,
        ),
    ],
)
def test_convert_git_changes_to_table(input: str, output: str, markdown: bool, changes_len):
    table, list_of_changes = _convert_git_changes_to_table(
        version="1.0.1", changes=input, base_url="https://url/", markdown=markdown
    )
    assert table.strip() == output.strip()
    assert len(list_of_changes) == changes_len
    assert list_of_changes[0].pr is None
    assert list_of_changes[1].pr == "12345"
    assert list_of_changes[2].pr == "12346"


def generate_random_string(length):
    c = string.hexdigits.lower()
    return "".join(random.choice(c) for _ in range(length))


def generate_long_hash():
    return generate_random_string(40)


def generate_short_hash():
    return generate_random_string(10)


@pytest.mark.parametrize(
    (
        "descriptions",
        "with_breaking_changes",
        "maybe_with_new_features",
        "breaking_count",
        "feature_count",
        "bugfix_count",
        "other_count",
        "misc_count",
        "type_of_change",
    ),
    [
        (["Added feature x"], True, True, 0, 1, 0, 0, 0, [TypeOfChange.FEATURE]),
        (["Added feature x"], False, True, 0, 1, 0, 0, 0, [TypeOfChange.FEATURE]),
        (["Breaking change in"], True, True, 1, 0, 0, 0, 0, [TypeOfChange.BREAKING_CHANGE]),
        (["Misc change in"], False, False, 0, 0, 0, 0, 1, [TypeOfChange.MISC]),
        (
            ["Fix change in", "Breaking feature y"],
            True,
            False,
            1,
            0,
            1,
            0,
            0,
            [TypeOfChange.BUGFIX, TypeOfChange.BREAKING_CHANGE],
        ),
    ],
)
def test_classify_changes_automatically(
    descriptions: list[str],
    with_breaking_changes: bool,
    maybe_with_new_features: bool,
    breaking_count: int,
    feature_count: int,
    bugfix_count: int,
    other_count: int,
    misc_count: int,
    type_of_change: TypeOfChange,
):
    from airflow_breeze.prepare_providers.provider_documentation import SHORT_HASH_TO_TYPE_DICT

    """Test simple automated classification of the changes based on their single-line description."""
    changes = [
        _get_change_from_line(
            f"{generate_long_hash()} {generate_short_hash()} 2023-12-01 {description}", version="0.1.0"
        )
        for description in descriptions
    ]

    for i in range(0, len(changes)):
        SHORT_HASH_TO_TYPE_DICT[changes[i].short_hash] = type_of_change[i]

    classified_changes = _get_changes_classified(
        changes, with_breaking_changes=with_breaking_changes, maybe_with_new_features=maybe_with_new_features
    )
    assert len(classified_changes.breaking_changes) == breaking_count
    assert len(classified_changes.features) == feature_count
    assert len(classified_changes.fixes) == bugfix_count
    assert len(classified_changes.other) == other_count
    assert len(classified_changes.other) == other_count
    assert len(classified_changes.misc) == misc_count


@pytest.mark.parametrize(
    ("initial_version", "bump_index", "expected_version"),
    [
        ("4.2.1", VERSION_MAJOR_INDEX, "5.0.0"),
        ("3.5.9", VERSION_MINOR_INDEX, "3.6.0"),
        ("2.0.0", VERSION_PATCHLEVEL_INDEX, "2.0.1"),
    ],
)
def test_version_bump_for_provider_documentation(initial_version, bump_index, expected_version):
    from airflow_breeze.prepare_providers.provider_documentation import Version, bump_version

    result = bump_version(Version(initial_version), bump_index)
    assert str(result) == expected_version


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        pytest.param([TypeOfChange.SKIP], TypeOfChange.SKIP, id="only-skip"),
        pytest.param([TypeOfChange.DOCUMENTATION], TypeOfChange.DOCUMENTATION, id="only-doc"),
        pytest.param([TypeOfChange.MISC], TypeOfChange.MISC, id="only-misc"),
        pytest.param([TypeOfChange.BUGFIX], TypeOfChange.BUGFIX, id="only-bugfix"),
        pytest.param(
            [TypeOfChange.MIN_AIRFLOW_VERSION_BUMP],
            TypeOfChange.MIN_AIRFLOW_VERSION_BUMP,
            id="only-min-airflow-bump",
        ),
        pytest.param([TypeOfChange.FEATURE], TypeOfChange.FEATURE, id="only-feature"),
        pytest.param([TypeOfChange.BREAKING_CHANGE], TypeOfChange.BREAKING_CHANGE, id="only-breaking"),
        pytest.param(
            [TypeOfChange.SKIP, TypeOfChange.DOCUMENTATION], TypeOfChange.DOCUMENTATION, id="doc-vs-skip"
        ),
        pytest.param([TypeOfChange.SKIP, TypeOfChange.MISC], TypeOfChange.MISC, id="misc-vs-skip"),
        pytest.param([TypeOfChange.DOCUMENTATION, TypeOfChange.MISC], TypeOfChange.MISC, id="misc-vs-doc"),
        pytest.param([TypeOfChange.MISC, TypeOfChange.BUGFIX], TypeOfChange.BUGFIX, id="bugfix-vs-misc"),
        pytest.param(
            [TypeOfChange.BUGFIX, TypeOfChange.MIN_AIRFLOW_VERSION_BUMP],
            TypeOfChange.MIN_AIRFLOW_VERSION_BUMP,
            id="bump-vs-bugfix",
        ),
        pytest.param(
            [TypeOfChange.MIN_AIRFLOW_VERSION_BUMP, TypeOfChange.FEATURE],
            TypeOfChange.FEATURE,
            id="feature-vs-bump",
        ),
        pytest.param(
            [TypeOfChange.FEATURE, TypeOfChange.BREAKING_CHANGE],
            TypeOfChange.BREAKING_CHANGE,
            id="breaking-vs-feature",
        ),
        # Bigger combos
        pytest.param(
            [
                TypeOfChange.SKIP,
                TypeOfChange.DOCUMENTATION,
                TypeOfChange.MISC,
                TypeOfChange.BUGFIX,
                TypeOfChange.MIN_AIRFLOW_VERSION_BUMP,
                TypeOfChange.FEATURE,
                TypeOfChange.BREAKING_CHANGE,
            ],
            TypeOfChange.BREAKING_CHANGE,
            id="full-spectrum",
        ),
        pytest.param(
            [
                TypeOfChange.DOCUMENTATION,
                TypeOfChange.BUGFIX,
                TypeOfChange.MIN_AIRFLOW_VERSION_BUMP,
            ],
            TypeOfChange.MIN_AIRFLOW_VERSION_BUMP,
            id="version-bump-over-bugfix-doc",
        ),
        pytest.param(
            [
                TypeOfChange.DOCUMENTATION,
                TypeOfChange.MISC,
                TypeOfChange.SKIP,
            ],
            TypeOfChange.MISC,
            id="misc-over-doc-skip",
        ),
    ],
)
def test_get_most_impactful_change(changes, expected):
    assert get_most_impactful_change(changes) == expected


@pytest.mark.parametrize(
    ("provider_id", "changed_files", "expected"),
    [
        pytest.param("slack", ["providers/slack/docs/slack.rst"], "documentation", id="only_docs"),
        pytest.param(
            "apache.flink",
            ["providers/apache/flink/docs/slack.rst"],
            "documentation",
            id="only_docs_longer_path",
        ),
        pytest.param(
            "slack", ["providers/slack/tests/test_slack.py"], "test_or_example_only", id="only_tests"
        ),
        pytest.param(
            "apache.flink",
            ["providers/apache/flink/tests/unit/apache/flink/sensors/test_flink_kubernetes.py"],
            "test_or_example_only",
            id="only_tests_longer_path",
        ),
        pytest.param(
            "slack",
            ["providers/slack/src/airflow/providers/slack/example_dags/example_notify.py"],
            "test_or_example_only",
            id="only_example_dags",
        ),
        pytest.param(
            "slack",
            [
                "providers/slack/tests/test_slack.py",
                "providers/slack/src/airflow/providers/slack/example_dags/example_notify.py",
            ],
            "test_or_example_only",
            id="tests_and_example_dags",
        ),
        pytest.param(
            "slack",
            [
                "providers/slack/tests/test_slack.py",
                "providers/slack/docs/slack.rst",
            ],
            "documentation",
            id="docs_and_tests",
        ),
        pytest.param(
            "slack",
            [
                "providers/slack/src/airflow/providers/slack/hooks/slack.py",
                "providers/slack/tests/test_slack.py",
            ],
            "other",
            id="real_code_and_tests",
        ),
        pytest.param(
            "slack",
            [
                "providers/slack/src/airflow/providers/slack/hooks/slack.py",
                "providers/slack/tests/test_slack.py",
                "providers/slack/docs/slack.rst",
            ],
            "other",
            id="docs_and_real_code",
        ),
        pytest.param(
            "google",
            [
                "providers/google/tests/some_test.py",
                "providers/amazon/tests/test_something.py",
            ],
            "test_or_example_only",
            id="tests_in_multiple_providers",
        ),
        pytest.param(
            "amazon",
            [
                "providers/google/tests/some_test.py",
                "providers/amazon/tests/test_something.py",
            ],
            "test_or_example_only",
            id="tests_in_multiple_providers",
        ),
        pytest.param("slack", ["airflow/utils/db.py"], "other", id="non_provider_file"),
        pytest.param("slack", [], "other", id="empty_commit"),
    ],
)
def test_classify_provider_pr_files_logic(provider_id, changed_files, expected):
    result = classification_result(provider_id, changed_files)
    assert result == expected
