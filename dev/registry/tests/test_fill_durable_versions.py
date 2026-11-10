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
import subprocess
from unittest import mock

import pytest
from fill_durable_versions import (
    DURABLE_TOKENS,
    GitCommandError,
    build_source_path,
    durable_group,
    find_first_releases,
    get_released_tags,
    main,
    run_git,
)

POD_PATH = "providers/cncf/kubernetes/src/airflow/providers/cncf/kubernetes/operators/pod.py"
POD_CLASS = "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator"


@pytest.fixture(autouse=True)
def clear_tag_cache():
    get_released_tags.cache_clear()
    yield
    get_released_tags.cache_clear()


def fake_git(tags=(), matches=(), token=None, paths=(), allow_grep=True):
    """Stand in for run_git. The grep argv is checked, since without `-lE` git prints
    `tag:path:line` and nothing gets dated."""

    def run(*args, **kwargs):
        if args[0] == "tag":
            return list(tags)
        if args[0] == "grep":
            if not allow_grep:
                raise AssertionError("grep ran even though there were no tags to search")
            assert args[1] == "-lE", f"grep must list file names only, got {args[1]!r}"
            if token is not None:
                assert args[2] == token
            assert list(args[3 : 3 + len(tags)]) == list(tags)
            assert args[3 + len(tags)] == "--"
            if paths:
                assert list(args[4 + len(tags) :]) == sorted(paths)
            assert kwargs["allowed_returncodes"] == (0, 1)
            return list(matches)
        raise AssertionError(f"unexpected git call: {args}")

    return run


class TestRunGit:
    @mock.patch("fill_durable_versions.subprocess.run", autospec=True)
    def test_unexpected_exit_code_is_raised_with_stderr(self, run):
        run.return_value = subprocess.CompletedProcess(
            args=[], returncode=128, stdout="", stderr="fatal: not a git repository"
        )
        with pytest.raises(GitCommandError, match="fatal: not a git repository"):
            run_git("tag", "--list")

    @mock.patch("fill_durable_versions.subprocess.run", autospec=True)
    def test_allowed_exit_code_is_an_empty_answer(self, run):
        run.return_value = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="")
        assert run_git("grep", "-lE", "x", allowed_returncodes=(0, 1)) == []


class TestBuildSourcePath:
    @pytest.mark.parametrize(
        ("provider_id", "qualified_class", "expected"),
        [
            ("cncf-kubernetes", POD_CLASS, POD_PATH),
            (
                "amazon",
                "airflow.providers.amazon.aws.operators.glue.GlueJobOperator",
                "providers/amazon/src/airflow/providers/amazon/aws/operators/glue.py",
            ),
        ],
    )
    def test_multi_part_provider_id_becomes_a_directory(self, provider_id, qualified_class, expected):
        assert build_source_path(provider_id, qualified_class) == expected


class TestDurableGroup:
    def test_marker_and_mixin_get_different_tokens(self):
        marker = durable_group(
            {
                "durable_mechanism": "marker",
                "durable_gating_provider": "cncf-kubernetes",
                "durable_gating_class": POD_CLASS,
            }
        )
        mixin = durable_group(
            {
                "durable_mechanism": "resumable_mixin",
                "durable_gating_provider": "apache-livy",
                "durable_gating_class": "airflow.providers.apache.livy.operators.livy.LivyOperator",
            }
        )
        assert marker == ("cncf-kubernetes", DURABLE_TOKENS["marker"], POD_PATH)
        assert mixin[1] == "execute_resumable"

    def test_marker_token_also_catches_the_durable_parameter(self):
        """AgentOperator took `durable` a release before it took task_state_store."""
        assert "durable: bool" in DURABLE_TOKENS["marker"]
        assert "task_state_store" in DURABLE_TOKENS["marker"]

    @pytest.mark.parametrize(
        "module",
        [
            pytest.param({"durable_mechanism": None}, id="not-durable"),
            pytest.param(
                {"durable_mechanism": "marker", "durable_gating_provider": "cncf-kubernetes"},
                id="no-gating-class",
            ),
            pytest.param(
                {
                    "durable_mechanism": "something-else",
                    "durable_gating_provider": "x",
                    "durable_gating_class": "a.B",
                },
                id="unknown-mechanism",
            ),
        ],
    )
    def test_module_without_a_usable_gate_is_skipped(self, module):
        assert durable_group(module) is None


class TestFindFirstReleases:
    def test_lowest_matching_version_wins_over_tag_order(self):
        tags = [f"providers-cncf-kubernetes/{v}" for v in ("10.20.0", "10.21.0", "9.9.0")]
        matches = [f"{t}:{POD_PATH}" for t in tags]
        with mock.patch(
            "fill_durable_versions.run_git",
            fake_git(tags, matches, token="task_state_store", paths={POD_PATH}),
        ):
            assert find_first_releases("cncf-kubernetes", "task_state_store", {POD_PATH}) == {
                POD_PATH: "9.9.0"
            }

    def test_versions_are_compared_as_versions_not_strings(self):
        tags = [f"providers-amazon/{v}" for v in ("9.35.0", "10.1.0")]
        matches = [f"{t}:x.py" for t in tags]
        with mock.patch("fill_durable_versions.run_git", fake_git(tags, matches)):
            assert find_first_releases("amazon", "execute_resumable", {"x.py"}) == {"x.py": "9.35.0"}

    def test_each_path_is_dated_on_its_own(self):
        tags = [f"providers-amazon/{v}" for v in ("9.33.0", "9.35.0")]
        matches = [
            "providers-amazon/9.33.0:redshift.py",
            "providers-amazon/9.35.0:redshift.py",
            "providers-amazon/9.35.0:glue.py",
        ]
        with mock.patch("fill_durable_versions.run_git", fake_git(tags, matches)):
            assert find_first_releases("amazon", "execute_resumable", {"glue.py", "redshift.py"}) == {
                "redshift.py": "9.33.0",
                "glue.py": "9.35.0",
            }

    def test_prerelease_tags_are_not_offered_as_an_answer(self):
        tags = ["providers-amazon/9.35.0rc1", "providers-amazon/9.35.0"]
        with mock.patch("fill_durable_versions.run_git", fake_git(tags, [])):
            assert get_released_tags("amazon") == ("providers-amazon/9.35.0",)

    def test_no_tags_means_no_grep_at_all(self):
        with mock.patch("fill_durable_versions.run_git", fake_git([], [], allow_grep=False)):
            assert find_first_releases("amazon", "execute_resumable", {"glue.py"}) == {}


class TestMain:
    def write_catalog(self, tmp_path, modules):
        path = tmp_path / "modules.json"
        path.write_text(json.dumps({"modules": modules}))
        return path

    def test_durable_modules_are_dated_and_others_left_alone(self, tmp_path, capsys):
        path = self.write_catalog(
            tmp_path,
            [
                {
                    "name": "KubernetesPodOperator",
                    "durable_mechanism": "marker",
                    "durable_gating_provider": "cncf-kubernetes",
                    "durable_gating_class": POD_CLASS,
                    "durable_since_version": None,
                },
                {"name": "BashOperator", "durable_mechanism": None, "durable_since_version": None},
            ],
        )
        tags = ["providers-cncf-kubernetes/10.20.0"]
        with mock.patch("fill_durable_versions.run_git", fake_git(tags, [f"{tags[0]}:{POD_PATH}"])):
            assert main_with_args(path) == 0

        modules = json.loads(path.read_text())["modules"]
        assert modules[0]["durable_since_version"] == "10.20.0"
        assert modules[1]["durable_since_version"] is None
        assert "dated 1 durable modules" in capsys.readouterr().out

    def test_extraction_only_fields_never_reach_the_published_catalog(self, tmp_path):
        path = self.write_catalog(
            tmp_path,
            [
                {
                    "name": "KubernetesPodOperator",
                    "durable_mechanism": "marker",
                    "durable_gating_provider": "cncf-kubernetes",
                    "durable_gating_class": POD_CLASS,
                    "durable_since_version": None,
                }
            ],
        )
        tags = ["providers-cncf-kubernetes/10.20.0"]
        with mock.patch("fill_durable_versions.run_git", fake_git(tags, [f"{tags[0]}:{POD_PATH}"])):
            assert main_with_args(path) == 0

        module = json.loads(path.read_text())["modules"][0]
        assert module["durable_since_version"] == "10.20.0"
        assert "durable_mechanism" not in module
        assert "durable_gating_class" not in module
        assert module["durable_gating_provider"] == "cncf-kubernetes"

    def test_tagless_checkout_still_strips_but_dates_nothing(self, tmp_path):
        path = self.write_catalog(
            tmp_path,
            [
                {
                    "name": "KubernetesPodOperator",
                    "durable_mechanism": "marker",
                    "durable_gating_provider": "cncf-kubernetes",
                    "durable_gating_class": POD_CLASS,
                    "durable_since_version": None,
                }
            ],
        )
        with mock.patch("fill_durable_versions.run_git", fake_git([], [], allow_grep=False)):
            assert main_with_args(path) == 0

        module = json.loads(path.read_text())["modules"][0]
        assert module["durable_since_version"] is None
        assert "durable_mechanism" not in module
        assert "durable_gating_class" not in module

    def test_broken_checkout_is_not_mistaken_for_a_tagless_one(self, tmp_path):
        path = self.write_catalog(tmp_path, [])

        def exploding_git(*args, **kwargs):
            raise GitCommandError("fatal: not a git repository")

        with mock.patch("fill_durable_versions.run_git", exploding_git):
            with pytest.raises(GitCommandError):
                main_with_args(path)

    def test_missing_file_is_skipped_without_failing(self, tmp_path, capsys):
        with mock.patch("fill_durable_versions.run_git", fake_git(["providers-amazon/1.0.0"], [])):
            assert main_with_args(tmp_path / "nope.json") == 0
        assert "Skipping missing" in capsys.readouterr().out


def main_with_args(*paths):
    argv = ["fill_durable_versions.py", "--modules", *[str(p) for p in paths]]
    with mock.patch("sys.argv", argv):
        return main()
