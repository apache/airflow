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

from pathlib import Path

import pytest
from ci.prek.common_prek_utils import (
    ConsoleDiff,
    check_list_sorted,
    get_imports_from_file,
    get_provider_base_dir_from_path,
    get_provider_id_from_path,
    initialize_breeze_prek,
    insert_documentation,
    pre_process_mypy_files,
    read_airflow_version,
    read_allowed_kubernetes_versions,
    temporary_tsc_project,
)

PROVIDERS_AMAZON_S3_PATH = "providers/amazon/hooks/s3.py"
AIRFLOW_MODELS_DAG_PATH = "airflow/models/dag.py"


class TestPreProcessMypyFiles:
    def test_excludes_conftest(self):
        files = ["tests/conftest.py", "tests/test_foo.py"]
        result = pre_process_mypy_files(files)
        assert "tests/conftest.py" not in result
        assert "tests/test_foo.py" in result

    def test_excludes_init(self):
        files = ["airflow/__init__.py", AIRFLOW_MODELS_DAG_PATH]
        result = pre_process_mypy_files(files)
        assert "airflow/__init__.py" not in result
        assert AIRFLOW_MODELS_DAG_PATH in result

    def test_excludes_both(self):
        files = ["conftest.py", "__init__.py", "test_foo.py"]
        result = pre_process_mypy_files(files)
        assert result == ["test_foo.py"]

    def test_empty_list(self):
        assert pre_process_mypy_files([]) == []

    def test_on_non_main_branch_excludes_providers(self, monkeypatch):
        monkeypatch.setenv("DEFAULT_BRANCH", "v2-10-stable")
        files = [PROVIDERS_AMAZON_S3_PATH, AIRFLOW_MODELS_DAG_PATH]
        result = pre_process_mypy_files(files)
        assert PROVIDERS_AMAZON_S3_PATH not in result
        assert AIRFLOW_MODELS_DAG_PATH in result

    def test_on_main_branch_keeps_providers(self, monkeypatch):
        monkeypatch.setenv("DEFAULT_BRANCH", "main")
        files = [PROVIDERS_AMAZON_S3_PATH, AIRFLOW_MODELS_DAG_PATH]
        result = pre_process_mypy_files(files)
        assert PROVIDERS_AMAZON_S3_PATH in result
        assert AIRFLOW_MODELS_DAG_PATH in result

    def test_no_default_branch_keeps_providers(self, monkeypatch):
        monkeypatch.delenv("DEFAULT_BRANCH", raising=False)
        files = [PROVIDERS_AMAZON_S3_PATH]
        result = pre_process_mypy_files(files)
        assert PROVIDERS_AMAZON_S3_PATH in result


class TestGetImportsFromFile:
    def test_simple_import(self, write_python_file):
        path = write_python_file("import os\nimport sys\n")
        result = get_imports_from_file(path, only_top_level=True)
        assert "os" in result
        assert "sys" in result

    def test_from_import(self, write_python_file):
        path = write_python_file("from collections import defaultdict\n")
        result = get_imports_from_file(path, only_top_level=True)
        assert "collections.defaultdict" in result

    def test_skips_future_imports(self, write_python_file):
        path = write_python_file("from __future__ import annotations\nimport os\n")
        result = get_imports_from_file(path, only_top_level=True)
        assert not any("__future__" in imp for imp in result)
        assert "os" in result

    def test_top_level_only_excludes_nested(self, write_python_file):
        code = """\
        import os

        def inner():
            import json
        """
        path = write_python_file(code)
        top_level = get_imports_from_file(path, only_top_level=True)
        assert "os" in top_level
        assert "json" not in top_level

    def test_all_levels_includes_nested(self, write_python_file):
        code = """\
        import os

        def inner():
            import json
        """
        path = write_python_file(code)
        all_level = get_imports_from_file(path, only_top_level=False)
        assert "os" in all_level
        assert "json" in all_level

    def test_multiple_from_imports(self, write_python_file):
        path = write_python_file("from pathlib import Path, PurePath\n")
        result = get_imports_from_file(path, only_top_level=True)
        assert "pathlib.Path" in result
        assert "pathlib.PurePath" in result

    def test_empty_file(self, write_python_file):
        path = write_python_file("")
        result = get_imports_from_file(path, only_top_level=True)
        assert result == []


class TestInsertDocumentation:
    def test_replaces_content_between_header_and_footer(self, write_text_file):
        path = write_text_file("before\n<!-- START -->\nold content\n<!-- END -->\nafter\n")
        result = insert_documentation(
            path,
            content=["new line 1\n", "new line 2\n"],
            header="<!-- START -->",
            footer="<!-- END -->",
        )
        assert result is True
        text = path.read_text()
        assert "new line 1" in text
        assert "new line 2" in text
        assert "old content" not in text
        assert "before" in text
        assert "after" in text

    def test_returns_false_when_content_unchanged(self, write_text_file):
        path = write_text_file("before\n<!-- START -->\nkept\n<!-- END -->\nafter\n")
        result = insert_documentation(
            path,
            content=["kept\n"],
            header="<!-- START -->",
            footer="<!-- END -->",
        )
        assert result is False

    def test_exits_when_header_not_found(self, write_text_file):
        path = write_text_file("no markers here\n")
        with pytest.raises(SystemExit):
            insert_documentation(
                path,
                content=["anything\n"],
                header="<!-- MISSING -->",
                footer="<!-- END -->",
            )

    def test_add_comment_prefixes_lines(self, write_text_file):
        path = write_text_file("before\n# START\nold\n# END\nafter\n")
        result = insert_documentation(
            path,
            content=["line one\n", "line two\n"],
            header="# START",
            footer="# END",
            add_comment=True,
        )
        assert result is True
        text = path.read_text()
        assert "# line one\n" in text
        assert "# line two\n" in text

    def test_add_comment_handles_blank_lines(self, write_text_file):
        path = write_text_file("# START\nold\n# END\n")
        result = insert_documentation(
            path,
            content=["\n"],
            header="# START",
            footer="# END",
            add_comment=True,
        )
        assert result is True
        text = path.read_text()
        assert "#\n" in text

    def test_preserves_header_and_footer_lines(self, write_text_file):
        path = write_text_file("<!-- START -->\nold\n<!-- END -->\n")
        insert_documentation(
            path,
            content=["new\n"],
            header="<!-- START -->",
            footer="<!-- END -->",
        )
        text = path.read_text()
        assert "<!-- START -->" in text
        assert "<!-- END -->" in text

    def test_header_with_leading_whitespace(self, write_text_file):
        path = write_text_file("  <!-- START -->\nold\n  <!-- END -->\n")
        result = insert_documentation(
            path,
            content=["new\n"],
            header="<!-- START -->",
            footer="<!-- END -->",
        )
        assert result is True
        assert "new" in path.read_text()

    def test_multiple_content_lines(self, write_text_file):
        path = write_text_file("header line\n## BEGIN\nreplaced\n## FINISH\nfooter line\n")
        insert_documentation(
            path,
            content=["a\n", "b\n", "c\n"],
            header="## BEGIN",
            footer="## FINISH",
        )
        text = path.read_text()
        assert "a\nb\nc\n" in text
        assert "replaced" not in text
        assert "header line" in text
        assert "footer line" in text


class TestReadAirflowVersion:
    def test_returns_version_string(self):
        version = read_airflow_version()
        assert isinstance(version, str)
        # Airflow version should look like X.Y.Z or X.Y.Z.devN
        parts = version.split(".")
        assert len(parts) >= 3
        assert parts[0].isdigit()
        assert parts[1].isdigit()


class TestReadAllowedKubernetesVersions:
    def test_returns_list_of_versions(self):
        versions = read_allowed_kubernetes_versions()
        assert isinstance(versions, list)
        assert len(versions) > 0

    def test_versions_have_no_v_prefix(self):
        versions = read_allowed_kubernetes_versions()
        for v in versions:
            assert not v.startswith("v"), f"Version {v!r} should not have 'v' prefix"

    def test_versions_look_like_semver(self):
        versions = read_allowed_kubernetes_versions()
        for v in versions:
            parts = v.split(".")
            assert len(parts) >= 2, f"Version {v!r} should have at least major.minor"
            assert parts[0].isdigit()
            assert parts[1].isdigit()


class TestConsoleDiff:
    def test_dump_added_lines(self):
        diff = ConsoleDiff()
        lines = list(diff._dump("+", ["line1", "line2"], 0, 2))
        assert lines == ["[green]+ line1[/]", "[green]+ line2[/]"]

    def test_dump_removed_lines(self):
        diff = ConsoleDiff()
        lines = list(diff._dump("-", ["line1"], 0, 1))
        assert lines == ["[red]- line1[/]"]

    def test_dump_unchanged_lines(self):
        diff = ConsoleDiff()
        lines = list(diff._dump(" ", ["line1", "line2"], 0, 2))
        assert lines == ["  line1", "  line2"]

    def test_dump_range(self):
        diff = ConsoleDiff()
        lines = list(diff._dump("+", ["a", "b", "c", "d"], 1, 3))
        assert lines == ["[green]+ b[/]", "[green]+ c[/]"]


class TestCheckListSorted:
    def test_sorted_list_returns_true(self):
        errors: list[str] = []
        result = check_list_sorted(["a", "b", "c"], "test list", errors)
        assert result is True
        assert errors == []

    def test_unsorted_list_returns_false(self):
        errors: list[str] = []
        result = check_list_sorted(["c", "a", "b"], "test list", errors)
        assert result is False
        assert len(errors) == 1
        assert "not sorted" in errors[0]

    def test_duplicates_returns_false(self):
        errors: list[str] = []
        result = check_list_sorted(["a", "a", "b"], "test list", errors)
        assert result is False
        assert len(errors) == 1

    def test_empty_list_returns_true(self):
        errors: list[str] = []
        result = check_list_sorted([], "empty", errors)
        assert result is True
        assert errors == []

    def test_single_element_returns_true(self):
        errors: list[str] = []
        result = check_list_sorted(["only"], "single", errors)
        assert result is True
        assert errors == []


class TestGetProviderIdFromPath:
    def test_simple_provider(self, create_provider_tree):
        file_path = create_provider_tree("providers/amazon")
        result = get_provider_id_from_path(file_path)
        assert result == "amazon"

    def test_nested_provider(self, create_provider_tree):
        file_path = create_provider_tree("providers/apache/hive")
        result = get_provider_id_from_path(file_path)
        assert result == "apache.hive"

    def test_no_provider_yaml(self, tmp_path):
        some_dir = tmp_path / "no_provider"
        some_dir.mkdir()
        test_file = some_dir / "file.py"
        test_file.touch()
        result = get_provider_id_from_path(test_file)
        assert result is None

    def test_no_providers_parent(self, tmp_path):
        # provider.yaml exists but no "providers" parent directory
        some_dir = tmp_path / "something" / "else"
        some_dir.mkdir(parents=True)
        (some_dir / "provider.yaml").touch()
        test_file = some_dir / "file.py"
        test_file.touch()
        result = get_provider_id_from_path(test_file)
        assert result is None


class TestGetProviderBaseDirFromPath:
    def test_finds_provider_dir(self, tmp_path):
        provider_dir = tmp_path / "providers" / "amazon"
        provider_dir.mkdir(parents=True)
        (provider_dir / "provider.yaml").touch()
        sub_file = provider_dir / "hooks" / "s3.py"
        sub_file.parent.mkdir()
        sub_file.touch()
        result = get_provider_base_dir_from_path(sub_file)
        assert result == provider_dir

    def test_returns_none_without_provider_yaml(self, tmp_path):
        some_dir = tmp_path / "no_provider"
        some_dir.mkdir()
        test_file = some_dir / "file.py"
        test_file.touch()
        result = get_provider_base_dir_from_path(test_file)
        assert result is None

    def test_finds_nearest_provider_yaml(self, tmp_path):
        outer = tmp_path / "providers" / "google"
        inner = outer / "cloud"
        inner.mkdir(parents=True)
        (outer / "provider.yaml").touch()
        test_file = inner / "hooks.py"
        test_file.touch()
        result = get_provider_base_dir_from_path(test_file)
        assert result == outer


class TestInitializeBreezePrek:
    def test_raises_when_not_main(self):
        with pytest.raises(SystemExit, match="intended to be executed"):
            initialize_breeze_prek("some_module", "script.py")

    def test_exits_when_skip_env_set(self, monkeypatch):
        monkeypatch.setenv("SKIP_BREEZE_PREK_HOOKS", "1")
        with pytest.raises(SystemExit) as exc_info:
            initialize_breeze_prek("__main__", "script.py")
        assert exc_info.value.code == 0

    def test_exits_when_breeze_not_found(self, monkeypatch):
        monkeypatch.delenv("SKIP_BREEZE_PREK_HOOKS", raising=False)
        monkeypatch.setattr("shutil.which", lambda _: None)
        with pytest.raises(SystemExit) as exc_info:
            initialize_breeze_prek("__main__", "script.py")
        assert exc_info.value.code == 1


class TestTemporaryTscProject:
    def test_creates_temp_tsconfig(self, tmp_path):
        tsconfig = tmp_path / "tsconfig.json"
        tsconfig.write_text("{}")
        with temporary_tsc_project(tsconfig, ["src/app.ts", "src/main.ts"]) as temp:
            content = Path(temp.name).read_text()
            assert '"src/app.ts"' in content
            assert '"src/main.ts"' in content
            assert f"./{tsconfig.name}" in content

    def test_raises_when_tsconfig_missing(self, tmp_path):
        missing = tmp_path / "nonexistent.json"
        with pytest.raises(RuntimeError, match="Cannot find"):
            with temporary_tsc_project(missing, []):
                pass

    def test_extends_original_tsconfig(self, tmp_path):
        tsconfig = tmp_path / "tsconfig.base.json"
        tsconfig.write_text("{}")
        with temporary_tsc_project(tsconfig, ["file.ts"]) as temp:
            content = Path(temp.name).read_text()
            assert f'"extends": "./{tsconfig.name}"' in content
            assert '"include": ["file.ts"]' in content
