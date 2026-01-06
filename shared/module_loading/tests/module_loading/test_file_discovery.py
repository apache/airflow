#
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
from pathlib import Path

import pytest

from airflow_shared.module_loading import find_path_from_directory


class TestFindPathFromDirectory:
    @pytest.fixture
    def test_dir(self, tmp_path):
        # create test tree with symlinks
        source = os.path.join(tmp_path, "folder")
        target = os.path.join(tmp_path, "symlink")
        py_file = os.path.join(source, "hello_world.py")
        ignore_file = os.path.join(tmp_path, ".airflowignore")
        os.mkdir(source)
        os.symlink(source, target)
        # write ignore files
        with open(ignore_file, "w") as f:
            f.write("folder")
        # write sample pyfile
        with open(py_file, "w") as f:
            f.write("print('hello world')")
        return tmp_path

    def test_find_path_from_directory_respects_symlinks_regexp_ignore(self, test_dir):
        ignore_list_file = ".airflowignore"
        found = list(find_path_from_directory(test_dir, ignore_list_file, "regexp"))

        assert os.path.join(test_dir, "symlink", "hello_world.py") in found
        assert os.path.join(test_dir, "folder", "hello_world.py") not in found

    def test_find_path_from_directory_respects_symlinks_glob_ignore(self, test_dir):
        ignore_list_file = ".airflowignore"
        found = list(find_path_from_directory(test_dir, ignore_list_file, ignore_file_syntax="glob"))

        assert os.path.join(test_dir, "symlink", "hello_world.py") in found
        assert os.path.join(test_dir, "folder", "hello_world.py") not in found

    def test_find_path_from_directory_fails_on_recursive_link(self, test_dir):
        # add a recursive link
        recursing_src = os.path.join(test_dir, "folder2", "recursor")
        recursing_tgt = os.path.join(test_dir, "folder2")
        os.mkdir(recursing_tgt)
        os.symlink(recursing_tgt, recursing_src)

        ignore_list_file = ".airflowignore"

        error_message = (
            f"Detected recursive loop when walking DAG directory {test_dir}: "
            f"{Path(recursing_tgt).resolve()} has appeared more than once."
        )
        with pytest.raises(RuntimeError, match=error_message):
            list(find_path_from_directory(test_dir, ignore_list_file, ignore_file_syntax="glob"))

    def test_airflowignore_negation_unignore_subfolder_file_glob(self, tmp_path):
        """Ensure negation rules can unignore a subfolder and a file inside it when using glob syntax.

        Patterns:
          *                     -> ignore everything
          !subfolder/           -> unignore the subfolder (must match directory rule)
          !subfolder/keep.py    -> unignore a specific file inside the subfolder
        """
        dags_root = tmp_path / "dags"
        (dags_root / "subfolder").mkdir(parents=True)
        # files
        (dags_root / "drop.py").write_text("raise Exception('ignored')\n")
        (dags_root / "subfolder" / "keep.py").write_text("# should be discovered\n")
        (dags_root / "subfolder" / "drop.py").write_text("raise Exception('ignored')\n")

        (dags_root / ".airflowignore").write_text(
            "\n".join(
                [
                    "*",
                    "!subfolder/",
                    "!subfolder/keep.py",
                ]
            )
        )

        detected = set()
        for raw in find_path_from_directory(dags_root, ".airflowignore", "glob"):
            p = Path(raw)
            if p.is_file() and p.suffix == ".py":
                detected.add(p.relative_to(dags_root).as_posix())

        assert detected == {"subfolder/keep.py"}

    def test_airflowignore_negation_nested_with_globstar(self, tmp_path):
        """Negation with ** should work for nested subfolders."""
        dags_root = tmp_path / "dags"
        nested = dags_root / "a" / "b" / "subfolder"
        nested.mkdir(parents=True)

        # files
        (dags_root / "ignore_top.py").write_text("raise Exception('ignored')\n")
        (nested / "keep.py").write_text("# should be discovered\n")
        (nested / "drop.py").write_text("raise Exception('ignored')\n")

        (dags_root / ".airflowignore").write_text(
            "\n".join(
                [
                    "*",
                    "!a/",
                    "!a/b/",
                    "!**/subfolder/",
                    "!**/subfolder/keep.py",
                    "drop.py",
                ]
            )
        )

        detected = set()
        for raw in find_path_from_directory(dags_root, ".airflowignore", "glob"):
            p = Path(raw)
            if p.is_file() and p.suffix == ".py":
                detected.add(p.relative_to(dags_root).as_posix())

        assert detected == {"a/b/subfolder/keep.py"}
