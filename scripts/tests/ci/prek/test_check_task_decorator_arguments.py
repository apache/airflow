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

import ast
import re
import textwrap
from pathlib import Path

import pytest
import yaml
from check_task_decorator_arguments import (
    DECORATORS,
    FRAMEWORK_PARAMETERS,
    SHARED_FACTORY,
    STUB_PATH,
    collect_errors,
    extract_parameter_names,
    main,
)
from common_prek_utils import AIRFLOW_ROOT_PATH


@pytest.fixture
def source_tree(tmp_path: Path) -> Path:
    # Breeze may put temporary files in the checkout; hide fixtures from repo-wide source scans.
    root = tmp_path / ".sources"
    modules = {
        DECORATORS[0].operator.path: """
            raise RuntimeError("Source must not be imported")
            class KubernetesPodOperator:
                def __init__(self, *, image=None, cmds=None, arguments=None, **kwargs): ...
        """,
        DECORATORS[0].factory.path: """
            class _KubernetesDecoratedOperator:
                def __init__(self, namespace=None, use_dill=False, **kwargs): ...
            def kubernetes_task(python_callable=None, multiple_outputs=None, **kwargs): ...
        """,
        DECORATORS[1].factory.path: """
            class _KubernetesCmdDecoratedOperator:
                def __init__(self, *, python_callable, args_only=False, **kwargs): ...
            def kubernetes_cmd_task(python_callable=None, **kwargs): ...
        """,
        SHARED_FACTORY.path: """
            def task_decorator_factory(
                python_callable=None, *, multiple_outputs=must_not_run(), decorated_operator_class, **kwargs
            ): ...
        """,
        STUB_PATH: """
            class TaskDecoratorCollection:
                def kubernetes(self, *, image=None, namespace=None, arguments=None, use_dill=False, multiple_outputs=None, **kwargs): ...
                def kubernetes(self, python_callable): ...
                def kubernetes_cmd(self, *, image=None, args_only=False, multiple_outputs=None, **kwargs): ...
                def kubernetes_cmd(self, python_callable): ...
        """,
    }
    for relative_path, source in modules.items():
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source), encoding="utf-8")
    return root


def replace_source(root: Path, relative_path: Path, old: str, new: str) -> None:
    path = root / relative_path
    source = path.read_text(encoding="utf-8")
    assert old in source
    path.write_text(source.replace(old, new), encoding="utf-8")


def test_synchronized_signatures_pass_without_importing_source(source_tree):
    assert collect_errors(source_tree) == []


@pytest.mark.parametrize(
    ("replacement", "missing", "unexpected"),
    [
        pytest.param("image=None, z_option=None, a_option=None,", "a_option, z_option", None, id="added"),
        pytest.param("", None, "image", id="removed"),
        pytest.param("renamed_image=None,", "renamed_image", "image", id="renamed"),
    ],
)
def test_operator_changes_report_both_decorators(source_tree, replacement, missing, unexpected):
    replace_source(source_tree, DECORATORS[0].operator.path, "image=None,", replacement)
    errors = collect_errors(source_tree)
    assert len(errors) == 2 * (bool(missing) + bool(unexpected))
    for spec in DECORATORS:
        relevant = [error for error in errors if f"@task.{spec.name} " in error]
        if missing:
            assert any(f"missing parameters: {missing}." in error for error in relevant)
        if unexpected:
            assert any(f"unexpected parameters: {unexpected}." in error for error in relevant)
        assert all(str(STUB_PATH) in error and str(spec.operator.path) in error for error in relevant)


@pytest.mark.parametrize(("name", "parameter"), [("kubernetes", "use_dill"), ("kubernetes_cmd", "args_only")])
def test_either_stub_can_drift_independently(source_tree, name, parameter):
    replace_source(source_tree, STUB_PATH, f"{parameter}=False,", "")
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert f"@task.{name} is missing parameters: {parameter}." in errors[0]


@pytest.mark.parametrize(
    ("reference", "old", "new", "affected"),
    [
        pytest.param(
            DECORATORS[0].decorated_operator,
            "use_dill=False,",
            "use_dill=False, wrapper_option=None,",
            ("kubernetes",),
            id="decorated-operator",
        ),
        pytest.param(
            DECORATORS[0].factory,
            "def kubernetes_task(python_callable=None,",
            "def kubernetes_task(python_callable=None, wrapper_option=None,",
            ("kubernetes",),
            id="provider-factory",
        ),
        pytest.param(
            SHARED_FACTORY,
            "multiple_outputs=must_not_run(),",
            "multiple_outputs=must_not_run(), wrapper_option=None,",
            ("kubernetes", "kubernetes_cmd"),
            id="shared-factory",
        ),
    ],
)
def test_new_decorator_and_factory_options_require_declarations(source_tree, reference, old, new, affected):
    replace_source(source_tree, reference.path, old, new)
    errors = collect_errors(source_tree)
    assert len(errors) == len(affected)
    for name, error in zip(affected, errors):
        assert f"@task.{name} is missing parameters: wrapper_option." in error
        assert str(reference.path) in error


def test_shared_multiple_outputs_must_be_declared_by_both_stubs(source_tree):
    replace_source(source_tree, STUB_PATH, "multiple_outputs=None,", "")
    errors = collect_errors(source_tree)
    assert len(errors) == 2
    assert all("missing parameters: multiple_outputs." in error for error in errors)


@pytest.mark.parametrize("parameter", sorted(FRAMEWORK_PARAMETERS))
def test_framework_supplied_parameters_are_excluded(source_tree, parameter):
    replace_source(source_tree, DECORATORS[0].operator.path, "image=None,", f"image=None, {parameter}=None,")
    assert collect_errors(source_tree) == []


@pytest.mark.parametrize(
    ("name", "parameter"),
    [("kubernetes", "cmds"), ("kubernetes_cmd", "cmds"), ("kubernetes_cmd", "arguments")],
)
def test_internally_supplied_parameters_must_not_be_declared(source_tree, name, parameter):
    replace_source(source_tree, STUB_PATH, f"def {name}(self, *,", f"def {name}(self, *, {parameter}=None,")
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert f"@task.{name} declares unexpected parameters: {parameter}." in errors[0]


def test_obsolete_exclusions_fail(source_tree):
    replace_source(source_tree, DECORATORS[0].operator.path, "cmds=None,", "")
    errors = collect_errors(source_tree)
    assert len(errors) == 2
    assert all(
        "internally supplied parameters no longer exist on KubernetesPodOperator: cmds" in e for e in errors
    )


def test_bare_overloads_are_ignored(source_tree):
    replace_source(source_tree, STUB_PATH, "self, python_callable", "self, python_callable, bare_only=None")
    assert collect_errors(source_tree) == []


def test_annotations_and_defaults_are_not_compared(source_tree):
    replace_source(source_tree, STUB_PATH, "image=None", "image: int = another_unevaluated_default()")
    assert collect_errors(source_tree) == []


@pytest.mark.parametrize("receiver", ["self", "cls"])
def test_extracts_all_named_argument_kinds_but_not_variadic_arguments(receiver):
    source = f"def f({receiver}, positional, /, named=None, *args, keyword=None, **kwargs): ..."
    function = ast.parse(source).body[0]
    assert extract_parameter_names(function) == {"positional", "named", "keyword"}


@pytest.mark.parametrize("path", [DECORATORS[0].operator.path, SHARED_FACTORY.path, STUB_PATH])
def test_missing_files_fail_for_each_affected_decorator(source_tree, path):
    (source_tree / path).unlink()
    errors = collect_errors(source_tree)
    assert len(errors) == 2
    assert all(str(path) in error and "No such file" in error for error in errors)


@pytest.mark.parametrize("path", [DECORATORS[0].operator.path, STUB_PATH])
def test_syntax_errors_fail_for_each_affected_decorator(source_tree, path):
    (source_tree / path).write_text("def broken(:\n", encoding="utf-8")
    errors = collect_errors(source_tree)
    assert len(errors) == 2
    assert all("invalid syntax" in error for error in errors)


@pytest.mark.parametrize(
    ("path", "old", "new", "message", "count"),
    [
        (
            DECORATORS[0].operator.path,
            "class KubernetesPodOperator:",
            "class Other:",
            "class KubernetesPodOperator, found 0",
            2,
        ),
        (
            DECORATORS[0].operator.path,
            "def __init__",
            "def other",
            "function KubernetesPodOperator.__init__, found 0",
            2,
        ),
        (
            DECORATORS[0].factory.path,
            "class _KubernetesDecoratedOperator:",
            "class Other:",
            "class _KubernetesDecoratedOperator, found 0",
            1,
        ),
        (
            DECORATORS[0].factory.path,
            "def kubernetes_task",
            "def other",
            "function kubernetes_task, found 0",
            1,
        ),
        (
            SHARED_FACTORY.path,
            "def task_decorator_factory",
            "def other",
            "function task_decorator_factory, found 0",
            2,
        ),
        (
            STUB_PATH,
            "class TaskDecoratorCollection:",
            "class Other:",
            "class TaskDecoratorCollection, found 0",
            2,
        ),
        (
            STUB_PATH,
            "def kubernetes(self, *,",
            "def other(self, *,",
            "configuration overload TaskDecoratorCollection.kubernetes, found 0",
            1,
        ),
    ],
)
def test_missing_definitions_fail_clearly(source_tree, path, old, new, message, count):
    replace_source(source_tree, path, old, new)
    errors = collect_errors(source_tree)
    assert len(errors) == count
    assert all(message in error for error in errors)


@pytest.mark.parametrize(
    ("path", "extra", "message", "count"),
    [
        (
            STUB_PATH,
            "    def kubernetes(self, *, image=None): ...\n",
            "configuration overload TaskDecoratorCollection.kubernetes, found 2",
            1,
        ),
        (
            DECORATORS[0].operator.path,
            "class KubernetesPodOperator: ...\n",
            "class KubernetesPodOperator, found 2",
            2,
        ),
        (
            SHARED_FACTORY.path,
            "def task_decorator_factory(**kwargs): ...\n",
            "function task_decorator_factory, found 2",
            2,
        ),
    ],
)
def test_ambiguous_definitions_fail_clearly(source_tree, path, extra, message, count):
    file_path = source_tree / path
    file_path.write_text(file_path.read_text(encoding="utf-8") + extra, encoding="utf-8")
    errors = collect_errors(source_tree)
    assert len(errors) == count
    assert all(message in error for error in errors)


@pytest.mark.parametrize(("drift", "status"), [(False, 0), (True, 1)])
def test_main_reports_errors_and_returns_exit_status(source_tree, capsys, drift, status):
    if drift:
        replace_source(source_tree, STUB_PATH, "use_dill=False,", "")
    assert main(source_tree) == status
    output = capsys.readouterr().out
    if drift:
        assert "@task.kubernetes is missing parameters: use_dill." in output
    else:
        assert output == ""


@pytest.fixture
def hook():
    config = yaml.safe_load((AIRFLOW_ROOT_PATH / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    return next(
        hook
        for repo in config["repos"]
        for hook in repo["hooks"]
        if hook["id"] == "check-kubernetes-task-decorator-arguments"
    )


@pytest.mark.parametrize(
    ("path", "matches"),
    [
        (DECORATORS[0].operator.path, True),
        (DECORATORS[0].factory.path, True),
        (DECORATORS[1].factory.path, True),
        (SHARED_FACTORY.path, True),
        (STUB_PATH, True),
        (Path("scripts/ci/prek/check_task_decorator_arguments.py"), True),
        (Path("scripts/tests/ci/prek/test_check_task_decorator_arguments.py"), True),
        (Path(".pre-commit-config.yaml"), True),
        (Path("providers/docker/src/airflow/providers/docker/operators/docker.py"), False),
        (Path("dev/decorator_walkthrough.py"), False),
    ],
)
def test_hook_file_selection(hook, path, matches):
    assert bool(re.search(hook["files"], path.as_posix())) is matches


def test_hook_runs_without_file_arguments_or_python_only_filter(hook):
    assert hook["language"] == "python"
    assert hook["pass_filenames"] is False
    assert hook["types"] == ["file"]


def test_repository_kubernetes_stubs_are_synchronized():
    assert collect_errors() == []
