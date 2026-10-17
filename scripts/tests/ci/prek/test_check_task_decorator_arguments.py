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
    BASE_OPERATOR,
    COLLECTION_PATH,
    DECORATED_OPERATOR,
    FRAMEWORK_PARAMETERS,
    PROVIDERS_PATH,
    STUB_PATH,
    Definition,
    SourceTree,
    collect_errors,
    discover_decorators,
    extract_parameter_names,
    find_constructor_calls,
    find_internally_supplied,
    find_overridden_kwargs,
    main,
)
from common_prek_utils import AIRFLOW_ROOT_PATH

KUBERNETES_PACKAGE = PROVIDERS_PATH / "cncf/kubernetes/src/airflow/providers/cncf/kubernetes"
POD_PATH = KUBERNETES_PACKAGE / "operators/pod.py"
KUBERNETES_PATH = KUBERNETES_PACKAGE / "decorators/kubernetes.py"
KUBERNETES_CMD_PATH = KUBERNETES_PACKAGE / "decorators/kubernetes_cmd.py"
KUBERNETES_METADATA = PROVIDERS_PATH / "cncf/kubernetes/provider.yaml"
DEMO_PACKAGE = PROVIDERS_PATH / "demo/src/airflow/providers/demo"


def write_source(root: Path, path: Path, source: str) -> None:
    destination = root / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(textwrap.dedent(source), encoding="utf-8")


def append_source(root: Path, path: Path, source: str) -> None:
    destination = root / path
    destination.write_text(destination.read_text(encoding="utf-8") + source, encoding="utf-8")


def replace_source(root: Path, relative_path: Path, old: str, new: str) -> None:
    path = root / relative_path
    source = path.read_text(encoding="utf-8")
    assert old in source
    path.write_text(source.replace(old, new), encoding="utf-8")


@pytest.fixture
def source_tree(tmp_path: Path) -> Path:
    # Breeze may put temporary files in the checkout; hide fixtures from repo-wide source scans.
    root = tmp_path / ".sources"
    modules = {
        BASE_OPERATOR.path: """
            raise RuntimeError("Source must not be imported")


            class BaseOperator:
                def __init__(self, *, task_id, **kwargs): ...
        """,
        DECORATED_OPERATOR.path: """
            from airflow.sdk.bases.operator import BaseOperator


            class DecoratedOperator(BaseOperator):
                def __init__(self, *, python_callable, task_id, op_args=None, op_kwargs=None, **kwargs): ...


            def task_decorator_factory(
                python_callable=None, *, multiple_outputs=must_not_run(), decorated_operator_class, **kwargs
            ): ...
        """,
        POD_PATH: """
            from airflow.sdk.bases.operator import BaseOperator


            class KubernetesPodOperator(BaseOperator):
                def __init__(self, *, image=None, name=None, cmds=None, arguments=None, **kwargs): ...
        """,
        KUBERNETES_PATH: """
            from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
            from airflow.sdk.bases.decorator import DecoratedOperator, task_decorator_factory


            class _KubernetesDecoratedOperator(DecoratedOperator, KubernetesPodOperator):
                def __init__(self, namespace=None, use_dill=False, **kwargs):
                    op_name = kwargs.pop("name", "generated")
                    super().__init__(namespace=namespace, name=op_name, cmds=["placeholder"], **kwargs)


            def kubernetes_task(python_callable=None, multiple_outputs=None, **kwargs):
                return task_decorator_factory(
                    python_callable=python_callable,
                    multiple_outputs=multiple_outputs,
                    decorated_operator_class=_KubernetesDecoratedOperator,
                    **kwargs,
                )
        """,
        KUBERNETES_CMD_PATH: """
            from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
            from airflow.sdk.bases.decorator import DecoratedOperator, task_decorator_factory


            class _KubernetesCmdDecoratedOperator(DecoratedOperator, KubernetesPodOperator):
                def __init__(self, *, python_callable, args_only=False, **kwargs):
                    cmds = kwargs.pop("cmds", None)
                    super().__init__(python_callable=python_callable, cmds=None, arguments=None, **kwargs)


            def kubernetes_cmd_task(python_callable=None, **kwargs):
                return task_decorator_factory(
                    python_callable=python_callable, decorated_operator_class=_KubernetesCmdDecoratedOperator, **kwargs
                )
        """,
        STUB_PATH: """
            class TaskDecoratorCollection:
                def kubernetes(self, *, image=None, name=None, namespace=None, arguments=None, use_dill=False, multiple_outputs=None, **kwargs): ...
                def kubernetes(self, python_callable): ...
                def kubernetes_cmd(self, *, image=None, name=None, args_only=False, multiple_outputs=None, **kwargs): ...
                def kubernetes_cmd(self, python_callable): ...
        """,
        COLLECTION_PATH: """
            class TaskDecoratorCollection:
                run_if = staticmethod(run_if)
                skip_if: object = staticmethod(skip_if)
                def __getattr__(self, name): ...
                def __call__(self, *args, **kwargs): ...
        """,
        KUBERNETES_METADATA: """
            task-decorators:
              - name: kubernetes
                class-name: airflow.providers.cncf.kubernetes.decorators.kubernetes.kubernetes_task
              - name: kubernetes_cmd
                class-name: airflow.providers.cncf.kubernetes.decorators.kubernetes_cmd.kubernetes_cmd_task
        """,
    }
    for relative_path, source in modules.items():
        write_source(root, relative_path, source)
    return root


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
    replace_source(source_tree, POD_PATH, "image=None,", replacement)
    errors = collect_errors(source_tree)
    assert len(errors) == 2 * (bool(missing) + bool(unexpected))
    for name in ("kubernetes", "kubernetes_cmd"):
        relevant = [error for error in errors if f"@task.{name} " in error]
        if missing:
            assert any(f"missing parameters: {missing}. Declared in {POD_PATH}:" in e for e in relevant)
        if unexpected:
            assert any(f"unexpected parameters: {unexpected}. Update {STUB_PATH}" in e for e in relevant)
        assert all(str(STUB_PATH) in error for error in relevant)


@pytest.mark.parametrize(("name", "parameter"), [("kubernetes", "use_dill"), ("kubernetes_cmd", "args_only")])
def test_either_stub_can_drift_independently(source_tree, name, parameter):
    replace_source(source_tree, STUB_PATH, f"{parameter}=False,", "")
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert f"@task.{name} is missing parameters: {parameter}." in errors[0]


@pytest.mark.parametrize(
    ("path", "old", "new", "affected"),
    [
        pytest.param(
            KUBERNETES_PATH,
            "use_dill=False,",
            "use_dill=False, wrapper_option=None,",
            ("kubernetes",),
            id="decorated-operator",
        ),
        pytest.param(
            KUBERNETES_PATH,
            "def kubernetes_task(python_callable=None,",
            "def kubernetes_task(python_callable=None, wrapper_option=None,",
            ("kubernetes",),
            id="provider-factory",
        ),
        pytest.param(
            DECORATED_OPERATOR.path,
            "multiple_outputs=must_not_run(),",
            "multiple_outputs=must_not_run(), wrapper_option=None,",
            ("kubernetes", "kubernetes_cmd"),
            id="shared-factory",
        ),
    ],
)
def test_new_decorator_and_factory_options_require_declarations(source_tree, path, old, new, affected):
    replace_source(source_tree, path, old, new)
    errors = collect_errors(source_tree)
    assert len(errors) == len(affected)
    for name, error in zip(affected, errors):
        assert f"@task.{name} is missing parameters: wrapper_option. Declared in {path}:" in error


def test_shared_multiple_outputs_must_be_declared_by_both_stubs(source_tree):
    replace_source(source_tree, STUB_PATH, "multiple_outputs=None,", "")
    errors = collect_errors(source_tree)
    assert len(errors) == 2
    assert all("missing parameters: multiple_outputs." in error for error in errors)


@pytest.mark.parametrize("parameter", sorted(FRAMEWORK_PARAMETERS))
def test_framework_supplied_parameters_are_excluded(source_tree, parameter):
    replace_source(source_tree, POD_PATH, "image=None,", f"image=None, {parameter}=None,")
    assert collect_errors(source_tree) == []


@pytest.mark.parametrize(
    ("name", "parameter", "supplier"),
    [
        ("kubernetes", "cmds", KUBERNETES_PATH),
        ("kubernetes_cmd", "cmds", KUBERNETES_CMD_PATH),
        ("kubernetes_cmd", "arguments", KUBERNETES_CMD_PATH),
    ],
)
def test_internally_supplied_parameters_must_not_be_declared(source_tree, name, parameter, supplier):
    replace_source(source_tree, STUB_PATH, f"def {name}(self, *,", f"def {name}(self, *, {parameter}=None,")
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert f"@task.{name} declares unexpected parameters: {parameter}." in errors[0]
    assert f"{parameter} is internally supplied at {supplier}:" in errors[0]


def test_keywords_fixed_by_the_factory_must_not_be_declared(source_tree):
    replace_source(
        source_tree,
        KUBERNETES_CMD_PATH,
        "python_callable=python_callable, decorated_operator_class",
        "python_callable=python_callable, multiple_outputs=False, decorated_operator_class",
    )
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert "@task.kubernetes_cmd declares unexpected parameters: multiple_outputs." in errors[0]
    assert f"multiple_outputs is internally supplied at {KUBERNETES_CMD_PATH}:" in errors[0]
    replace_source(source_tree, STUB_PATH, "args_only=False, multiple_outputs=None,", "args_only=False,")
    assert collect_errors(source_tree) == []


@pytest.mark.parametrize(
    ("body", "supplied"),
    [
        pytest.param("super().__init__(own=own, fixed=1, **kwargs)", {"fixed"}, id="literal"),
        pytest.param(
            "name = kwargs.pop('name', 'default')\nsuper().__init__(name=name, **kwargs)",
            set(),
            id="read-from-kwargs",
        ),
        pytest.param(
            "cmds = kwargs.pop('cmds', None)\nsuper().__init__(cmds=None, **kwargs)",
            {"cmds"},
            id="read-from-kwargs-then-discarded",
        ),
        pytest.param(
            "if flag:\n    first = kwargs.pop('first')\nsecond = first\nsuper().__init__(second=second)",
            set(),
            id="derived-through-nested-assignment",
        ),
        pytest.param("super().__init__(value=wrap(own, extra=1))", set(), id="computed-from-parameter"),
        pytest.param("super().__init__(**{'hidden': 1})", set(), id="unpacked-mapping"),
        pytest.param("Parent.__init__(self, fixed=self.value)", {"fixed"}, id="explicit-parent-call"),
    ],
)
def test_find_internally_supplied(body, supplied):
    source = f"def __init__(self, own=None, **kwargs):\n{textwrap.indent(body, '    ')}\n"
    function = ast.parse(source).body[0]
    assert isinstance(function, ast.FunctionDef)
    (call,) = find_constructor_calls(function)
    assert find_internally_supplied(function, call) == supplied


@pytest.mark.parametrize(
    ("signature", "body", "overridden"),
    [
        pytest.param("own=None, **kwargs", "kwargs['fixed'] = True", {"fixed"}, id="unconditional-fixed"),
        pytest.param(
            "own=None, **kwargs",
            "if 'fixed' not in kwargs:\n    kwargs['fixed'] = 5",
            set(),
            id="conditional-default",
        ),
        pytest.param(
            "own=None, **kwargs", "kwargs['task_id'] = unique(own)", set(), id="computed-from-parameter"
        ),
        pytest.param("own=None, **kwargs", "kwargs[key] = True", set(), id="non-literal-key"),
        pytest.param("own=None, **options", "options['fixed'] = True", {"fixed"}, id="renamed-kwargs"),
        pytest.param("own=None", "other['fixed'] = True", set(), id="no-kwargs"),
    ],
)
def test_find_overridden_kwargs(signature, body, overridden):
    source = f"def __init__(self, {signature}):\n{textwrap.indent(body, '    ')}\n"
    function = ast.parse(source).body[0]
    assert isinstance(function, ast.FunctionDef)
    assert find_overridden_kwargs(function) == overridden


def test_kwargs_overridden_by_a_constructor_must_not_be_declared(source_tree):
    replace_source(
        source_tree,
        KUBERNETES_PATH,
        'op_name = kwargs.pop("name", "generated")',
        'op_name = kwargs.pop("name", "generated")\n        kwargs["image"] = "fixed"',
    )
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert "@task.kubernetes declares unexpected parameters: image." in errors[0]
    assert f"image is internally supplied at {KUBERNETES_PATH}:" in errors[0]


@pytest.mark.parametrize(
    ("parent", "missing"),
    [
        pytest.param("class _PodBase(BaseOperator):\n    def __init__(self, *, new_option=None): ...", True),
        pytest.param("class _PodBase(BaseOperator): ...", False, id="without-constructor"),
    ],
)
def test_parent_constructors_are_discovered(source_tree, parent, missing):
    replace_source(
        source_tree,
        POD_PATH,
        "class KubernetesPodOperator(BaseOperator):",
        f"{parent}\n\n\nclass KubernetesPodOperator(_PodBase):",
    )
    errors = collect_errors(source_tree)
    assert len(errors) == 2 * missing
    assert all("missing parameters: new_option." in error for error in errors)


@pytest.mark.parametrize("declared_earlier", [False, True])
def test_parent_parameter_fixed_by_a_constructor_is_hidden_unless_declared_earlier(
    source_tree, declared_earlier
):
    replace_source(
        source_tree,
        POD_PATH,
        "class KubernetesPodOperator(BaseOperator):",
        "class _PodBase(BaseOperator):\n"
        "    def __init__(self, *, mode=None, **kwargs): ...\n\n\n"
        "class KubernetesPodOperator(_PodBase):",
    )
    replace_source(
        source_tree,
        POD_PATH,
        "**kwargs): ...\n",
        "**kwargs):\n        super().__init__(mode='pod', **kwargs)\n",
    )
    if declared_earlier:
        replace_source(
            source_tree, KUBERNETES_PATH, "use_dill=False, **kwargs", "use_dill=False, mode=None, **kwargs"
        )
    errors = collect_errors(source_tree)
    assert len(errors) == declared_earlier
    assert all("@task.kubernetes is missing parameters: mode." in error for error in errors)


def test_classes_after_base_operator_in_the_mro_are_ignored(source_tree):
    replace_source(
        source_tree,
        POD_PATH,
        "class KubernetesPodOperator(BaseOperator):",
        "class LoggingLike:\n"
        "    def __init__(self, context=None): ...\n\n\n"
        "class Mixin(LoggingLike): ...\n\n\n"
        "class KubernetesPodOperator(BaseOperator, Mixin):",
    )
    assert collect_errors(source_tree) == []


def test_linearize_follows_c3_order(tmp_path):
    write_source(
        tmp_path,
        Path("module.py"),
        """
        class Base: ...
        class Left(Base): ...
        class Right(Base): ...
        class Child(Left, Right): ...
        """,
    )
    order = SourceTree(tmp_path).linearize(Definition(Path("module.py"), "Child"))
    assert [definition.name for definition in order] == ["Child", "Left", "Right", "Base"]


def test_linearize_rejects_inconsistent_order(tmp_path):
    write_source(
        tmp_path,
        Path("module.py"),
        """
        class X: ...
        class Y: ...
        class A(X, Y): ...
        class B(Y, X): ...
        class C(A, B): ...
        """,
    )
    with pytest.raises(ValueError, match="module.py:C: cannot create a consistent method resolution order"):
        SourceTree(tmp_path).linearize(Definition(Path("module.py"), "C"))


def test_builtin_and_third_party_bases_are_skipped(tmp_path):
    write_source(
        tmp_path,
        Path("module.py"),
        """
        import typing
        from abc import ABC
        from typing import Generic
        class C(ABC, Generic[T], typing.Protocol, object): ...
        """,
    )
    assert SourceTree(tmp_path).get_bases(Definition(Path("module.py"), "C")) == []


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ("class C(Missing): ...", "cannot resolve base class Missing"),
        ("from airflow.missing import Missing\nclass C(Missing): ...", "cannot resolve base class Missing"),
        ("class C(make_base()): ...", "unsupported base class make_base()"),
        (
            "import airflow.example as example\nclass C(example.Base): ...",
            "unsupported base class example.Base",
        ),
        ("from .sibling import Base\nclass C(Base): ...", "relative import outside a known source root"),
        ("def C(): ...", "module.py:C: expected a class"),
    ],
)
def test_unresolvable_bases_fail_clearly(tmp_path, source, message):
    write_source(tmp_path, Path("module.py"), source)
    with pytest.raises(ValueError, match=re.escape(message)):
        SourceTree(tmp_path).get_bases(Definition(Path("module.py"), "C"))


@pytest.fixture
def demo_tree(tmp_path: Path) -> Path:
    write_source(tmp_path, PROVIDERS_PATH / "demo/provider.yaml", "name: Demo\n")
    write_source(tmp_path, DEMO_PACKAGE / "target.py", "class Target: ...\nclass Renamed: ...\n")
    write_source(tmp_path, DEMO_PACKAGE / "other.py", "class Target: ...\n")
    write_source(tmp_path, DEMO_PACKAGE / "reexport.py", "from airflow.providers.demo.target import Target\n")
    return tmp_path


@pytest.mark.parametrize(
    ("source", "module", "name"),
    [
        pytest.param("class Target: ...", "user", "Target", id="local"),
        pytest.param("from airflow.providers.demo.target import Target", "target", "Target", id="absolute"),
        pytest.param("from .target import Target", "target", "Target", id="relative"),
        pytest.param("from .target import Renamed as Target", "target", "Renamed", id="alias"),
        pytest.param("from .reexport import Target", "target", "Target", id="re-export"),
        pytest.param(
            "if CONDITION:\n    from .target import Target\nelse:\n    from .other import Target",
            "target",
            "Target",
            id="first-binding-wins",
        ),
        pytest.param(
            "try:\n    from airflow.missing import Target\nexcept ImportError:\n    from .target import Target",
            "target",
            "Target",
            id="fallback-when-first-is-missing",
        ),
        pytest.param(
            "_IMPORT_MAP = {'Target': 'airflow.providers.demo.target'}", "target", "Target", id="lazy-table"
        ),
        pytest.param(
            "_IMPORT_MAP: dict = {'Target': ('airflow.missing', '.target')}",
            "target",
            "Target",
            id="lazy-table-fallback-relative",
        ),
    ],
)
def test_resolve_follows_imports_and_reexports(demo_tree, source, module, name):
    write_source(demo_tree, DEMO_PACKAGE / "user.py", source)
    resolved = SourceTree(demo_tree).resolve(DEMO_PACKAGE / "user.py", "Target")
    assert resolved == Definition(DEMO_PACKAGE / f"{module}.py", name)


def test_resolve_stops_on_reexport_cycles(demo_tree):
    write_source(demo_tree, DEMO_PACKAGE / "user.py", "from .cycle import Target\n")
    write_source(demo_tree, DEMO_PACKAGE / "cycle.py", "from .user import Target\n")
    assert SourceTree(demo_tree).resolve(DEMO_PACKAGE / "user.py", "Target") is None


def test_new_provider_decorator_is_checked_without_changing_the_checker(source_tree):
    write_source(
        source_tree,
        PROVIDERS_PATH / "new/provider.yaml",
        """
        task-decorators:
          - name: example
            class-name: airflow.providers.new.decorators.example.example_task
        """,
    )
    write_source(
        source_tree,
        PROVIDERS_PATH / "new/src/airflow/providers/new/operators/example.py",
        """
        from airflow.sdk.bases.operator import BaseOperator


        class ExampleOperator(BaseOperator):
            def __init__(self, *, query=None, timeout=None, **kwargs): ...
        """,
    )
    write_source(
        source_tree,
        PROVIDERS_PATH / "new/src/airflow/providers/new/decorators/example.py",
        """
        from airflow.providers.new.operators.example import ExampleOperator
        from airflow.sdk.bases.decorator import DecoratedOperator, task_decorator_factory


        class _ExampleDecoratedOperator(DecoratedOperator, ExampleOperator):
            def __init__(self, **kwargs):
                super().__init__(query="set during execution", **kwargs)


        def example_task(python_callable=None, **kwargs):
            return task_decorator_factory(
                python_callable=python_callable, decorated_operator_class=_ExampleDecoratedOperator, **kwargs
            )
        """,
    )
    append_source(source_tree, STUB_PATH, "    def example(self, *, multiple_outputs=None, **kwargs): ...\n")
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert "@task.example is missing parameters: timeout." in errors[0]
    replace_source(source_tree, STUB_PATH, "def example(self, *,", "def example(self, *, timeout=None,")
    assert collect_errors(source_tree) == []


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        pytest.param(
            "decorated_operator_class=_KubernetesDecoratedOperator,",
            "",
            "one call passing decorated_operator_class, found 0",
            id="no-call",
        ),
        pytest.param(
            "return task_decorator_factory(",
            "task_decorator_factory(decorated_operator_class=_KubernetesDecoratedOperator)\n"
            "    return task_decorator_factory(",
            "one call passing decorated_operator_class, found 2",
            id="two-calls",
        ),
        pytest.param(
            "decorated_operator_class=_KubernetesDecoratedOperator,",
            "decorated_operator_class=get_class(),",
            "cannot resolve decorated_operator_class get_class()",
            id="dynamic-class",
        ),
        pytest.param(
            "decorated_operator_class=_KubernetesDecoratedOperator,",
            "decorated_operator_class=kubernetes_task,",
            "kubernetes.py:kubernetes_task: expected a class",
            id="not-a-class",
        ),
        pytest.param(
            "class _KubernetesDecoratedOperator(DecoratedOperator, KubernetesPodOperator):",
            "class _KubernetesDecoratedOperator(object):",
            f"does not inherit {BASE_OPERATOR}",
            id="not-an-operator",
        ),
    ],
)
def test_factory_must_name_an_operator_class_statically(source_tree, old, new, message):
    replace_source(source_tree, KUBERNETES_PATH, old, new)
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert errors[0].startswith("@task.kubernetes: ")
    assert message in errors[0]


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


@pytest.mark.parametrize(
    ("path", "message", "count"),
    [
        (POD_PATH, "cannot resolve base class KubernetesPodOperator", 2),
        (DECORATED_OPERATOR.path, "cannot resolve factory call task_decorator_factory", 2),
        (KUBERNETES_PATH, "No such file", 1),
        (STUB_PATH, "No such file", 3),
    ],
)
def test_missing_files_fail_for_each_affected_decorator(source_tree, path, message, count):
    (source_tree / path).unlink()
    errors = collect_errors(source_tree)
    assert len(errors) == count
    assert all(message in error for error in errors)


@pytest.mark.parametrize(("path", "count"), [(POD_PATH, 2), (STUB_PATH, 3)])
def test_syntax_errors_fail_for_each_affected_decorator(source_tree, path, count):
    (source_tree / path).write_text("def broken(:\n", encoding="utf-8")
    errors = collect_errors(source_tree)
    assert len(errors) == count
    assert all(f"{path}:1: invalid syntax" in error for error in errors)


@pytest.mark.parametrize(
    ("path", "old", "new", "message", "count"),
    [
        (
            POD_PATH,
            "class KubernetesPodOperator(",
            "class Other(",
            "cannot resolve base class KubernetesPodOperator",
            2,
        ),
        (
            KUBERNETES_PATH,
            "def kubernetes_task",
            "def other",
            "no top-level class or function kubernetes_task",
            1,
        ),
        (
            KUBERNETES_PATH,
            "class _KubernetesDecoratedOperator(",
            "class Other(",
            "cannot resolve decorated_operator_class _KubernetesDecoratedOperator",
            1,
        ),
        (
            STUB_PATH,
            "class TaskDecoratorCollection:",
            "class Other:",
            "class TaskDecoratorCollection, found 0",
            3,
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
    assert sum(message in error for error in collect_errors(source_tree)) == count


@pytest.mark.parametrize(
    ("path", "extra", "message", "count"),
    [
        (
            STUB_PATH,
            "    def kubernetes(self, *, image=None): ...\n",
            "configuration overload TaskDecoratorCollection.kubernetes, found 2",
            1,
        ),
        (POD_PATH, "    def __init__(self): ...\n", "expected at most one __init__, found 2", 2),
    ],
)
def test_ambiguous_definitions_fail_clearly(source_tree, path, extra, message, count):
    append_source(source_tree, path, extra)
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
        if hook["id"] == "check-task-decorator-arguments"
    )


@pytest.mark.parametrize(
    ("path", "matches"),
    [
        (POD_PATH, True),
        (KUBERNETES_PATH, True),
        (KUBERNETES_CMD_PATH, True),
        (DECORATED_OPERATOR.path, True),
        (BASE_OPERATOR.path, True),
        (STUB_PATH, True),
        (COLLECTION_PATH, True),
        (Path("task-sdk/src/airflow/sdk/__init__.py"), True),
        (Path("task-sdk/src/airflow/sdk/definitions/_internal/logging_mixin.py"), True),
        (PROVIDERS_PATH / "standard/provider.yaml", True),
        (PROVIDERS_PATH / "new/provider.yaml", True),
        (Path("providers/common/compat/src/airflow/providers/common/compat/sdk.py"), True),
        (Path("providers/new/src/airflow/providers/new/decorators/example.py"), True),
        (Path("scripts/ci/prek/check_task_decorator_arguments.py"), True),
        (Path("scripts/tests/ci/prek/test_check_task_decorator_arguments.py"), True),
        (Path(".pre-commit-config.yaml"), True),
        (Path("providers/docker/tests/unit/docker/operators/test_docker.py"), False),
        (Path("task-sdk/tests/task_sdk/bases/test_decorator.py"), False),
        (Path("dev/decorator_walkthrough.py"), False),
    ],
)
def test_hook_file_selection(hook, path, matches):
    assert bool(re.search(hook["files"], path.as_posix())) is matches


def test_hook_runs_without_file_arguments_or_python_only_filter(hook):
    assert hook["language"] == "python"
    assert hook["pass_filenames"] is False
    assert hook["types"] == ["file"]
    assert hook["always_run"] is True


def test_repository_registered_decorators_are_covered_and_synchronized():
    assert collect_errors() == []


def test_registered_decorator_missing_named_declaration_fails(source_tree):
    replace_source(source_tree, STUB_PATH, "def kubernetes(self,", "def old_kubernetes(self,")
    errors = collect_errors(source_tree)
    assert any("@task.kubernetes: registered without a named typing declaration" in error for error in errors)
    assert any("@task.old_kubernetes: typing declaration has no provider registration" in e for e in errors)


def test_removed_registrations_reveal_stale_declarations(source_tree):
    write_source(source_tree, KUBERNETES_METADATA, "task-decorators: []\n")
    errors = collect_errors(source_tree)
    assert len(errors) == 2
    for name in ("kubernetes", "kubernetes_cmd"):
        assert any(f"@task.{name}: typing declaration has no provider registration" in e for e in errors)


def test_renamed_registration_reports_both_old_and_new_names(source_tree):
    replace_source(source_tree, KUBERNETES_METADATA, "name: kubernetes\n", "name: renamed\n")
    errors = collect_errors(source_tree)
    assert len(errors) == 3
    assert sum("@task.renamed:" in e for e in errors) == 2
    assert sum("@task.kubernetes:" in e for e in errors) == 1


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        ("other_task", "no top-level class or function other_task"),
        ("_KubernetesDecoratedOperator", "kubernetes.py:_KubernetesDecoratedOperator: expected a function"),
    ],
)
def test_changed_registered_factory_is_followed(source_tree, factory, message):
    replace_source(source_tree, KUBERNETES_METADATA, ".kubernetes_task", f".{factory}")
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert errors[0].startswith("@task.kubernetes: ") and message in errors[0]


def test_sdk_collection_helpers_are_not_provider_declarations(source_tree):
    append_source(
        source_tree,
        STUB_PATH,
        "    def run_if(self, **kwargs): ...\n"
        "    def skip_if(self, **kwargs): ...\n"
        "    def __getattr__(self, name): ...\n",
    )
    assert collect_errors(source_tree) == []


def test_registration_cannot_shadow_sdk_member(source_tree):
    append_source(source_tree, COLLECTION_PATH, "    kubernetes = staticmethod(existing_helper)\n")
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert "@task.kubernetes: provider registration conflicts with an SDK-owned" in errors[0]


@pytest.mark.parametrize(
    ("metadata", "message"),
    [
        ("{broken:", "while parsing"),
        ("[]", "expected a provider metadata mapping"),
        ("task-decorators: {}", "task-decorators must be a list"),
        ("task-decorators: [invalid]", "entries must be mappings"),
        ("task-decorators: [{name: 3}]", "invalid task decorator name"),
        ("task-decorators: [{name: 'invalid-name'}]", "invalid task decorator name"),
        ("task-decorators: [{name: example}]", "invalid class-name"),
        ("task-decorators: [{name: example, class-name: 3}]", "invalid class-name"),
        ("task-decorators: [{name: example, class-name: single_name}]", "invalid class-name"),
        ("task-decorators: [{name: example, class-name: unsafe/path.factory}]", "invalid class-name"),
        ("task-decorators: [{name: kubernetes, class-name: other.module.factory}]", "duplicate registration"),
    ],
)
def test_invalid_or_duplicate_metadata_fails_clearly(source_tree, metadata, message):
    path = PROVIDERS_PATH / "new/provider.yaml"
    write_source(source_tree, path, metadata)
    _, errors = discover_decorators(source_tree)
    assert len(errors) == 1
    assert str(path) in errors[0] and message in errors[0]


def test_providers_without_decorators_are_allowed(source_tree):
    write_source(source_tree, PROVIDERS_PATH / "new/provider.yaml", "name: Other provider\n")
    registrations, errors = discover_decorators(source_tree)
    assert errors == []
    assert registrations == {
        "kubernetes": Definition(KUBERNETES_PATH, "kubernetes_task"),
        "kubernetes_cmd": Definition(KUBERNETES_CMD_PATH, "kubernetes_cmd_task"),
    }


def test_linked_provider_source_tree_is_not_a_second_inventory(source_tree):
    write_source(
        source_tree,
        PROVIDERS_PATH / "src/airflow/providers/cncf/kubernetes/provider.yaml",
        "task-decorators: [{name: kubernetes, class-name: duplicate.module.factory}]",
    )
    assert collect_errors(source_tree) == []


def test_missing_provider_inventory_fails(source_tree):
    (source_tree / KUBERNETES_METADATA).unlink()
    registrations, errors = discover_decorators(source_tree)
    assert registrations == {}
    assert errors == [f"{PROVIDERS_PATH}: no provider.yaml files found"]


def test_metadata_with_invalid_encoding_fails(source_tree):
    path = PROVIDERS_PATH / "new/provider.yaml"
    write_source(source_tree, path, "")
    (source_tree / path).write_bytes(b"\xff")
    _, errors = discover_decorators(source_tree)
    assert len(errors) == 1
    assert str(path) in errors[0] and "decode" in errors[0]


def test_source_with_invalid_encoding_reports_its_path(source_tree):
    (source_tree / POD_PATH).write_bytes(b"\xff")
    errors = collect_errors(source_tree)
    assert len(errors) == 2
    assert all(str(POD_PATH) in error and "decode" in error for error in errors)


@pytest.mark.parametrize("failure", ["missing-file", "syntax-error", "missing-class"])
def test_unreadable_sdk_collection_fails_coverage(source_tree, failure):
    if failure == "missing-file":
        (source_tree / COLLECTION_PATH).unlink()
    else:
        write_source(source_tree, COLLECTION_PATH, "def broken(:" if failure == "syntax-error" else "")
    errors = collect_errors(source_tree)
    assert len(errors) == 1
    assert str(COLLECTION_PATH) in errors[0]


@pytest.mark.parametrize("failure", [None, "drift", "missing", "ambiguous"])
def test_python_call_alias_must_match_python_declaration(source_tree, failure):
    replace_source(source_tree, KUBERNETES_METADATA, "name: kubernetes\n", "name: python\n")
    replace_source(source_tree, STUB_PATH, "def kubernetes(self,", "def python(self,")
    alias = (
        "    def __call__(self, *, image=None, name=None, namespace=None, arguments=None, "
        "use_dill=False, multiple_outputs=None, **kwargs): ...\n"
    )
    if failure == "drift":
        alias = alias.replace("image=None,", "")
    if failure != "missing":
        append_source(source_tree, STUB_PATH, alias)
    if failure == "ambiguous":
        append_source(source_tree, STUB_PATH, alias)
    errors = collect_errors(source_tree)
    if failure is None:
        assert errors == []
    else:
        assert len(errors) == 1
        assert "@task.__call__" in errors[0] or "TaskDecoratorCollection.__call__" in errors[0]


def test_main_fails_for_new_uncovered_registration(source_tree, capsys):
    write_source(
        source_tree,
        PROVIDERS_PATH / "new/provider.yaml",
        "task-decorators: [{name: example, class-name: new.module.example}]",
    )
    assert main(source_tree) == 1
    output = capsys.readouterr().out
    assert "@task.example: registered without a named typing declaration" in output
    assert "@task.example: providers/new/src/new/module.py: No such file or directory" in output
