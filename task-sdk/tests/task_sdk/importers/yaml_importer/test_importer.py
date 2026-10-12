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

import inspect
import textwrap
from types import SimpleNamespace

import pytest

from airflow.sdk import BaseOperator, XComArg
from airflow.sdk.definitions._internal.templater import LiteralValue
from airflow.sdk.importers.base import FilesystemDagDefinition
from airflow.sdk.importers.yaml_importer.importer import YamlDagImporter

HEAD_SCHEMA = "https://airflow.apache.org/schemas/dag/2026-10-30.json"


class _RecordingOperator(BaseOperator):
    """A trivial operator with one templated field (``payload``) and one plain field (``note``)."""

    template_fields = ("payload",)

    def __init__(self, *, payload=None, note=None, **kwargs):
        super().__init__(**kwargs)
        self.payload = payload
        self.note = note

    def execute(self, context):
        return self.payload


OP = f"{__name__}.{_RecordingOperator.__name__}"


@pytest.fixture
def mock_bundle(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(name="test_bundle", path=bundle_dir)


def _def(bundle, name: str, body: str) -> FilesystemDagDefinition:
    path = bundle.path / name
    path.write_text(textwrap.dedent(body))
    return FilesystemDagDefinition(path=path)


def _doc(tasks: str, *, dag_id: str = "d", extra: str = "") -> str:
    return f"$schema: {HEAD_SCHEMA}\ndag_id: {dag_id}\n{extra}tasks:\n{textwrap.indent(textwrap.dedent(tasks), '  ')}"


def test_can_handle():
    importer = YamlDagImporter()
    assert importer.can_handle("dag.yaml")
    assert importer.can_handle("dag.yml")
    assert not importer.can_handle("dag.py")
    assert not importer.can_handle("dag.json")


def test_get_source_code(mock_bundle):
    definition = _def(mock_bundle, "d.yaml", f"$schema: {HEAD_SCHEMA}\ndag_id: d\ntasks: []\n")
    src = YamlDagImporter().get_source_code(definition)
    assert src.language == "yaml"
    assert "$schema" in src.source_code


def test_might_contain_dag(mock_bundle):
    importer = YamlDagImporter()
    has = _def(mock_bundle, "has.yaml", f"$schema: {HEAD_SCHEMA}\ndag_id: d\ntasks: []\n")
    nope = _def(mock_bundle, "nope.yaml", "just: config\n")
    assert importer.might_contain_dag(has, safe_mode=True)
    assert not importer.might_contain_dag(nope, safe_mode=True)
    assert importer.might_contain_dag(nope, safe_mode=False)  # sniff skipped


def test_list_dag_definitions_skips_non_dag_yaml(mock_bundle):
    _def(mock_bundle, "dag.yaml", f"$schema: {HEAD_SCHEMA}\ndag_id: d\ntasks: []\n")
    _def(mock_bundle, "config.yaml", "just: config\n")  # no $schema -> skipped
    _def(mock_bundle, "notes.txt", "hello")  # wrong extension
    names = {d.path.name for d in YamlDagImporter().list_dag_definitions(mock_bundle)}
    assert names == {"dag.yaml"}


def test_import_builds_operator_dag(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(
            f"""
            - {{id: a, uses: {OP}, with: {{payload: hi}}}}
            - {{id: b, uses: {OP}, with: {{payload: {{$x: a}}}}}}
            """,
            dag_id="pipeline",
            extra="catchup: false\n",
        ),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    (dag,) = result.dags
    assert dag.dag_id == "pipeline"
    assert set(dag.task_ids) == {"a", "b"}
    assert dag.catchup is False  # Dag-level attribute passed through
    assert dag.bundle_name == "test_bundle"
    a, b = dag.get_task("a"), dag.get_task("b")
    assert a.payload == "hi"  # literal
    assert isinstance(b.payload, XComArg)  # $x -> XComArg
    assert b.upstream_task_ids == {"a"}  # XCom wired the dependency


def test_import_wires_needs(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP}, with: {{payload: x}}}}
            - {{id: b, uses: {OP}, with: {{payload: y}}, needs: [a]}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].get_task("b").upstream_task_ids == {"a"}


def test_import_bodyless_task_becomes_empty_operator(mock_bundle):
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: a}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert type(result.dags[0].get_task("a")).__name__ == "EmptyOperator"


def test_import_bodyless_task_with_args_errors(mock_bundle):
    # A body-less task becomes an EmptyOperator, which rejects the extra 'with' arguments
    # (BaseOperator raises on unknown kwargs) -- so it fails to build rather than silently dropping.
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: a, with: {x: 1}}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert result.errors


def test_import_schedule_cron(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP}, with: {{payload: x}}}}", extra="schedule: '0 3 * * *'\n"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].schedule == "0 3 * * *"


def test_import_code_task_builds_stub_operator(mock_bundle):
    # A code task (run:) builds the stub operator Airflow understands (@task.stub). Its body is a
    # registered SDK task handler; the run mapping binds as that handler's arguments (op_kwargs),
    # and $x inside it wires the dependency natively.
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP}, with: {{payload: hi}}}}
            - {{id: b, run: {{value: {{$x: a}}, label: done}}, queue: handlers}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    b = result.dags[0].get_task("b")
    assert type(b).__name__ == "_StubOperator"
    assert getattr(b, "is_stub", False) is True  # the real stub operator Airflow serializes
    assert b.queue == "handlers"  # task-level attribute passed through
    assert isinstance(b.op_kwargs["value"], XComArg)  # $x -> XComArg, bound as a handler argument
    # op_kwargs is a template field, so the literal is protected to stay literal when rendered.
    assert isinstance(b.op_kwargs["label"], LiteralValue)
    assert b.op_kwargs["label"].value == "done"
    assert b.upstream_task_ids == {"a"}  # $x wired the dependency
    # The stub is fed a callable whose ordered parameters are the run argument names, so core
    # serialization can build the handler arg-binding spec (AIP-108) from a real signature.
    assert list(inspect.signature(b.python_callable).parameters) == ["value", "label"]


def test_import_mixed_container_keeps_literal_siblings_literal(mock_bundle):
    # When a container field renders (it holds a $x), its literal siblings must stay literal rather
    # than being rendered as Jinja. The importer wraps them in LiteralValue; only $x/$t render.
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: up, uses: {OP}, with: {{payload: x}}}}
            - {{id: t, uses: {OP}, with: {{payload: {{a: {{$x: up}}, b: '{{{{ not a template }}}}'}}}}}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    payload = result.dags[0].get_task("t").payload
    assert isinstance(payload["a"], XComArg)  # $x still resolves
    assert isinstance(payload["b"], LiteralValue)  # literal sibling protected from rendering
    assert payload["b"].value == "{{ not a template }}"


def test_import_code_task_rejects_non_identifier_arg(mock_bundle):
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: a, run: {'bad-name': 1}}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("not a valid identifier" in e.message for e in result.errors)


@pytest.mark.parametrize("marker", ["{$x: a}", "{$t: 'v'}", "{$f: q.sql}"])
def test_import_marker_on_non_template_field_errors(mock_bundle, marker):
    # A marker only works where the operator class already templates. ``note`` is not in
    # _RecordingOperator.template_fields, so any $f/$t/$x on it is an error -- the importer never
    # adds a field to template_fields to make a marker work.
    (mock_bundle.path / "q.sql").write_text("SELECT 1")  # exists, so this is not a missing-file error
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP}, with: {{payload: hi}}}}
            - {{id: b, uses: {OP}, with: {{payload: ok, note: {marker}}}}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("only allowed on template_fields" in e.message for e in result.errors)


def test_import_literal_by_default(mock_bundle):
    # A bare Jinja-looking string on a field the operator class *does* template is NOT rendered:
    # literal by default, and the instance's template_fields is emptied of it.
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP}, with: {{payload: '{{{{ ds }}}}'}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    a = result.dags[0].get_task("a")
    assert a.payload == "{{ ds }}"  # kept literal
    assert tuple(a.template_fields) == ()  # not rendered
    assert tuple(a.template_ext) == ()  # and the suffix-based file-load foot-gun is closed


def test_import_template_marker_sets_template_fields(mock_bundle):
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP}, with: {{payload: {{$t: '{{{{ ds }}}}'}}}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    a = result.dags[0].get_task("a")
    assert a.payload == "{{ ds }}"
    assert set(a.template_fields) == {"payload"}  # $t opts the field into rendering


def test_import_file_ref_reads_verbatim(mock_bundle):
    (mock_bundle.path / "query.sql").write_text("SELECT 1")
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP}, with: {{payload: {{$f: query.sql}}}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    a = result.dags[0].get_task("a")
    assert a.payload == "SELECT 1"  # file contents, read at build time
    assert tuple(a.template_fields) == ()  # $f is verbatim, not a template


@pytest.mark.parametrize("bad_path", ["../../etc/passwd", "/etc/passwd"])
def test_import_file_ref_outside_dag_dir_is_rejected(mock_bundle, bad_path):
    # $f resolves relative to the Dag file's directory, like a Python Dag's template search path;
    # a path that escapes it (traversal or absolute) is rejected rather than read.
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP}, with: {{payload: {{$f: '{bad_path}'}}}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("resolves outside" in e.message for e in result.errors)


def test_import_template_file_reads_then_templates(mock_bundle):
    (mock_bundle.path / "query.sql").write_text("SELECT {{ ds }}")
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP}, with: {{payload: {{$t: {{$f: query.sql}}}}}}}}"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    a = result.dags[0].get_task("a")
    assert a.payload == "SELECT {{ ds }}"  # file contents, to be rendered at run time
    assert set(a.template_fields) == {"payload"}  # $t over $f is a template file


def test_import_templated_filename_is_reserved(mock_bundle):
    # $f wrapping $t (a templated filename) needs run-time file resolution; the parser reserves it,
    # so the whole file fails to parse.
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP}, with: {{payload: {{$f: {{$t: '{{{{ ds }}}}.sql'}}}}}}}}"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert result.errors
    assert result.errors[0].error_type == "parse"


def test_import_parse_error(mock_bundle):
    definition = _def(mock_bundle, "dag.yaml", "dag_id: d\ntasks: []\n")  # missing $schema
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert result.errors
    assert result.errors[0].error_type == "parse"


def test_import_unknown_operator_errors(mock_bundle):
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: a, uses: does.not.Exist, with: {}}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert result.errors


def test_import_dependency_cycle_errors(mock_bundle):
    # The parser allows a cycle (full cycle detection is the builder's job); the build rejects it.
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP}, with: {{payload: x}}, needs: [b]}}
            - {{id: b, uses: {OP}, with: {{payload: y}}, needs: [a]}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("cycle" in e.message for e in result.errors)
