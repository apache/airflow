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
"""Unit tests for the YAML DAG format parser (airflow.sdk.importers.yaml_importer)."""

from __future__ import annotations

import io
import json
import pathlib
import textwrap

import jsonschema
import pytest
import yaml

from airflow.sdk.importers import yaml_importer
from airflow.sdk.importers.yaml_importer import YamlDagParseError, parse_documents
from airflow.sdk.importers.yaml_importer.models import (
    ConstRef,
    DagDocument,
    Task,
    TemplateRef,
    TimetableSchedule,
    XComRef,
    XComTarget,
    _Literal,
)

SCHEMA_PATH = pathlib.Path(yaml_importer.__file__).with_name("schema.json")

HEAD = "2026-10-30"
HEAD_SCHEMA = f"https://airflow.apache.org/schemas/dag/{HEAD}.json"


def _one(body: str):
    return next(iter(parse_documents(f"$schema: {HEAD_SCHEMA}\ndag_id: d\n{textwrap.dedent(body)}")))


def _task(task_yaml: str):
    return _one("tasks:\n" + textwrap.indent(textwrap.dedent(task_yaml), "  ")).tasks[0]


def test_literal_by_default():
    t = _task("- {id: t, run: {region: 'eu', n: 5, flag: true}}")
    assert isinstance(t.run["region"], _Literal)
    assert t.run["region"].root == "eu"
    assert t.run["n"].root == 5
    assert t.run["flag"].root is True


def test_xcom_short_long_and_keyed():
    doc = _one(
        "tasks:\n  - {id: up, run: {}}\n  - {id: t, run: {a: {$x: up}, b: {$xcom: up}, c: {$x: {task: up, key: rows}}}}"
    )
    t = {x.id_: x for x in doc.tasks}["t"]
    assert isinstance(t.run["a"], XComRef)
    assert t.run["a"].target == "up"
    assert isinstance(t.run["b"], XComRef)
    assert t.run["b"].target == "up"
    assert isinstance(t.run["c"].target, XComTarget)
    assert t.run["c"].target.key == "rows"


def test_template_marker():
    t = _task("- {id: t, uses: X, with: {k: {$t: 'v-{{ ds }}'}, k2: {$template: 'x'}}}")
    assert isinstance(t.with_["k"], TemplateRef)
    assert "{{ ds }}" in t.with_["k"].source
    assert isinstance(t.with_["k2"], TemplateRef)


def test_const_is_verbatim_not_recursed():
    t = _task("- {id: t, run: {a: {$const: {$x: not-a-ref}}}}")
    assert isinstance(t.run["a"], ConstRef)
    assert t.run["a"].value == {"$x": "not-a-ref"}  # inner marker NOT interpreted


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("{$x: up, extra: 1}", id="marker-key-plus-extra"),
        pytest.param("{$xcomm: up}", id="unknown-marker-typo"),
        pytest.param("{$tempalte: x}", id="unknown-template-typo"),  # codespell:ignore tempalte
        pytest.param("{cfg: {$weird: 1}}", id="dollar-key-nested-in-literal"),
    ],
)
def test_reserved_dollar_keys_rejected(value):
    # A '$'-prefixed key that is not a recognised sole-key marker is rejected, not silently literal.
    with pytest.raises(YamlDagParseError, match="reserved"):
        _task(f"- {{id: t, run: {{a: {value}}}}}")


def test_nested_marker_inside_literal_resolved():
    t = _task("- {id: t, uses: X, with: {cfg: {url: {$t: '{{ ds }}'}, n: 1}}}")
    inner = t.with_["cfg"].root
    assert isinstance(inner["url"], TemplateRef)
    assert inner["n"].root == 1


def test_operator_discrimination():
    op = _task("- {id: t, uses: a.b.C, with: {x: 1}}")
    assert isinstance(op, Task)
    assert op.uses == "a.b.C"
    assert op.run is None


def test_code_discrimination():
    code = _task("- {id: t, run: {}}")
    assert isinstance(code, Task)
    assert code.run == {}
    assert code.uses is None


def test_no_body_is_error():
    with pytest.raises(YamlDagParseError, match="exactly one"):
        _task("- {id: t}")


def test_both_bodies_is_error():
    with pytest.raises(YamlDagParseError, match="exactly one"):
        _task("- {id: t, uses: X, run: {}}")


def test_python_field_names_not_accepted_as_aliases():
    with pytest.raises(YamlDagParseError):
        _one("tasks:\n  - {id_: t, run: {}}")  # 'id_' is not 'id' -> id missing
    t = _task("- {id: t, uses: X, with_: {k: 1}}")
    assert t.with_ == {}  # 'with_' did not populate the 'with' field
    assert t.model_extra.get("with_") == {"k": 1}  # it is just a pass-through extra


def test_default_args_rejected():
    with pytest.raises(YamlDagParseError, match="default_args"):
        _one("default_args: {retries: 1}\ntasks: []")


def test_callbacks_rejected():
    with pytest.raises(YamlDagParseError, match="callback"):
        _one("on_failure_callback: cb\ntasks: []")
    with pytest.raises(YamlDagParseError, match="callback"):
        _task("- {id: t, run: {}, on_success_callback: cb}")


def test_inlets_outlets_rejected():
    with pytest.raises(YamlDagParseError, match="not yet implemented"):
        _task("- {id: t, run: {}, outlets: [a]}")


def test_extends_merges_shallow_task_wins():
    doc = _one(
        """
        templates:
          retryable: {retries: 2, retry_delay: '5m'}
          load:
            extends: [retryable]
            uses: a.b.S3ToRedshiftOperator
            with: {schema: public, conn_id: warehouse}
        tasks:
          - id: t
            extends: [load]
            with: {table: sales, conn_id: override}
        """
    )
    t = doc.tasks[0]
    assert isinstance(t, Task)
    assert t.uses.endswith("S3ToRedshiftOperator")
    assert t.model_extra["retries"] == 2  # from retryable
    assert isinstance(t.with_["schema"], _Literal)  # inherited
    assert t.with_["table"].root == "sales"  # own
    assert t.with_["conn_id"].root == "override"  # one-level with-merge, task wins


def test_unknown_template_errors():
    with pytest.raises(YamlDagParseError, match="unknown template"):
        _one("tasks:\n  - {id: t, extends: [ghost], run: {}}")


def test_template_cycle_errors():
    with pytest.raises(YamlDagParseError, match="cycle"):
        _one("templates: {a: {extends: [b]}, b: {extends: [a]}}\ntasks: [{id: t, extends: [a], run: {}}]")


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("tasks:\n  - {id: t, run: {}, extends: [[base]]}", id="task-extends-nested-list"),
        pytest.param("tasks:\n  - {id: t, run: {}, extends: [{base: 1}]}", id="task-extends-dict-element"),
        pytest.param("tasks:\n  - {id: t, run: {}, extends: base}", id="task-extends-bare-string"),
    ],
)
def test_task_extends_must_be_list_of_strings(body):
    # Non-string entries would TypeError on the template lookup; a bare string would iterate by
    # character. Both are rejected as a parse error instead.
    with pytest.raises(YamlDagParseError, match="extends"):
        _one(body)


def test_template_extends_must_be_list_of_strings():
    # A template's own 'extends' given as a bare string must not be iterated character by character.
    with pytest.raises(YamlDagParseError, match="extends"):
        _one(
            "templates: {base: {retries: 1}, child: {extends: base}}\ntasks: [{id: t, extends: [child], run: {}}]"
        )


def test_unknown_needs_rejected():
    with pytest.raises(YamlDagParseError, match="unknown task"):
        _one("tasks:\n  - {id: t, run: {}, needs: [ghost]}")


@pytest.mark.parametrize(
    "task",
    [
        pytest.param("{id: t, run: {e: {$x: ghost}}}", id="run-top-level"),
        pytest.param("{id: t, run: {e: [{$x: ghost}]}}", id="run-in-list"),
        pytest.param("{id: t, run: {e: {nested: {$x: ghost}}}}", id="run-nested-dict"),
        pytest.param("{id: t, uses: X, with: {a: {$x: ghost}}}", id="with-on-operator"),
    ],
)
def test_unknown_xcom_target_rejected(task):
    # XCom refs must be found at any depth inside with/run (the list and nested-dict branches too).
    with pytest.raises(YamlDagParseError, match="unknown task"):
        _one(f"tasks:\n  - {task}")


def test_duplicate_task_ids_rejected():
    with pytest.raises(YamlDagParseError, match="duplicate task id"):
        _one("tasks:\n  - {id: a, run: {}}\n  - {id: a, uses: x.Y}")


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("tasks:\n  - {id: a, run: {}, needs: [a]}", id="self-needs"),
        pytest.param("tasks:\n  - {id: a, run: {x: {$x: a}}}", id="self-xcom"),
    ],
)
def test_self_dependency_rejected(body):
    with pytest.raises(YamlDagParseError, match="depends on itself"):
        _one(body)


def test_schedule_forms():
    assert _one("schedule: '0 3 * * *'\ntasks: []").schedule == "0 3 * * *"
    assert _one("schedule: null\ntasks: []").schedule is None
    tt = _one("schedule: {uses: a.b.MyTimetable, with: {n: 1}}\ntasks: []").schedule
    assert isinstance(tt, TimetableSchedule)
    assert tt.uses.endswith("MyTimetable")


def test_extra_top_level_keys_pass_through():
    # Unknown top-level keys are Dag-level arguments: accepted and surfaced via pydantic extras.
    doc = _one("catchup: false\nmax_active_runs: 3\ntags: [etl]\ntasks: []")
    assert doc.model_extra == {"catchup": False, "max_active_runs": 3, "tags": ["etl"]}


def test_schema_required():
    with pytest.raises(YamlDagParseError, match=r"\$schema"):
        list(parse_documents("dag_id: d\ntasks: []"))


def test_unquoted_date_schema_is_coerced_to_url():
    # An unquoted date loads as datetime.date; accept it and normalise to the canonical schema URL.
    doc = next(iter(parse_documents("$schema: 2026-10-30\ndag_id: d\ntasks: []")))
    assert doc.schema_ == HEAD_SCHEMA


@pytest.mark.parametrize(
    "schema_value",
    [
        pytest.param("null", id="null"),
        pytest.param('""', id="empty-string"),
        pytest.param("false", id="false"),
        pytest.param("123", id="int"),
        pytest.param("[a, b]", id="list"),
    ],
)
def test_schema_must_be_a_non_empty_string_or_date(schema_value):
    # A present-but-falsy or wrong-typed $schema is reported as such, not as "missing".
    with pytest.raises(YamlDagParseError, match="must be a non-empty string or a date"):
        list(parse_documents(f"$schema: {schema_value}\ndag_id: d\ntasks: []"))


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("tasks: []\ntasks: []", id="dup-tasks-top-level"),
        pytest.param("tasks:\n  - {id: a, uses: X, with: {x: 1}, with: {y: 2}}", id="dup-with-on-task"),
        pytest.param("tasks:\n  - {id: a, run: {}, needs: [b], needs: [c]}", id="dup-needs-on-task"),
    ],
)
def test_duplicate_keys_rejected(body):
    # PyYAML keeps the last value for a repeated key; a hand-edited format should reject it instead
    # of silently dropping the first. The loader raises a ConstructorError (a YAMLError).
    with pytest.raises(YamlDagParseError, match="duplicate key"):
        _one(body)


def test_unknown_schema_version_is_rejected():
    # Strict exact match (mirroring the supervisor migrator): any version that is not published --
    # newer, ancient, or unreadable -- is rejected rather than guessed.
    base = "https://airflow.apache.org/schemas/dag/{}.json"
    for url in (base.format("2099-01-01"), base.format("2000-01-01"), "nonsense"):
        with pytest.raises(YamlDagParseError, match="not valid"):
            list(parse_documents(f"$schema: {url}\ndag_id: d\ntasks: []"))


def test_accepts_a_file_like_stream():
    stream = io.StringIO(f"$schema: {HEAD_SCHEMA}\ndag_id: d\ntasks: [{{id: t, run: {{}}}}]")
    docs = list(parse_documents(stream))
    assert [d.dag_id for d in docs] == ["d"]


def test_parse_is_lazy_errors_surface_on_iteration():
    # Building the iterator does not parse; the error only surfaces when consumed.
    gen = parse_documents("dag_id: d\ntasks: []")  # missing $schema
    with pytest.raises(YamlDagParseError, match=r"\$schema"):
        next(iter(gen))


def test_multiple_documents():
    docs = parse_documents(
        f"$schema: {HEAD_SCHEMA}\ndag_id: a\ntasks: []\n---\n$schema: {HEAD_SCHEMA}\ndag_id: b\ntasks: []\n"
    )
    assert [d.dag_id for d in docs] == ["a", "b"]


def test_non_mapping_document_rejected():
    with pytest.raises(YamlDagParseError, match="must be a mapping"):
        list(parse_documents("- a\n- b"))


def test_invalid_yaml_rejected():
    with pytest.raises(YamlDagParseError, match="invalid YAML"):
        list(parse_documents("key: [unclosed"))


def test_error_label_points_at_offending_document():
    # A failure in a later document of a multi-doc stream is labelled with its position.
    good = f"$schema: {HEAD_SCHEMA}\ndag_id: a\ntasks: []"
    bad = "dag_id: b\ntasks: []"  # missing $schema -> fails, and it is doc index 1
    with pytest.raises(YamlDagParseError, match=r"\[doc 1\]"):
        list(parse_documents(f"{good}\n---\n{bad}"))


def test_model_json_schema_describes_markers():
    # The models are the schema source of truth (the prek dump script decorates + snapshots
    # them). Assert the generated schema shape here; the snapshot itself is enforced by the
    # generate-yaml-importer-schema-snapshot prek hook.
    sch = DagDocument.model_json_schema(by_alias=True)
    assert set(sch["properties"]) >= {"$schema", "dag_id", "schedule", "tasks", "templates"}
    assert {"XComRef", "TemplateRef", "ConstRef", "Task"} <= set(sch["$defs"])


def test_parses_example_fixture():
    fixture = pathlib.Path(__file__).parent / "example_dag.yaml"
    with fixture.open(encoding="utf-8") as fh:
        docs = list(parse_documents(fh, source=str(fixture)))
    assert [d.dag_id for d in docs] == ["retail_daily_sales", "events_pipeline"]

    d1 = {t.id_: t for t in docs[0].tasks}
    load = d1["load_sales"]
    assert isinstance(load, Task)
    assert load.uses.endswith("S3ToRedshiftOperator")
    assert load.model_extra["retries"] == 2  # via redshift_load -> retryable
    assert isinstance(load.with_["schema"], TemplateRef)  # inherited $t
    assert isinstance(load.with_["s3_key"], TemplateRef)  # own $t
    assert load.needs == ["wait_for_export"]
    assert docs[0].model_extra["catchup"] is False

    d2 = {t.id_: t for t in docs[1].tasks}
    assert isinstance(d2["extract"], Task)
    assert d2["extract"].run == {}
    assert d2["extract"].queue == "extract-workers"
    assert isinstance(d2["transform"].run["events"], XComRef)  # {$x: extract} edge
    assert d2["load"].needs == ["transform"]


def test_example_fixture_conforms_to_published_schema():
    # Separately validate the fixture against the JSON Schema. Not all Pydantic
    # features survive the export.
    validator = jsonschema.Draft202012Validator(json.loads(SCHEMA_PATH.read_text()))
    fixture = pathlib.Path(__file__).parent / "example_dag.yaml"
    documents = [d for d in yaml.safe_load_all(fixture.read_text()) if d]
    assert documents
    for document in documents:
        assert list(validator.iter_errors(document)) == []


@pytest.mark.parametrize(
    "bad_task",
    [
        pytest.param({"id": "t"}, id="no-body-no-extends"),
        pytest.param({"id": "t", "uses": "a.b.C", "run": {}}, id="both-bodies"),
    ],
)
def test_published_schema_rejects_invalid_tasks(bad_task):
    validator = jsonschema.Draft202012Validator(json.loads(SCHEMA_PATH.read_text()))
    document = {"$schema": HEAD_SCHEMA, "dag_id": "d", "tasks": [bad_task]}
    assert list(validator.iter_errors(document))
