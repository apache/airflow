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
import textwrap

import pytest

from airflow.sdk.importers.yaml_importer import YamlDagParseError, parse_documents
from airflow.sdk.importers.yaml_importer.models import (
    CodeTask,
    ConstRef,
    DagDocument,
    OperatorTask,
    TemplateRef,
    TimetableSchedule,
    XComRef,
    XComTarget,
    _Literal,
)

HEAD = "2026-10-30"


def _one(body: str):
    return next(iter(parse_documents(f"compatibility_date: '{HEAD}'\ndag_id: d\n{textwrap.dedent(body)}")))


def _task(task_yaml: str):
    return _one("tasks:\n" + textwrap.indent(textwrap.dedent(task_yaml), "  ")).tasks[0]


# --------------------------------------------------------------------------- value grammar
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


def test_marker_only_as_sole_key():
    t = _task("- {id: t, run: {a: {$x: up, extra: 1}}}")  # two keys -> literal dict
    assert isinstance(t.run["a"], _Literal)
    assert set(t.run["a"].root) == {"$x", "extra"}


def test_nested_marker_inside_literal_resolved():
    t = _task("- {id: t, uses: X, with: {cfg: {url: {$t: '{{ ds }}'}, n: 1}}}")
    inner = t.with_["cfg"].root
    assert isinstance(inner["url"], TemplateRef)
    assert inner["n"].root == 1


# --------------------------------------------------------------------------- task bodies
def test_operator_vs_code_discrimination():
    assert isinstance(_task("- {id: t, uses: a.b.C, with: {x: 1}}"), OperatorTask)
    assert isinstance(_task("- {id: t, run: {}}"), CodeTask)


def test_no_body_is_error():
    with pytest.raises(YamlDagParseError, match="exactly one"):
        _task("- {id: t}")


def test_both_bodies_is_error():
    with pytest.raises(YamlDagParseError, match="exactly one"):
        _task("- {id: t, uses: X, run: {}}")


# --------------------------------------------------------------------------- deferred keys
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


# --------------------------------------------------------------------------- templates / extends
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
    assert isinstance(t, OperatorTask)
    assert t.uses.endswith("S3ToRedshiftOperator")
    assert t.__pydantic_extra__["retries"] == 2  # from retryable
    assert isinstance(t.with_["schema"], _Literal)  # inherited
    assert t.with_["table"].root == "sales"  # own
    assert t.with_["conn_id"].root == "override"  # one-level with-merge, task wins


def test_unknown_template_errors():
    with pytest.raises(YamlDagParseError, match="unknown template"):
        _one("tasks:\n  - {id: t, extends: [ghost], run: {}}")


def test_template_cycle_errors():
    with pytest.raises(YamlDagParseError, match="cycle"):
        _one("templates: {a: {extends: [b]}, b: {extends: [a]}}\ntasks: [{id: t, extends: [a], run: {}}]")


# --------------------------------------------------------------------------- references / edges
def test_unknown_needs_rejected():
    with pytest.raises(YamlDagParseError, match="unknown task"):
        _one("tasks:\n  - {id: t, run: {}, needs: [ghost]}")


def test_unknown_xcom_target_rejected():
    with pytest.raises(YamlDagParseError, match="unknown task"):
        _one("tasks:\n  - {id: t, run: {e: {$x: ghost}}}")


# --------------------------------------------------------------------------- schedule / dag attrs
def test_schedule_forms():
    assert _one("schedule: '0 3 * * *'\ntasks: []").schedule == "0 3 * * *"
    assert _one("schedule: null\ntasks: []").schedule is None
    tt = _one("schedule: {uses: a.b.MyTimetable, with: {n: 1}}\ntasks: []").schedule
    assert isinstance(tt, TimetableSchedule)
    assert tt.uses.endswith("MyTimetable")


def test_dag_attributes_pass_through():
    doc = _one("catchup: false\nmax_active_runs: 3\ntags: [etl]\ntasks: []")
    assert doc.dag_attributes == {"catchup": False, "max_active_runs": 3, "tags": ["etl"]}


# --------------------------------------------------------------------------- compatibility_date
def test_compatibility_date_required():
    with pytest.raises(YamlDagParseError, match="compatibility_date"):
        list(parse_documents("dag_id: d\ntasks: []"))


def test_unknown_date_warns_and_falls_back():
    with pytest.warns(UserWarning, match="not a known version"):
        doc = next(iter(parse_documents("compatibility_date: '2099-01-01'\ndag_id: d\ntasks: []")))
    assert doc.dag_id == "d"


# --------------------------------------------------------------------------- multi-document
def test_accepts_a_file_like_stream():
    stream = io.StringIO(f"compatibility_date: '{HEAD}'\ndag_id: d\ntasks: [{{id: t, run: {{}}}}]")
    docs = list(parse_documents(stream))
    assert [d.dag_id for d in docs] == ["d"]


def test_parse_is_lazy_errors_surface_on_iteration():
    # Building the iterator does not parse; the error only surfaces when consumed.
    gen = parse_documents("dag_id: d\ntasks: []")  # missing compatibility_date
    with pytest.raises(YamlDagParseError, match="compatibility_date"):
        next(iter(gen))


def test_multiple_documents():
    docs = parse_documents(
        f"compatibility_date: '{HEAD}'\ndag_id: a\ntasks: []\n---\n"
        f"compatibility_date: '{HEAD}'\ndag_id: b\ntasks: []\n"
    )
    assert [d.dag_id for d in docs] == ["a", "b"]


# --------------------------------------------------------------------------- schema
def test_model_json_schema_describes_markers():
    # The models are the schema source of truth (the prek dump script decorates + snapshots
    # them). Assert the generated schema shape here; the snapshot itself is enforced by the
    # generate-yaml-importer-schema-snapshot prek hook.
    sch = DagDocument.model_json_schema(by_alias=True)
    assert set(sch["properties"]) >= {"compatibility_date", "dag_id", "schedule", "tasks", "templates"}
    assert {"XComRef", "TemplateRef", "ConstRef", "OperatorTask", "CodeTask"} <= set(sch["$defs"])


# --------------------------------------------------------------------------- integration
def test_parses_example_fixture():
    import pathlib

    fixture = pathlib.Path(__file__).parent / "example_dag.yaml"
    with fixture.open(encoding="utf-8") as fh:
        docs = list(parse_documents(fh, source=str(fixture)))
    assert [d.dag_id for d in docs] == ["retail_daily_sales", "events_pipeline"]

    d1 = {t.id_: t for t in docs[0].tasks}
    load = d1["load_sales"]
    assert isinstance(load, OperatorTask)
    assert load.uses.endswith("S3ToRedshiftOperator")
    assert load.__pydantic_extra__["retries"] == 2  # via redshift_load -> retryable
    assert isinstance(load.with_["schema"], TemplateRef)  # inherited $t
    assert isinstance(load.with_["s3_key"], TemplateRef)  # own $t
    assert load.needs == ["wait_for_export"]
    assert docs[0].dag_attributes["catchup"] is False

    d2 = {t.id_: t for t in docs[1].tasks}
    assert isinstance(d2["extract"], CodeTask)
    assert d2["extract"].run == {}
    assert d2["extract"].queue == "extract-workers"
    assert isinstance(d2["transform"].run["events"], XComRef)  # {$x: extract} edge
    assert d2["load"].needs == ["transform"]


# --------------------------------------------------------------------------- compatibility_date migration
# A synthetic two-version bundle with a real forward converter, to exercise the migration
# machinery (the real bundle has a single version, so nothing migrates there).
from cadwyn import (  # noqa: E402
    HeadVersion,
    Version,
    VersionBundle,
    VersionChange,
    convert_request_to_next_version_for,
)

from airflow.sdk.importers.yaml_importer import migrator  # noqa: E402


class _RenameOwnerAttr(VersionChange):
    "test-only: 2026-10-30 spelled the attribute `owner_old`; head renamed it to `owner`."

    description = __doc__
    instructions_to_migrate_to_previous_version = ()

    @convert_request_to_next_version_for(DagDocument)  # type: ignore[arg-type]
    def _move(request):
        if "owner_old" in request.body:
            request.body["owner"] = request.body.pop("owner_old")


_SYNTH_BUNDLE = VersionBundle(HeadVersion(), Version("2027-06-01", _RenameOwnerAttr), Version("2026-10-30"))


def _migrate(m, date, **extra):
    body = {"compatibility_date": date, "dag_id": "d", "tasks": [], **extra}
    return m.resolve_and_migrate(body, source="x")


def test_resolution_picks_the_newest_applicable_ruleset():
    # Observed through the converter: it runs whenever the resolved source is older than head.
    m = migrator.DagDocumentMigrator(_SYNTH_BUNDLE)
    assert _migrate(m, "2026-10-30", owner_old="t")["owner"] == "t"  # oldest, exact
    assert _migrate(m, "2027-03-01", owner_old="t")["owner"] == "t"  # in-between -> oldest ruleset
    with pytest.warns(UserWarning, match="not a known version"):  # ancient -> earliest ruleset (warns)
        assert _migrate(m, "2020-01-01", owner_old="t")["owner"] == "t"
    # head-authored: resolved source == head, so no converter runs
    at_head = _migrate(m, "2027-06-01", owner_old="t")
    assert at_head.get("owner_old") == "t"
    assert "owner" not in at_head


def test_warns_only_for_out_of_range_dates(recwarn):
    m = migrator.DagDocumentMigrator(_SYNTH_BUNDLE)
    _migrate(m, "2026-10-30")  # exact
    _migrate(m, "2027-03-01")  # in-between
    assert not recwarn.list, "exact/in-between dates must not warn"
    with pytest.warns(UserWarning, match="not a known version"):
        _migrate(m, "2099-01-01")  # future
    with pytest.warns(UserWarning, match="not a known version"):
        _migrate(m, "2020-01-01")  # older than the earliest published version


def test_migrate_is_noop_at_head_for_the_real_bundle():
    body = {"compatibility_date": HEAD, "dag_id": "d", "tasks": []}
    assert migrator.get_migrator().resolve_and_migrate(body, source="x") is body  # one version -> no copy
