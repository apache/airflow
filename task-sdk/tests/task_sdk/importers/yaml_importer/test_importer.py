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

import datetime
import inspect
import textwrap
import zipfile
from types import SimpleNamespace

import pendulum
import pytest
from dateutil.relativedelta import relativedelta

from airflow.sdk import BaseOperator, XComArg
from airflow.sdk.definitions._internal.templater import LiteralValue
from airflow.sdk.definitions.timetables.simple import OnceTimetable
from airflow.sdk.importers import ZipMemberDagDefinition
from airflow.sdk.importers.base import FilesystemDagDefinition
from airflow.sdk.importers.yaml_importer.importer import YamlDagImporter

HEAD_SCHEMA = "https://airflow.apache.org/schemas/dag/2026-10-30.json"


class _RecordingOperator(BaseOperator):
    """A trivial operator with one templated field (``payload``) and one plain field (``note``)."""

    template_fields = ("payload",)
    template_ext = (".sql",)  # non-empty so the importer emptying it to () is observable, not vacuous

    def __init__(self, *, payload=None, note=None, **kwargs):
        super().__init__(**kwargs)
        self.payload = payload
        self.note = note

    def execute(self, context):
        return self.payload


OP = f"{__name__}.{_RecordingOperator.__name__}"


class _DefaultTemplateOperator(BaseOperator):
    """Templates ``preset`` by default (set in __init__, not by the author) plus author-set ``payload``."""

    template_fields = ("payload", "preset")

    def __init__(self, *, payload=None, preset="{{ ds }}", **kwargs):
        super().__init__(**kwargs)
        self.payload = payload
        self.preset = preset

    def execute(self, context):
        return self.payload


OP_DEFAULT_TEMPLATE = f"{__name__}.{_DefaultTemplateOperator.__name__}"


class _TemporalOperator(BaseOperator):
    """An operator with a datetime-typed argument (``cutoff``)."""

    def __init__(self, *, cutoff: datetime.datetime | None = None, **kwargs):
        super().__init__(**kwargs)
        self.cutoff = cutoff


OP_TEMPORAL = f"{__name__}.{_TemporalOperator.__name__}"


class _DateOperator(BaseOperator):
    """An operator with a date-typed argument (``day``)."""

    def __init__(self, *, day: datetime.date | None = None, **kwargs):
        super().__init__(**kwargs)
        self.day = day


OP_DATE = f"{__name__}.{_DateOperator.__name__}"


class _PendulumOperator(BaseOperator):
    """An operator with pendulum-typed temporal arguments."""

    def __init__(self, *, when: pendulum.DateTime | None = None, day: pendulum.Date | None = None, **kwargs):
        super().__init__(**kwargs)
        self.when = when
        self.day = day


OP_PENDULUM = f"{__name__}.{_PendulumOperator.__name__}"


class _PeriodOperator(BaseOperator):
    """An operator with time-period-typed arguments."""

    def __init__(
        self,
        *,
        gap: datetime.timedelta | None = None,
        window: relativedelta | None = None,
        either: datetime.timedelta | relativedelta | None = None,
        span: pendulum.Duration | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.gap = gap
        self.window = window
        self.either = either
        self.span = span


OP_PERIOD = f"{__name__}.{_PeriodOperator.__name__}"


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


_DRAFT_SCHEMA = "https://json-schema.org/draft/2020-12/schema"


@pytest.mark.parametrize(
    ("case", "body", "expected"),
    [
        # Recognized: $schema declares the canonical Dag URL, in any of its spellings.
        ("yaml", f"$schema: {HEAD_SCHEMA}\ndag_id: d\ntasks: []\n", True),
        ("yaml_quoted_value", f'$schema: "{HEAD_SCHEMA}"\n', True),
        ("yaml_quoted_key", f'"$schema": {HEAD_SCHEMA}\n', True),
        ("yaml_space_before_colon", f"$schema : {HEAD_SCHEMA}\n", True),
        ("yaml_not_first_line", f"dag_id: d\n$schema: {HEAD_SCHEMA}\n", True),
        ("json", f'{{"$schema": "{HEAD_SCHEMA}", "dag_id": "d", "tasks": []}}', True),
        # Skipped: no $schema declaration naming the canonical Dag URL.
        ("no_schema", "just: config\n", False),
        ("other_schema_url", f"$schema: {_DRAFT_SCHEMA}\n", False),
        ("url_only_no_schema", f"note: see {HEAD_SCHEMA}\n", False),
        # The URL sits in another field while $schema is something else -- the published snapshot's shape.
        ("url_in_other_field", f"$schema: {_DRAFT_SCHEMA}\n$id: {HEAD_SCHEMA}\n", False),
        # The word "$schema" appears in prose, not as a declaration whose value is the URL.
        ("schema_word_in_prose", f"description: set $schema at {HEAD_SCHEMA}\n", False),
        # A different key that merely starts with "$schema".
        ("different_schema_key", f"$schema_version: {HEAD_SCHEMA}\n", False),
    ],
)
def test_might_contain_dag_sniff(mock_bundle, case, body, expected):
    importer = YamlDagImporter()
    definition = _def(mock_bundle, f"{case}.yaml", body)
    assert importer.might_contain_dag(definition, safe_mode=True) is expected


def test_might_contain_dag_skips_sniff_without_safe_mode(mock_bundle):
    # Without safe mode the sniff is bypassed and every file is kept; the parser reports non-Dags.
    importer = YamlDagImporter()
    definition = _def(mock_bundle, "config.yaml", "just: config\n")
    assert importer.might_contain_dag(definition, safe_mode=False)


def test_might_contain_dag_keeps_unreadable_file(mock_bundle):
    # A file that cannot be read is kept rather than silently dropped.
    importer = YamlDagImporter()
    definition = _def(mock_bundle, "gone.yaml", f"$schema: {HEAD_SCHEMA}\n")
    definition.path.unlink()
    assert importer.might_contain_dag(definition, safe_mode=True)


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
    assert "payload" in b.template_fields  # $x opted the field into rendering
    assert "payload" not in a.template_fields  # a's unmarked literal was dropped from rendering


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
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: a, with: {x: 1}}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any(
        "Invalid arguments were passed to EmptyOperator" in e.message and "'x'" in e.message
        for e in result.errors
    )


def test_import_schedule_cron(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP}, with: {{payload: x}}}}", extra="schedule: '0 3 * * *'\n"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].schedule == "0 3 * * *"


def test_import_schedule_timetable(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(
            f"- {{id: a, uses: {OP}, with: {{payload: x}}}}",
            extra="schedule: {uses: airflow.sdk.definitions.timetables.simple.OnceTimetable}\n",
        ),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert isinstance(result.dags[0].timetable, OnceTimetable)


def test_import_keyed_xcom_on_uses_task(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP}, with: {{payload: x}}}}
            - {{id: b, uses: {OP}, with: {{payload: {{$x: {{task: a, key: out}}}}}}}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    b = result.dags[0].get_task("b")
    assert isinstance(b.payload, XComArg)
    assert b.payload.key == "out"
    assert b.upstream_task_ids == {"a"}


@pytest.mark.parametrize(
    ("value", "hour"),
    [("2024-01-01", 0), ("'2024-01-01'", 0), ("2024-01-01T03:30:00", 3)],  # YAML date, ISO string, datetime
)
def test_import_coerces_temporal_start_date(mock_bundle, value, hour):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP}, with: {{payload: x}}}}", extra=f"start_date: {value}\n"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    start = result.dags[0].start_date
    assert isinstance(start, datetime.datetime)  # promoted from date/string
    assert start.tzinfo is not None  # localized by DAG()
    assert (start.year, start.month, start.day, start.hour) == (2024, 1, 1, hour)


def test_import_does_not_coerce_nontemporal_arg(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP}, owner: '2024-01-01', with: {{payload: x}}}}"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].get_task("a").owner == "2024-01-01"


def test_import_coerces_temporal_with_arg(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP_TEMPORAL}, with: {{cutoff: 2024-01-01}}}}"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    cutoff = result.dags[0].get_task("a").cutoff
    assert isinstance(cutoff, datetime.datetime)
    assert (cutoff.year, cutoff.month, cutoff.day) == (2024, 1, 1)


@pytest.mark.parametrize(
    ("value", "exp_type"),
    [
        ("2024-01-01", datetime.date),  # YAML date -> plain date
        ("'2024-01-01'", datetime.date),  # ISO date string -> plain date
        ("2024-01-01T03:30:00", datetime.datetime),  # a datetime is a date subtype, left as-is
    ],
)
def test_import_coerces_date_with_arg(mock_bundle, value, exp_type):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP_DATE}, with: {{day: {value}}}}}"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    day = result.dags[0].get_task("a").day
    assert type(day) is exp_type
    assert (day.year, day.month, day.day) == (2024, 1, 1)


def test_import_coerces_pendulum_temporal_args(mock_bundle):
    # a pendulum.DateTime / pendulum.Date annotation yields pendulum values, not stdlib
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP_PENDULUM}, with: {{when: 2024-01-01T03:30:00, day: 2024-01-01}}}}"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    task = result.dags[0].get_task("a")
    assert type(task.when) is pendulum.DateTime
    assert (task.when.year, task.when.month, task.when.day, task.when.hour) == (2024, 1, 1, 3)
    assert type(task.day) is pendulum.Date
    assert (task.day.year, task.day.month, task.day.day) == (2024, 1, 1)


def test_import_coerces_timedelta_with_arg(mock_bundle):
    # a timedelta-typed argument accepts the short form and ISO-8601
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP_PERIOD}, with: {{gap: 1h30m}}}}
            - {{id: b, uses: {OP_PERIOD}, with: {{gap: PT90M}}}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].get_task("a").gap == datetime.timedelta(hours=1, minutes=30)
    assert result.dags[0].get_task("b").gap == datetime.timedelta(minutes=90)


def test_import_coerces_relativedelta_with_arg(mock_bundle):
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP_PERIOD}, with: {{window: P1Y2M}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].get_task("a").window == relativedelta(years=1, months=2)


def test_import_coerces_pendulum_duration_with_arg(mock_bundle):
    # a pendulum.Duration arg accepts the short form and an ISO duration carrying
    # years/months (which a timedelta cannot represent)
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(
            f"- {{id: a, uses: {OP_PERIOD}, with: {{span: 5m}}}}\n"
            f"- {{id: b, uses: {OP_PERIOD}, with: {{span: P1Y2M}}}}",
        ),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    span_a = result.dags[0].get_task("a").span
    span_b = result.dags[0].get_task("b").span
    assert type(span_a) is pendulum.Duration
    assert span_a == pendulum.Duration(minutes=5)
    assert type(span_b) is pendulum.Duration
    assert (span_b.years, span_b.months) == (1, 2)


def test_import_period_union_picks_type_by_components(mock_bundle):
    # for an arg that accepts both, an ISO duration with months/years becomes a relativedelta,
    # otherwise a timedelta
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP_PERIOD}, with: {{either: P1D}}}}
            - {{id: b, uses: {OP_PERIOD}, with: {{either: P1M}}}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].get_task("a").either == datetime.timedelta(days=1)
    assert result.dags[0].get_task("b").either == relativedelta(months=1)


def test_import_does_not_coerce_unparseable_duration(mock_bundle):
    # months need a relativedelta, so P1M on a timedelta-only arg is left as-is; the short form is
    # timedelta-only, so it is left as-is on a relativedelta arg
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP_PERIOD}, with: {{gap: P1M}}}}
            - {{id: b, uses: {OP_PERIOD}, with: {{window: 5m}}}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].get_task("a").gap == "P1M"
    assert result.dags[0].get_task("b").window == "5m"


def test_import_coerces_task_level_timedelta(mock_bundle):
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP}, with: {{payload: x}}, retry_delay: PT5M}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].get_task("a").retry_delay == datetime.timedelta(minutes=5)


def test_import_coerces_dag_level_timedelta(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP}, with: {{payload: x}}}}", extra="dagrun_timeout: 5m\n"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].dagrun_timeout == datetime.timedelta(minutes=5)


def test_import_schedule_timetable_coerces_delta(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(
            f"- {{id: a, uses: {OP}, with: {{payload: x}}}}",
            extra=(
                "schedule: {uses: airflow.sdk.definitions.timetables.interval."
                "DeltaDataIntervalTimetable, with: {delta: P1D}}\n"
            ),
        ),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    assert result.dags[0].timetable.delta == datetime.timedelta(days=1)


def test_import_code_task_builds_stub_operator(mock_bundle):
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
    assert b.op_kwargs["label"] == "done"  # literal bound verbatim (run args are never rendered)
    assert b.upstream_task_ids == {"a"}  # $x wired the dependency
    # The stub is fed a callable whose ordered parameters are the run argument names, so core
    # serialization can build the handler arg-binding spec (AIP-108) from a real signature.
    assert list(inspect.signature(b.python_callable).parameters) == ["value", "label"]


def test_import_mixed_container_keeps_literal_siblings_literal(mock_bundle):
    (mock_bundle.path / "note.txt").write_text("hello")
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: up, uses: {OP}, with: {{payload: x}}}}
            - {{id: t, uses: {OP}, with: {{payload: {{a: {{$x: up}}, b: '{{{{ not a template }}}}', c: {{$const: 7}}, d: {{$f: note.txt}}}}}}}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    payload = result.dags[0].get_task("t").payload
    assert isinstance(payload["a"], XComArg)  # $x still resolves
    # the container renders ($x present), so every literal sibling -- plain, $const, $f -- stays wrapped
    assert isinstance(payload["b"], LiteralValue)
    assert payload["b"].value == "{{ not a template }}"
    assert isinstance(payload["c"], LiteralValue)
    assert payload["c"].value == 7
    assert isinstance(payload["d"], LiteralValue)
    assert payload["d"].value == "hello"


def test_import_code_task_rejects_non_identifier_arg(mock_bundle):
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: a, run: {'bad-name': 1}}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("not a valid identifier" in e.message for e in result.errors)


def test_import_run_rejects_template(mock_bundle):
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: a, run: {day: {$t: '{{ ds }}'}}}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("template ($t)" in e.message for e in result.errors)


def test_import_run_rejects_keyed_xcom(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc("""
            - {id: a, run: {}}
            - {id: b, run: {v: {$x: {task: a, key: k}}}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("keyed XCom" in e.message for e in result.errors)


def test_import_run_task_rejects_with(mock_bundle):
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: t, run: {n: 1}, with: {region: eu}}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("takes its inputs under 'run:'" in e.message for e in result.errors)


def test_import_run_rejects_nested_xcom(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP}, with: {{payload: x}}}}
            - {{id: t, run: {{items: [{{$x: a}}]}}}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("nested inside a run argument" in e.message for e in result.errors)


def test_import_run_rejects_non_json_literal(mock_bundle):
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: t, run: {day: 2024-01-01}}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("must be JSON-serializable" in e.message for e in result.errors)


def test_import_run_rejects_context_key_arg(mock_bundle):
    definition = _def(mock_bundle, "dag.yaml", _doc("- {id: t, run: {ds: 1}}"))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("cannot be named" in e.message for e in result.errors)


def test_import_rejects_marker_key_in_extras(mock_bundle):
    # Dag-level extras bypass the value grammar; a marker nested there is a typo, not a literal dict.
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"- {{id: a, uses: {OP}, with: {{payload: x}}}}", extra="params: {p: {$f: README.md}}\n"),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("marker key '$f'" in e.message for e in result.errors)


@pytest.mark.parametrize("marker", ["{$x: a}", "{$t: 'v'}", "{$f: q.sql}"])
def test_import_marker_on_non_template_field_errors(mock_bundle, marker):
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
    assert any("only allowed on templated fields" in e.message for e in result.errors)


def test_import_base_operator_arg_in_with_errors(mock_bundle):
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP}, with: {{payload: x, retries: 2}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("'retries'" in e.message and "not under 'with'" in e.message for e in result.errors)


@pytest.mark.parametrize(
    "body", ["- {{id: a, uses: {op}, with: {{payload: x}}, note: 1}}", "- {{id: a, run: {{}}, note: 1}}"]
)
def test_import_operator_arg_at_task_level_errors(mock_bundle, body):
    definition = _def(mock_bundle, "dag.yaml", _doc(body.format(op=OP)))
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("not task-level" in e.message and "'note'" in e.message for e in result.errors)


def test_import_literal_by_default(mock_bundle):
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP}, with: {{payload: '{{{{ ds }}}}'}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    a = result.dags[0].get_task("a")
    assert a.payload == "{{ ds }}"  # kept literal
    assert tuple(a.template_fields) == ()  # not rendered
    assert tuple(a.template_ext) == ()


def test_import_template_marker_sets_template_fields(mock_bundle):
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP}, with: {{payload: {{$t: '{{{{ ds }}}}'}}}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    a = result.dags[0].get_task("a")
    assert a.payload == "{{ ds }}"
    assert set(a.template_fields) == {"payload"}  # $t opts the field into rendering


def test_import_keeps_operator_default_template_fields(mock_bundle):
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP_DEFAULT_TEMPLATE}, with: {{payload: hi}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert result.success, result.errors
    a = result.dags[0].get_task("a")
    assert set(a.template_fields) == {"preset"}  # operator default kept; author's unmarked payload dropped
    assert a.payload == "hi"  # author-set unmarked field no longer renders


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
    definition = _def(
        mock_bundle, "dag.yaml", _doc(f"- {{id: a, uses: {OP}, with: {{payload: {{$f: '{bad_path}'}}}}}}")
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("resolves outside" in e.message for e in result.errors)


_ZIP_OP_SOURCE = """
from airflow.sdk import BaseOperator


class ZipOp(BaseOperator):
    template_fields = ("payload",)

    def __init__(self, *, payload=None, **kwargs):
        super().__init__(**kwargs)
        self.payload = payload

    def execute(self, context):
        return self.payload
"""


def test_import_yaml_inside_zip_bundle(tmp_path):
    zip_path = tmp_path / "bundle.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("_ziptest_ops.py", _ZIP_OP_SOURCE)
        z.writestr("q.sql", "SELECT 42")
        z.writestr(
            "dag.yaml",
            _doc("- {id: a, uses: _ziptest_ops.ZipOp, with: {payload: {$f: q.sql}}}", dag_id="z"),
        )
    definition = ZipMemberDagDefinition(zip_path=zip_path, file_path="dag.yaml")
    bundle = SimpleNamespace(name="zbundle", path=tmp_path)
    result = YamlDagImporter().import_definition(definition, bundle=bundle)
    assert result.success, result.errors
    a = result.dags[0].get_task("a")
    assert type(a).__name__ == "ZipOp"  # imported from a module inside the zip (import_context)
    assert a.payload == "SELECT 42"  # $f read a sibling member in the same archive


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
    assert any("No module named" in e.message for e in result.errors)


def test_import_one_bad_document_fails_whole_file(mock_bundle):
    good = _doc(f"- {{id: a, uses: {OP}, with: {{payload: x}}}}", dag_id="good")
    bad = _doc("- {id: a, uses: does.not.Exist, with: {}}", dag_id="bad")
    definition = _def(mock_bundle, "dag.yaml", f"{good}\n---\n{bad}")
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert len(result.errors) == 1
    err = result.errors[0]
    assert "dag_id=" not in err.source_reference  # keyed by the file, not the document
    assert "dag_id='bad'" in err.message  # the failing document is named, not mashed


def test_import_duplicate_dag_id_across_documents_errors(mock_bundle):
    one = _doc(f"- {{id: a, uses: {OP}, with: {{payload: x}}}}", dag_id="dup")
    two = _doc(f"- {{id: b, uses: {OP}, with: {{payload: y}}}}", dag_id="dup")
    definition = _def(mock_bundle, "dag.yaml", f"{one}\n---\n{two}")
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    assert any("duplicate dag_id 'dup'" in e.message for e in result.errors)


def test_import_dependency_cycle_errors(mock_bundle):
    definition = _def(
        mock_bundle,
        "dag.yaml",
        _doc(f"""
            - {{id: a, uses: {OP}, with: {{payload: x}}, needs: [b]}}
            - {{id: b, uses: {OP}, with: {{payload: y}}, needs: [a]}}
            - {{id: c, uses: {OP}, with: {{payload: z}}, needs: [a]}}
        """),
    )
    result = YamlDagImporter().import_definition(definition, bundle=mock_bundle)
    assert not result.dags
    (error,) = [e for e in result.errors if "cycle" in e.message]
    assert "'a'" in error.message
    assert "'b'" in error.message
    assert "'c'" not in error.message
