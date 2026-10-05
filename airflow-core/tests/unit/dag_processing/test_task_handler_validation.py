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

import pytest

from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import LiteralArgBinding, XComArgBinding
from airflow.dag_processing.processor import TaskHandlerDeclaration, TaskHandlerParam
from airflow.dag_processing.task_handler_validation import (
    ArgumentCheck,
    StubTask,
    StubTaskWarning,
    TaskHandlerAnswer,
    TaskHandlerMatch,
    TaskHandlerProblem,
    check_task_handler_arguments,
    check_value_schema,
    collect_stub_tasks,
    format_import_errors,
    match_task_handlers,
)
from airflow.sdk import DAG, task
from airflow.serialization.serialized_objects import LazyDeserializedDAG

BUNDLE_NAME = "go-task-handlers"
DAG_FILE = "dags/etl.py"

INTEGER = {"type": "integer"}
NUMBER = {"type": "number"}
STRING = {"type": "string"}
NULL = {"type": "null"}
NULLABLE_INTEGER = {"anyOf": [INTEGER, NULL]}
OBJECT = {"type": "object", "additionalProperties": True}


def _literal(name, value=None, *, schema=None, from_default=False):
    return LiteralArgBinding(
        kind="literal", name=name, value=value, value_schema=schema, from_default=from_default
    )


def _default(name, value=None, *, schema=None):
    return _literal(name, value, schema=schema, from_default=True)


def _xcom(name, *, schema=None):
    return XComArgBinding(kind="xcom", name=name, task_id="extract", value_schema=schema)


def _param(name=None, *, schema=None, exact_name=False):
    return TaskHandlerParam(name=name, value_schema=schema, exact_name=exact_name)


def _positional(*params, task_id="transform"):
    return TaskHandlerDeclaration(task_id=task_id, binding="positional", params=list(params))


def _named(*params, task_id="transform"):
    return TaskHandlerDeclaration(task_id=task_id, binding="named", params=list(params))


def _check(*, errors=(), passed_not_declared=(), declared_not_passed=()):
    return ArgumentCheck(
        errors=list(errors),
        passed_not_declared=list(passed_not_declared),
        declared_not_passed=list(declared_not_passed),
    )


def _stub(task_id="transform", *, arg_bindings=(), is_mapped=False, dag_id="etl"):
    return StubTask(
        dag_id=dag_id,
        task_id=task_id,
        queue="go",
        relative_fileloc=DAG_FILE,
        arg_bindings=list(arg_bindings),
        is_mapped=is_mapped,
    )


def _answer(**task_handlers):
    return TaskHandlerAnswer(bundle_name=BUNDLE_NAME, rel_path="bin/etl", task_handlers=task_handlers)


@pytest.mark.parametrize(
    ("arg_bindings", "declaration", "expected"),
    [
        pytest.param(
            [_literal("a"), _xcom("b")],
            _positional(_param(), _param()),
            _check(),
            id="positional-count-matches",
        ),
        pytest.param(
            [_literal("a"), _literal("b"), _literal("c")],
            _positional(_param(), _param()),
            _check(errors=["passes 3 arguments, the task handler takes 2"]),
            id="positional-too-many",
        ),
        pytest.param(
            [_literal("a")],
            _positional(_param(), _param()),
            _check(errors=["passes 1 argument, the task handler takes 2"]),
            id="positional-too-few",
        ),
        pytest.param(
            [_literal("a"), _default("b")], _positional(_param()), _check(), id="defaulted-dropped-to-match"
        ),
        pytest.param(
            [_literal("a"), _default("b")],
            _positional(_param(), _param()),
            _check(),
            id="defaulted-kept-when-the-full-count-matches",
        ),
        pytest.param(
            [_literal("a"), _default("b"), _literal("c")],
            _positional(_param()),
            _check(errors=["passes 3 arguments (2 without defaults), the task handler takes 1"]),
            id="dropping-defaulted-still-mismatches",
        ),
        pytest.param(
            [_literal("x", schema=INTEGER), _literal("y", schema=STRING)],
            _positional(_param("y", schema=INTEGER), _param("x", schema=STRING)),
            _check(),
            id="positional-names-ignored",
        ),
        pytest.param(
            [_literal("a", schema=INTEGER), _xcom("b", schema=STRING)],
            _positional(_param(schema=INTEGER), _param(schema=INTEGER)),
            _check(errors=["argument 'b' is string, the task handler takes integer"]),
            id="positional-type-mismatch",
        ),
        pytest.param(
            [_literal("a", schema=STRING), _literal("b", schema=STRING)],
            _positional(_param(schema=INTEGER), _param(schema=INTEGER)),
            _check(
                errors=[
                    "argument 'a' is string, the task handler takes integer",
                    "argument 'b' is string, the task handler takes integer",
                ]
            ),
            id="every-mismatching-argument-is-reported",
        ),
        pytest.param([], _positional(), _check(), id="argless-against-no-params"),
        pytest.param(
            [],
            _positional(_param()),
            _check(errors=["passes 0 arguments, the task handler takes 1"]),
            id="argless-against-positional-params",
        ),
        pytest.param(
            [_literal("user_id", schema=INTEGER)],
            _named(_param("user_id", schema=INTEGER)),
            _check(),
            id="named-exact",
        ),
        pytest.param(
            [_literal("user_id"), _literal("region")],
            _named(_param("UserId"), _param("region")),
            _check(),
            id="named-folded",
        ),
        pytest.param(
            [_literal("user_id"), _literal("region")],
            _named(_param("UserId", exact_name=True), _param("region")),
            _check(passed_not_declared=["user_id"], declared_not_passed=["UserId"]),
            id="exact-name-blocks-folding",
        ),
        pytest.param(
            [_literal("region"), _literal("extra")],
            _named(_param("region")),
            _check(passed_not_declared=["extra"]),
            id="passed-argument-no-param-takes",
        ),
        pytest.param(
            [_literal("region"), _default("extra")],
            _named(_param("region")),
            _check(),
            id="defaulted-argument-no-param-takes",
        ),
        pytest.param(
            [_literal("region")],
            _named(_param("region"), _param("limit")),
            _check(declared_not_passed=["limit"]),
            id="param-no-argument-fills",
        ),
        pytest.param(
            [],
            _named(_param("region")),
            _check(declared_not_passed=["region"]),
            id="argless-against-named-params",
        ),
        pytest.param(
            [_literal("config"), _default("extra")],
            _named(_param("region"), _param("limit")),
            _check(),
            id="lone-argument-without-a-schema-may-be-the-whole-value",
        ),
        pytest.param(
            [_literal("config", schema=OBJECT)],
            _named(_param("region"), _param("limit")),
            _check(),
            id="lone-object-argument-may-be-the-whole-value",
        ),
        pytest.param(
            [_literal("config", schema={"$ref": "#/$defs/Config"})],
            _named(_param("region"), _param("limit")),
            _check(),
            id="lone-argument-of-unknown-type-may-be-the-whole-value",
        ),
        pytest.param(
            [_literal("config", schema=STRING)],
            _named(_param("region"), _param("limit")),
            _check(passed_not_declared=["config"], declared_not_passed=["region", "limit"]),
            id="lone-string-argument-is-not-the-whole-value",
        ),
        pytest.param(
            [_literal("config")],
            _named(_param("region", exact_name=True), _param("limit")),
            _check(passed_not_declared=["config"], declared_not_passed=["region", "limit"]),
            id="lone-argument-with-an-exact-name-param",
        ),
        pytest.param(
            [_literal("config")],
            _named(),
            _check(passed_not_declared=["config"]),
            id="lone-argument-against-no-params",
        ),
        pytest.param(
            [_literal("a"), _literal("b")],
            _named(_param("region")),
            _check(passed_not_declared=["a", "b"], declared_not_passed=["region"]),
            id="two-unmatched-arguments",
        ),
        pytest.param(
            [_literal("user_id"), _literal("userId"), _literal("region")],
            _named(_param("UserID"), _param("region")),
            _check(passed_not_declared=["user_id", "userId"], declared_not_passed=["UserID"]),
            id="folded-name-two-arguments-share",
        ),
        pytest.param(
            [_literal("user_id"), _literal("userId")],
            _named(_param("user_id")),
            _check(passed_not_declared=["userId"]),
            id="exact-name-despite-a-shared-fold",
        ),
        pytest.param(
            [_literal("region"), _literal("limit")],
            _named(_param("region"), _param()),
            _check(passed_not_declared=["limit"], declared_not_passed=["#1"]),
            id="nameless-named-param",
        ),
        pytest.param(
            [_literal("region", schema=INTEGER), _literal("limit", schema=INTEGER)],
            _named(_param("limit", schema=INTEGER), _param("region", schema=STRING)),
            _check(errors=["argument 'region' is integer, the task handler takes string"]),
            id="named-type-mismatch",
        ),
        pytest.param(
            [_literal("a", schema=INTEGER), _default("b", schema=STRING)],
            _positional(_param(schema=INTEGER), _param(schema=INTEGER)),
            _check(errors=["argument 'b' is string, the task handler takes integer"]),
            id="positional-defaulted-argument-type-mismatch",
        ),
        pytest.param(
            [_literal("region", schema=STRING), _default("limit", schema=STRING)],
            _named(_param("region", schema=STRING), _param("limit", schema=INTEGER)),
            _check(errors=["argument 'limit' is string, the task handler takes integer"]),
            id="named-defaulted-argument-type-mismatch",
        ),
        pytest.param(
            [_literal("a", schema=STRING)],
            TaskHandlerDeclaration(task_id="transform", binding="named", params=None),
            _check(),
            id="unlisted-params",
        ),
    ],
)
def test_check_task_handler_arguments(arg_bindings, declaration, expected):
    assert check_task_handler_arguments(arg_bindings, declaration) == expected


@pytest.mark.parametrize(
    ("stub_schema", "handler_schema", "expected"),
    [
        pytest.param(INTEGER, INTEGER, None, id="equal"),
        pytest.param(INTEGER, NUMBER, None, id="integer-into-number"),
        pytest.param(NUMBER, INTEGER, "number, the task handler takes integer", id="number-into-integer"),
        pytest.param(STRING, INTEGER, "string, the task handler takes integer", id="string-into-integer"),
        pytest.param(
            NULLABLE_INTEGER,
            INTEGER,
            "integer or null, the task handler takes integer",
            id="nullable-into-non-nullable",
        ),
        pytest.param(INTEGER, NULLABLE_INTEGER, None, id="non-nullable-into-nullable"),
        pytest.param(NULLABLE_INTEGER, NULLABLE_INTEGER, None, id="nullable-into-nullable"),
        pytest.param(NULL, NULLABLE_INTEGER, None, id="null-only-into-nullable"),
        pytest.param(
            {"const": None}, STRING, "null, the task handler takes string", id="null-into-non-nullable"
        ),
        pytest.param({"anyOf": [INTEGER, STRING]}, INTEGER, None, id="union-with-one-accepted-type"),
        pytest.param(
            {"anyOf": [STRING, {"type": "boolean"}]},
            INTEGER,
            "string or boolean, the task handler takes integer",
            id="union-with-no-accepted-type",
        ),
        pytest.param(
            {"anyOf": [INTEGER, STRING]}, {"anyOf": [STRING, INTEGER, NULL]}, None, id="any-of-subset"
        ),
        pytest.param(
            {"anyOf": [INTEGER, STRING, NULL]},
            {"oneOf": [STRING, INTEGER]},
            "string, integer or null, the task handler takes string or integer",
            id="any-of-superset",
        ),
        pytest.param(
            {"type": ["integer", "null"]},
            INTEGER,
            "integer or null, the task handler takes integer",
            id="type-list",
        ),
        pytest.param(
            {"type": ["string", "null"]}, {"anyOf": [STRING, NULL]}, None, id="type-list-into-any-of"
        ),
        pytest.param({"const": "eu"}, STRING, None, id="const"),
        pytest.param({"const": 1}, STRING, "integer, the task handler takes string", id="const-mismatch"),
        pytest.param({"enum": [1, 2.5]}, NUMBER, None, id="enum"),
        pytest.param(
            {"enum": ["eu", None]},
            {"enum": ["eu", "us"]},
            "string or null, the task handler takes string",
            id="enum-mismatch",
        ),
        pytest.param(
            {"enum": [True]}, INTEGER, "boolean, the task handler takes integer", id="boolean-into-integer"
        ),
        pytest.param({"$ref": "#/$defs/Config"}, INTEGER, None, id="ref-skipped"),
        pytest.param({"allOf": [INTEGER]}, STRING, None, id="all-of-skipped"),
        pytest.param(
            {"anyOf": [{"$ref": "#/$defs/Config"}, NULL]}, INTEGER, None, id="any-of-with-a-ref-skipped"
        ),
        pytest.param({}, INTEGER, None, id="empty-stub-schema-skipped"),
        pytest.param(STRING, {}, None, id="empty-handler-schema-skipped"),
        pytest.param(None, INTEGER, None, id="no-stub-schema"),
        pytest.param(STRING, None, None, id="no-handler-schema"),
        pytest.param(
            {"type": "integer", "format": "int64"},
            {"type": "integer", "format": "int32", "minimum": 0},
            None,
            id="format-and-range-ignored",
        ),
    ],
)
def test_check_value_schema(stub_schema, handler_schema, expected):
    assert check_value_schema(stub_schema, handler_schema) == expected


def test_each_stub_task_is_checked_against_the_handler_of_its_own_dag():
    stub_tasks = [
        _stub("extract"),
        _stub("transform", arg_bindings=[_literal("a")]),
        _stub("transform", dag_id="reporting"),
    ]
    # "audit" has no stub task here; it and "reporting" both register a "transform" that takes nothing,
    # while "etl"'s takes one.
    answer = _answer(
        reporting=[_positional(task_id="transform")],
        etl=[_positional(task_id="extract"), _positional(_param(), task_id="transform")],
        audit=[_positional(task_id="transform")],
    )

    assert match_task_handlers(stub_tasks, answer) == TaskHandlerMatch(problems=[], warnings=[])


def test_a_stub_task_without_a_handler_is_a_problem():
    answer = _answer(other=[_positional()], etl=[_positional(task_id="extract")])

    assert match_task_handlers([_stub()], answer) == TaskHandlerMatch(
        problems=[
            TaskHandlerProblem(
                relative_fileloc=DAG_FILE,
                message="Dag 'etl', task 'transform': 'bin/etl' in Dag bundle 'go-task-handlers' "
                "registers no task handler for it",
                dag_id="etl",
                task_id="transform",
            )
        ],
        warnings=[],
    )


def test_a_handler_without_a_stub_task_is_not_a_problem():
    answer = _answer(etl=[_positional(), _positional(task_id="load")], reporting=[_named(task_id="publish")])

    assert match_task_handlers([_stub()], answer) == TaskHandlerMatch(problems=[], warnings=[])


def test_a_mapped_stub_task_is_checked_for_its_handler_only():
    stub_tasks = [_stub(is_mapped=True), _stub("load", is_mapped=True)]

    match = match_task_handlers(stub_tasks, _answer(etl=[_positional(_param(), _param())]))

    assert match.warnings == []
    assert [problem.task_id for problem in match.problems] == ["load"]


def test_unlisted_params_are_checked_for_the_handler_only():
    stub = _stub(arg_bindings=[_literal("a", schema=STRING)])
    answer = _answer(etl=[TaskHandlerDeclaration(task_id="transform", binding="named", params=None)])

    assert match_task_handlers([stub], answer) == TaskHandlerMatch(problems=[], warnings=[])


@pytest.mark.parametrize(
    ("arg_bindings", "params", "passed_not_declared", "declared_not_passed"),
    [
        pytest.param(
            [_literal("region"), _literal("unused_label")],
            [_param("region"), _param("limit")],
            ["unused_label"],
            ["limit"],
            id="both-sides-mismatch",
        ),
        pytest.param(
            [_literal("region")],
            [_param("region"), _param("limit")],
            [],
            ["limit"],
            id="only-a-declared-param-is-unfilled",
        ),
        pytest.param(
            [_literal("region"), _literal("extra")],
            [_param("region")],
            ["extra"],
            [],
            id="only-a-passed-argument-is-unclaimed",
        ),
    ],
)
def test_a_name_mismatch_is_a_warning_not_a_problem(
    arg_bindings, params, passed_not_declared, declared_not_passed
):
    stub = _stub(arg_bindings=arg_bindings)
    answer = _answer(etl=[_named(*params)])

    assert match_task_handlers([stub], answer) == TaskHandlerMatch(
        problems=[],
        warnings=[
            StubTaskWarning(
                dag_id="etl",
                task_id="transform",
                artifact_bundle_name=BUNDLE_NAME,
                artifact_rel_path="bin/etl",
                passed_not_declared=passed_not_declared,
                declared_not_passed=declared_not_passed,
            )
        ],
    )


@pytest.mark.parametrize(
    ("params", "error"),
    [
        pytest.param(
            [_param(schema=INTEGER), _param(), _param()],
            "passes 2 arguments, the task handler takes 3",
            id="count",
        ),
        pytest.param(
            [_param(schema=INTEGER), _param()],
            "argument 'count' is integer or null, the task handler takes integer",
            id="value-type",
        ),
    ],
)
def test_an_argument_problem_names_the_artifact(params, error):
    stub = _stub(arg_bindings=[_literal("count", schema=NULLABLE_INTEGER), _literal("label")])

    match = match_task_handlers([stub], _answer(etl=[_positional(*params)]))

    assert match == TaskHandlerMatch(
        problems=[
            TaskHandlerProblem(
                relative_fileloc=DAG_FILE,
                message=f"Dag 'etl', task 'transform' ('bin/etl' in Dag bundle 'go-task-handlers'): {error}",
                dag_id="etl",
                task_id="transform",
            )
        ],
        warnings=[],
    )


def test_each_mismatching_argument_is_its_own_problem():
    stub = _stub(arg_bindings=[_literal("a", schema=STRING), _literal("b", schema=STRING)])
    answer = _answer(etl=[_positional(_param(schema=INTEGER), _param(schema=INTEGER))])

    match = match_task_handlers([stub], answer)

    assert match == TaskHandlerMatch(
        problems=[
            TaskHandlerProblem(
                relative_fileloc=DAG_FILE,
                message="Dag 'etl', task 'transform' ('bin/etl' in Dag bundle 'go-task-handlers'): "
                "argument 'a' is string, the task handler takes integer",
                dag_id="etl",
                task_id="transform",
            ),
            TaskHandlerProblem(
                relative_fileloc=DAG_FILE,
                message="Dag 'etl', task 'transform' ('bin/etl' in Dag bundle 'go-task-handlers'): "
                "argument 'b' is string, the task handler takes integer",
                dag_id="etl",
                task_id="transform",
            ),
        ],
        warnings=[],
    )


def test_collect_stub_tasks():
    with DAG(dag_id="etl", schedule=None) as dag:

        @task.stub(queue="go")
        def extract(): ...

        @task.stub(queue="go")
        def transform(data: str, region: str = "eu"): ...

        @task.stub(queue="go")
        def fan_out(item: int): ...

        @task.stub
        def unrouted(): ...

        @task
        def report(): ...

        transform(extract())
        fan_out.expand(item=[1, 2])
        unrouted()
        report()

    with DAG(dag_id="unserialized", schedule=None) as unserialized_dag:

        @task.stub(queue="go")
        def load(): ...

        load()

    dag.relative_fileloc = DAG_FILE

    stub_tasks = collect_stub_tasks([dag, unserialized_dag], [LazyDeserializedDAG.from_dag(dag)])

    assert stub_tasks == [
        StubTask(
            dag_id="etl",
            task_id="extract",
            queue="go",
            relative_fileloc=DAG_FILE,
            arg_bindings=[],
            is_mapped=False,
        ),
        StubTask(
            dag_id="etl",
            task_id="transform",
            queue="go",
            relative_fileloc=DAG_FILE,
            arg_bindings=[
                XComArgBinding(kind="xcom", name="data", task_id="extract", value_schema=STRING),
                _default("region", "eu", schema=STRING),
            ],
            is_mapped=False,
        ),
        StubTask(
            dag_id="etl",
            task_id="fan_out",
            queue="go",
            relative_fileloc=DAG_FILE,
            arg_bindings=[],
            is_mapped=True,
        ),
        StubTask(
            dag_id="etl",
            task_id="unrouted",
            queue="default",
            relative_fileloc=DAG_FILE,
            arg_bindings=[],
            is_mapped=False,
        ),
    ]


def test_collect_stub_tasks_reads_the_arguments_the_worker_gets():
    with DAG(dag_id="etl", schedule=None) as dag:

        @task.stub(queue="go")
        def load(table: str, columns: tuple = ("id", "name")): ...

        @task.stub(queue="go")
        def merge(mapping: dict): ...

        load("users")
        merge({1: "a"})

    stub_tasks = collect_stub_tasks([dag], [LazyDeserializedDAG.from_dag(dag)])

    assert [stub.arg_bindings for stub in stub_tasks] == [
        [
            _literal("table", "users", schema=STRING),
            _default("columns", ["id", "name"], schema={"type": "array", "items": {}}),
        ],
        [_literal("mapping", {"1": "a"}, schema=OBJECT)],
    ]


def test_format_import_errors_groups_by_file_in_a_stable_order():
    problems = [
        TaskHandlerProblem(relative_fileloc="dags/z.py", message="z: load", dag_id="z", task_id="load"),
        TaskHandlerProblem(
            relative_fileloc=DAG_FILE, message="etl: transform", dag_id="etl", task_id="transform"
        ),
        TaskHandlerProblem(relative_fileloc=DAG_FILE, message="Coordinator 'java'"),
        TaskHandlerProblem(
            relative_fileloc=DAG_FILE, message="etl: load, count", dag_id="etl", task_id="load"
        ),
        TaskHandlerProblem(
            relative_fileloc=DAG_FILE, message="audit: check", dag_id="audit", task_id="check"
        ),
        TaskHandlerProblem(relative_fileloc=DAG_FILE, message="Coordinator 'go-sdk'"),
        TaskHandlerProblem(
            relative_fileloc=DAG_FILE, message="etl: load, type", dag_id="etl", task_id="load"
        ),
    ]

    import_errors = format_import_errors(problems)

    assert list(import_errors) == [DAG_FILE, "dags/z.py"]
    assert import_errors == {
        "dags/z.py": "Stub tasks in dags/z.py do not match their task handlers:\n- z: load",
        DAG_FILE: "\n".join(
            [
                "Stub tasks in dags/etl.py do not match their task handlers:",
                "- Coordinator 'java'",
                "- Coordinator 'go-sdk'",
                "- audit: check",
                "- etl: load, count",
                "- etl: load, type",
                "- etl: transform",
            ]
        ),
    }
