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
import logging
import pprint

from sqlalchemy import inspect

from airflow.models import Base

log = logging.getLogger("tests.otel.test_utils")


def dump_airflow_metadata_db(session):
    inspector = inspect(session.bind)
    all_tables = inspector.get_table_names()

    # dump with the entire db
    db_dump = {}

    log.debug("\n-----START_airflow_db_dump-----\n")

    for table_name in all_tables:
        log.debug("\nDumping table: %s", table_name)
        table = Base.metadata.tables.get(table_name)
        if table is not None:
            query = session.query(table)
            results = [dict(row) for row in query.all()]
            db_dump[table_name] = results
            # Pretty-print the table contents
            if table_name == "connection":
                filtered_results = [row for row in results if row.get("conn_id") == "airflow_db"]
                pprint.pprint({table_name: filtered_results}, width=120)
            else:
                pprint.pprint({table_name: results}, width=120)
        else:
            log.debug("Table %s not found in metadata.", table_name)

    log.debug("\nAirflow metadata database dump complete.")
    log.debug("\n-----END_airflow_db_dump-----\n")


def extract_spans_from_output(output_lines: list):
    """
    For a given list of ConsoleSpanExporter output lines, it extracts the json spans
    and creates two dictionaries.
    :return: root spans dict (key: root_span_id - value: root_span), spans dict (key: span_id - value: span)
    """
    span_dict = {}
    root_span_dict = {}
    total_lines = len(output_lines)
    index = 0

    while index < total_lines:
        line = output_lines[index].strip()
        # The start and the end of the json object, don't have any indentation.
        # We can use that to identify them.
        if line.startswith("{") and line == "{":  # Json start.
            # Get all the lines and append them until we reach the end.
            json_lines = [line]
            index += 1
            while index < total_lines:
                line = output_lines[index]
                # The 'command' line uses single quotes, and it results in an error when parsing the json.
                # It's not needed when checking for spans. So instead of formatting it properly, just skip it.
                if '"command":' not in line:
                    json_lines.append(line)
                if line.strip().startswith("}") and line == "}":  # Json end.
                    # Since, this is the end of the object, break the loop.
                    break
                index += 1
            # Create a formatted json string and then convert the string to a python dict.
            json_str = "\n".join(json_lines)
            try:
                span = json.loads(json_str)
                span_id = span["context"]["span_id"]
                span_dict[span_id] = span

                if span["parent_id"] is None:
                    # This is a root span, add it to the root_span_map as well.
                    root_span_id = span["context"]["span_id"]
                    root_span_dict[root_span_id] = span

            except json.JSONDecodeError as e:
                log.error("Failed to parse JSON span: %s", e)
                log.error("Failed JSON string:")
                log.error(json_str)
        else:
            index += 1

    return root_span_dict, span_dict


def get_id_for_a_given_name(span_dict: dict, span_name: str):
    for span_id, span in span_dict.items():
        if span["name"] == span_name:
            return span_id
    return None


def get_parent_child_dict(root_span_dict, span_dict):
    """
    Create a dictionary with parent-child span relationships.
    :return: key: root_span_id - value: list of child spans
    """
    parent_child_dict = {}
    for root_span_id, root_span in root_span_dict.items():
        # Compare each 'root_span_id' with each 'parent_id' from the span_dict.
        # If there is a match, then the span in the span_dict, is a child.
        # For every root span, create a list of child spans.
        child_span_list = []
        for span_id, span in span_dict.items():
            if root_span_id == span_id:
                # It's the same span, skip.
                continue
            # If the parent_id matches the root_span_id and if the trace_id is the same.
            if (
                span["parent_id"] == root_span_id
                and root_span["context"]["trace_id"] == span["context"]["trace_id"]
            ):
                child_span_list.append(span)
        parent_child_dict[root_span_id] = child_span_list
    return parent_child_dict


def get_child_list_for_non_root(span_dict: dict, span_name: str):
    """
    Get a list of children spans for a parent span that isn't also a root span.
    e.g. a task span with sub-spans, is a parent span but not a root span.
    :return: list of spans
    """
    parent_span_id = get_id_for_a_given_name(span_dict=span_dict, span_name=span_name)
    parent_span = span_dict.get(parent_span_id)

    if parent_span is None:
        return []

    child_span_list = []
    for span_id, span in span_dict.items():
        if span_id == parent_span_id:
            # It's the same span, skip.
            continue
        if (
            span["parent_id"] == parent_span_id
            and span["context"]["trace_id"] == parent_span["context"]["trace_id"]
        ):
            child_span_list.append(span)

    return child_span_list


def assert_parent_name_and_get_id(root_span_dict: dict, span_name: str):
    parent_id = get_id_for_a_given_name(root_span_dict, span_name)

    assert parent_id is not None, f"Parent span '{span_name}' wasn't found."

    return parent_id


def assert_span_name_belongs_to_root_span(root_span_dict: dict, span_name: str, should_succeed: bool):
    """Check that a given span name belongs to a root span."""
    log.info("Checking that '%s' is a root span.", span_name)
    # Check if any root span has the specified span_name
    name_exists = any(root_span.get("name", None) == span_name for root_span in root_span_dict.values())

    # Assert based on the should_succeed flag
    if should_succeed:
        assert name_exists, f"Expected span '{span_name}' to belong to a root span, but it does not."
        log.info("Span '%s' belongs to a root span, as expected.", span_name)
    else:
        assert not name_exists, f"Expected span '{span_name}' not to belong to a root span, but it does."
        log.info("Span '%s' doesn't belong to a root span, as expected.", span_name)


def assert_parent_children_spans(
    parent_child_dict: dict, root_span_dict: dict, parent_name: str, children_names: list[str]
):
    """Check that all spans in a given list are children of a given root span name."""
    log.info("Checking that spans '%s' are children of root span '%s'.", children_names, parent_name)
    # Iterate the root_span_dict, to get the span_id for the parent_name.
    parent_id = assert_parent_name_and_get_id(root_span_dict=root_span_dict, span_name=parent_name)

    # Use the root span_id to get the children ids.
    child_span_list = parent_child_dict[parent_id]

    # For each children id, get the entry from the span_dict.
    names_from_dict = []
    for child_span in child_span_list:
        name = child_span["name"]
        names_from_dict.append(name)

    # Assert that all given children names match the names from the dictionary.
    for name in children_names:
        assert (
            name in names_from_dict
        ), f"Span name '{name}' wasn't found in children span names. It's not a child of span '{parent_name}'."


def assert_parent_children_spans_for_non_root(span_dict: dict, parent_name: str, children_names: list[str]):
    """Check that all spans in a given list are children of a given non-root span name."""
    log.info("Checking that spans '%s' are children of span '%s'.", children_names, parent_name)
    child_span_list = get_child_list_for_non_root(span_dict=span_dict, span_name=parent_name)

    # For each children id, get the entry from the span_dict.
    names_from_dict = []
    for child_span in child_span_list:
        name = child_span["name"]
        names_from_dict.append(name)

    # Assert that all given children names match the names from the dictionary.
    for name in children_names:
        assert (
            name in names_from_dict
        ), f"Span name '{name}' wasn't found in children span names. It's not a child of span '{parent_name}'."


def assert_span_not_in_children_spans(
    parent_child_dict: dict,
    root_span_dict: dict,
    span_dict: dict,
    parent_name: str,
    child_name: str,
    span_exists: bool,
):
    """Check that a span for a given name, doesn't belong to the children of a given root span name."""
    log.info("Checking that span '%s' is not a child of span '%s'.", child_name, parent_name)
    # Iterate the root_span_dict, to get the span_id for the parent_name.
    parent_id = assert_parent_name_and_get_id(root_span_dict=root_span_dict, span_name=parent_name)

    # Use the root span_id to get the children ids.
    child_span_id_list = parent_child_dict[parent_id]

    child_id = get_id_for_a_given_name(span_dict=span_dict, span_name=child_name)

    if span_exists:
        assert child_id is not None, f"Span '{child_name}' should exist but it doesn't."
        assert (
            child_id not in child_span_id_list
        ), f"Span '{child_name}' shouldn't be a child of span '{parent_name}', but it is."
    else:
        assert child_id is None, f"Span '{child_name}' shouldn't exist but it does."


class TestUtilsUnit:
    # The method that extracts the spans from the output,
    # counts that there is no indentation on the cli, when a span starts and finishes.
    example_output = """
{
    "name": "test_dag",
    "context": {
        "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
        "span_id": "0x779a3a331684439e",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": null,
    "start_time": "2024-11-30T14:01:21.738052Z",
    "end_time": "2024-11-30T14:01:36.541442Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "airflow.category": "DAG runs",
        "airflow.dag_run.dag_id": "otel_test_dag_with_pause",
        "airflow.dag_run.logical_date": "2024-11-30 14:01:15+00:00",
        "airflow.dag_run.run_id": "manual__2024-11-30T14:01:15.333003+00:00",
        "airflow.dag_run.queued_at": "2024-11-30 14:01:21.738052+00:00",
        "airflow.dag_run.run_start_date": "2024-11-30 14:01:22.192655+00:00",
        "airflow.dag_run.run_end_date": "2024-11-30 14:01:36.541442+00:00",
        "airflow.dag_run.run_duration": "14.348787",
        "airflow.dag_run.state": "success",
        "airflow.dag_run.external_trigger": "True",
        "airflow.dag_run.run_type": "manual",
        "airflow.dag_run.data_interval_start": "2024-11-30 14:01:15+00:00",
        "airflow.dag_run.data_interval_end": "2024-11-30 14:01:15+00:00",
        "airflow.dag_version.version": "2",
        "airflow.dag_run.conf": "{}"
    },
    "events": [
        {
            "name": "airflow.dag_run.queued",
            "timestamp": "2024-11-30T14:01:21.738052Z",
            "attributes": {}
        },
        {
            "name": "airflow.dag_run.started",
            "timestamp": "2024-11-30T14:01:22.192655Z",
            "attributes": {}
        },
        {
            "name": "airflow.dag_run.ended",
            "timestamp": "2024-11-30T14:01:36.541442Z",
            "attributes": {}
        }
    ],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "task_1",
    "context": {
        "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
        "span_id": "0xba9f48dcfac5d40a",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0x779a3a331684439e",
    "start_time": "2024-11-30T14:01:22.220785Z",
    "end_time": "2024-11-30T14:01:34.339423Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "airflow.category": "scheduler",
        "airflow.task.task_id": "task_1",
        "airflow.task.dag_id": "otel_test_dag_with_pause",
        "airflow.task.state": "success",
        "airflow.task.start_date": "2024-11-30 14:01:23.468047+00:00",
        "airflow.task.end_date": "2024-11-30 14:01:34.339423+00:00",
        "airflow.task.duration": 10.871376,
        "airflow.task.executor_config": "{}",
        "airflow.task.logical_date": "2024-11-30 14:01:15+00:00",
        "airflow.task.hostname": "351295342ba2",
        "airflow.task.log_url": "http://localhost:8080/dags/otel_test_dag_with_pause/grid?dag_run_id=manual__2024-11-30T14%3A01%3A15.333003%2B00%3A00&task_id=task_1&base_date=2024-11-30T14%3A01%3A15%2B0000&tab=logs",
        "airflow.task.operator": "PythonOperator",
        "airflow.task.try_number": 1,
        "airflow.task.executor_state": "success",
        "airflow.task.pool": "default_pool",
        "airflow.task.queue": "default",
        "airflow.task.priority_weight": 2,
        "airflow.task.queued_dttm": "2024-11-30 14:01:22.216965+00:00",
        "airflow.task.queued_by_job_id": 1,
        "airflow.task.pid": 1748
    },
    "events": [
        {
            "name": "task to trigger",
            "timestamp": "2024-11-30T14:01:22.220873Z",
            "attributes": {
                "command": "['airflow', 'tasks', 'run', 'otel_test_dag_with_pause', 'task_1', 'manual__2024-11-30T14:01:15.333003+00:00', '--local', '--subdir', 'DAGS_FOLDER/otel_test_dag_with_pause.py', '--carrier', '{\"traceparent\": \"00-01f441c9c53e793e8808c77939ddbf36-ba9f48dcfac5d40a-01\"}']",
                "conf": "{}"
            }
        },
        {
            "name": "airflow.task.queued",
            "timestamp": "2024-11-30T14:01:22.216965Z",
            "attributes": {}
        },
        {
            "name": "airflow.task.started",
            "timestamp": "2024-11-30T14:01:23.468047Z",
            "attributes": {}
        },
        {
            "name": "airflow.task.ended",
            "timestamp": "2024-11-30T14:01:34.339423Z",
            "attributes": {}
        }
    ],
    "links": [
        {
            "context": {
                "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
                "span_id": "0x779a3a331684439e",
                "trace_state": "[]"
            },
            "attributes": {
                "meta.annotation_type": "link",
                "from": "parenttrace"
            }
        }
    ],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "start_new_processes",
    "context": {
        "trace_id": "0x3f6d11237d2b2b8cb987e7ec923a4dc4",
        "span_id": "0x0b133494760fa56d",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0xcf656e5db2b777be",
    "start_time": "2024-11-30T14:01:29.316313Z",
    "end_time": "2024-11-30T14:01:29.316397Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {},
    "events": [],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "task_2",
    "context": {
        "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
        "span_id": "0xe573c104743b6d34",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0x779a3a331684439e",
    "start_time": "2024-11-30T14:01:34.698666Z",
    "end_time": "2024-11-30T14:01:36.002687Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "airflow.category": "scheduler",
        "airflow.task.task_id": "task_2",
        "airflow.task.dag_id": "otel_test_dag_with_pause",
        "airflow.task.state": "success",
        "airflow.task.start_date": "2024-11-30 14:01:35.872318+00:00",
        "airflow.task.end_date": "2024-11-30 14:01:36.002687+00:00",
        "airflow.task.duration": 0.130369,
        "airflow.task.executor_config": "{}",
        "airflow.task.logical_date": "2024-11-30 14:01:15+00:00",
        "airflow.task.hostname": "351295342ba2",
        "airflow.task.log_url": "http://localhost:8080/dags/otel_test_dag_with_pause/grid?dag_run_id=manual__2024-11-30T14%3A01%3A15.333003%2B00%3A00&task_id=task_2&base_date=2024-11-30T14%3A01%3A15%2B0000&tab=logs",
        "airflow.task.operator": "PythonOperator",
        "airflow.task.try_number": 1,
        "airflow.task.executor_state": "success",
        "airflow.task.pool": "default_pool",
        "airflow.task.queue": "default",
        "airflow.task.priority_weight": 1,
        "airflow.task.queued_dttm": "2024-11-30 14:01:34.694842+00:00",
        "airflow.task.queued_by_job_id": 3,
        "airflow.task.pid": 1950
    },
    "events": [
        {
            "name": "task to trigger",
            "timestamp": "2024-11-30T14:01:34.698810Z",
            "attributes": {
                "command": "['airflow', 'tasks', 'run', 'otel_test_dag_with_pause', 'task_2', 'manual__2024-11-30T14:01:15.333003+00:00', '--local', '--subdir', 'DAGS_FOLDER/otel_test_dag_with_pause.py', '--carrier', '{\"traceparent\": \"00-01f441c9c53e793e8808c77939ddbf36-e573c104743b6d34-01\"}']",
                "conf": "{}"
            }
        },
        {
            "name": "airflow.task.queued",
            "timestamp": "2024-11-30T14:01:34.694842Z",
            "attributes": {}
        },
        {
            "name": "airflow.task.started",
            "timestamp": "2024-11-30T14:01:35.872318Z",
            "attributes": {}
        },
        {
            "name": "airflow.task.ended",
            "timestamp": "2024-11-30T14:01:36.002687Z",
            "attributes": {}
        }
    ],
    "links": [
        {
            "context": {
                "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
                "span_id": "0x779a3a331684439e",
                "trace_state": "[]"
            },
            "attributes": {
                "meta.annotation_type": "link",
                "from": "parenttrace"
            }
        }
    ],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "task_1_sub_span",
    "context": {
        "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
        "span_id": "0x7fc9e2289c7df4b8",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0xba9f48dcfac5d40a",
    "start_time": "2024-11-30T14:01:34.321996Z",
    "end_time": "2024-11-30T14:01:34.324249Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "attr1": "val1"
    },
    "events": [],
    "links": [
        {
            "context": {
                "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
                "span_id": "0xba9f48dcfac5d40a",
                "trace_state": "[]"
            },
            "attributes": {
                "meta.annotation_type": "link",
                "from": "parenttrace"
            }
        }
    ],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "emit_metrics",
    "context": {
        "trace_id": "0x3f6d11237d2b2b8cb987e7ec923a4dc4",
        "span_id": "0xa19a88e8dac9645b",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0xcf656e5db2b777be",
    "start_time": "2024-11-30T14:01:29.315255Z",
    "end_time": "2024-11-30T14:01:29.315290Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "total_parse_time": 0.9342440839973278,
        "dag_bag_size": 2,
        "import_errors": 0
    },
    "events": [],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "dag_parsing_loop",
    "context": {
        "trace_id": "0x3f6d11237d2b2b8cb987e7ec923a4dc4",
        "span_id": "0xcf656e5db2b777be",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": null,
    "start_time": "2024-11-30T14:01:28.382690Z",
    "end_time": "2024-11-30T14:01:29.316499Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {},
    "events": [
        {
            "name": "heartbeat",
            "timestamp": "2024-11-30T14:01:29.313549Z",
            "attributes": {}
        },
        {
            "name": "_kill_timed_out_processors",
            "timestamp": "2024-11-30T14:01:29.314763Z",
            "attributes": {}
        },
        {
            "name": "prepare_file_path_queue",
            "timestamp": "2024-11-30T14:01:29.315300Z",
            "attributes": {}
        },
        {
            "name": "start_new_processes",
            "timestamp": "2024-11-30T14:01:29.315941Z",
            "attributes": {}
        },
        {
            "name": "collect_results",
            "timestamp": "2024-11-30T14:01:29.316409Z",
            "attributes": {}
        },
        {
            "name": "print_stat",
            "timestamp": "2024-11-30T14:01:29.316432Z",
            "attributes": {}
        }
    ],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
    """

    # In the example output, there are two parent child relationships.
    #
    # test_dag
    #   |_ task_1
    #       |_ task_1_sub_span
    #   |_ task_2
    #
    # dag_parsing_loop
    #   |_ emit_metrics
    #   |_ start_new_processes

    def test_extract_spans_from_output(self):
        output_lines = self.example_output.splitlines()
        root_span_dict, span_dict = extract_spans_from_output(output_lines)

        assert len(root_span_dict) == 2
        assert len(span_dict) == 7

        expected_root_span_names = ["test_dag", "dag_parsing_loop"]
        actual_root_span_names = []
        for key, value in root_span_dict.items():
            assert key == value["context"]["span_id"]
            assert value["parent_id"] is None
            actual_root_span_names.append(value["name"])

        assert sorted(actual_root_span_names) == sorted(expected_root_span_names)

        expected_span_names = [
            "test_dag",
            "task_1",
            "task_1_sub_span",
            "task_2",
            "dag_parsing_loop",
            "emit_metrics",
            "start_new_processes",
        ]
        actual_span_names = []
        for key, value in span_dict.items():
            assert key == value["context"]["span_id"]
            actual_span_names.append(value["name"])

        assert sorted(actual_span_names) == sorted(expected_span_names)

    def test_get_id_for_a_given_name(self):
        output_lines = self.example_output.splitlines()
        root_span_dict, span_dict = extract_spans_from_output(output_lines)

        span_name_to_test = "test_dag"

        span_id = get_id_for_a_given_name(span_dict, span_name_to_test)

        # Get the id from the two dictionaries, and then cross-reference the name.
        span_from_root_dict = root_span_dict.get(span_id)
        span_from_dict = span_dict.get(span_id)

        assert span_from_root_dict is not None
        assert span_from_dict is not None

        assert span_name_to_test == span_from_root_dict["name"]
        assert span_name_to_test == span_from_dict["name"]

    def test_get_parent_child_dict(self):
        output_lines = self.example_output.splitlines()
        root_span_dict, span_dict = extract_spans_from_output(output_lines)

        parent_child_dict = get_parent_child_dict(root_span_dict, span_dict)

        # There are two root spans. The dictionary should also have length equal to two.
        assert len(parent_child_dict) == 2

        assert sorted(root_span_dict.keys()) == sorted(parent_child_dict.keys())

        for root_span_id, child_spans in parent_child_dict.items():
            # Both root spans have two direct child spans.
            assert len(child_spans) == 2

            root_span = root_span_dict.get(root_span_id)
            root_span_trace_id = root_span["context"]["trace_id"]

            expected_child_span_names = []
            if root_span["name"] == "test_dag":
                expected_child_span_names.extend(["task_1", "task_2"])
            elif root_span["name"] == "dag_parsing_loop":
                expected_child_span_names.extend(["emit_metrics", "start_new_processes"])

            actual_child_span_names = []

            for child_span in child_spans:
                # root_span_id should be the parent.
                assert root_span_id == child_span["parent_id"]
                # all spans should have the same trace_id.
                assert root_span_trace_id == child_span["context"]["trace_id"]
                actual_child_span_names.append(child_span["name"])

            assert sorted(actual_child_span_names) == sorted(expected_child_span_names)

    def test_get_child_list_for_non_root(self):
        output_lines = self.example_output.splitlines()
        root_span_dict, span_dict = extract_spans_from_output(output_lines)

        span_name_to_test = "task_1"
        span_id = get_id_for_a_given_name(span_dict, span_name_to_test)

        assert span_name_to_test == span_dict.get(span_id)["name"]

        # The span isn't a root span.
        assert span_id not in root_span_dict.keys()
        assert span_id in span_dict.keys()

        expected_child_span_names = ["task_1_sub_span"]
        actual_child_span_names = []

        task_1_child_spans = get_child_list_for_non_root(span_dict, "task_1")

        for span in task_1_child_spans:
            actual_child_span_names.append(span["name"])

        assert sorted(actual_child_span_names) == sorted(expected_child_span_names)
