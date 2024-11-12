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


# Example span from the ConsoleSpanExporter
#     {
#         "name": "perform_heartbeat",
#         "context": {
#             "trace_id": "0xa18781ea597c3d07c85e95fd3a6d7d40",
#             "span_id": "0x8ae7bb13ec5b28ba",
#             "trace_state": "[]"
#         },
#         "kind": "SpanKind.INTERNAL",
#         "parent_id": "0x17ac77a4a840758d",
#         "start_time": "2024-10-30T16:19:33.947155Z",
#         "end_time": "2024-10-30T16:19:33.947192Z",
#         "status": {
#             "status_code": "UNSET"
#         },
#         "attributes": {},
#         "events": [],
#         "links": [],
#         "resource": {
#             "attributes": {
#                 "telemetry.sdk.language": "python",
#                 "telemetry.sdk.name": "opentelemetry",
#                 "telemetry.sdk.version": "1.27.0",
#                 "host.name": "host.local",
#                 "service.name": "Airflow"
#             },
#             "schema_url": ""
#         }
#     }


def extract_spans_from_output(output_lines):
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
    """Create a dictionary with parent-child span relationships."""
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
    """
    parent_span_id = get_id_for_a_given_name(span_dict=span_dict, span_name=span_name)
    parent_span = span_dict.get(parent_span_id)

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
