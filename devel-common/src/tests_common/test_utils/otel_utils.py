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

from sqlalchemy import inspect, select

from airflow.models import Base

log = logging.getLogger("integration.otel.otel_utils")


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
            results = [dict(row._mapping) for row in session.execute(select(table)).all()]
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


def extract_task_event_value(line: str) -> str:
    # Parse the JSON line, which starts with '{"timestamp":'.
    data = json.loads(line)
    # Extract the event value.
    raw_event = data.get("event", "")

    # Replace backslash-escaped sequences with simple sequences, e.g. '\"' -> '"', where needed.
    event_decoded = raw_event.encode("utf-8").decode("unicode_escape")

    # Don't perform any other transformations so that indentation is kept intact.
    # The next processing depends on it, otherwise it won't handle the span data correctly.

    return event_decoded


def clean_task_lines(lines: list) -> list:
    r"""
    Clean up the task lines.

    Each task line follows this format
    '{"timestamp":"2025-03-31T18:03:17.098657Z","level":"info","event":"    \"name\": \"task1_sub_span3\",","chan":"stdout","logger":"task"}'.
    The actual output is on the value of the 'event' fragment.

    For each log line that starts with '{"timestamp":', parse and
    extract the 'event' JSON fragment up until the enclosing quote. Indentation should be preserved.
    """
    cleaned_lines = []
    # Iterate over all lines to identify the ones belonging to tasks.
    for line in lines:
        if line.startswith('{"timestamp":'):
            event_frag = extract_task_event_value(line)
            cleaned_lines.append(event_frag)
        else:
            # The rest of the lines will be processed later.
            cleaned_lines.append(line)
    return cleaned_lines


def extract_spans_from_output(output_lines: list):
    """
    For a given list of ConsoleSpanExporter output lines, it extracts the json spans and creates two dictionaries.

    :return: root spans dict (key: root_span_id - value: root_span), spans dict (key: span_id - value: span)
    """
    span_dict = {}
    root_span_dict = {}
    total_lines = len(output_lines)
    index = 0
    output_lines = clean_task_lines(output_lines)
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
                orig_line = str(line)
                striped_line = line.lstrip()
                if '"command":' not in line:
                    if orig_line == "{" or orig_line == "}":
                        json_lines.append(line)
                    # In certain cases, irrelevant logs might interrupt the json span printing.
                    # This will result in an error while parsing the json. Ignore these lines.
                    if striped_line != orig_line:
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
        assert name in names_from_dict, (
            f"Span name '{name}' wasn't found in children span names. It's not a child of span '{parent_name}'."
        )


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
        assert name in names_from_dict, (
            f"Span name '{name}' wasn't found in children span names. It's not a child of span '{parent_name}'."
        )


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
        assert child_id not in child_span_id_list, (
            f"Span '{child_name}' shouldn't be a child of span '{parent_name}', but it is."
        )
    else:
        assert child_id is None, f"Span '{child_name}' shouldn't exist but it does."
