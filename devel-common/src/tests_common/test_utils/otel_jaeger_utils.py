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
"""Helpers for spans fetched from the Jaeger REST API."""

from __future__ import annotations

from collections import defaultdict


def get_span_tags(span: dict) -> dict:
    return {t["key"]: t["value"] for t in span.get("tags", [])}


def get_parent_span_id(span: dict) -> str | None:
    """Return the spanID of the span's CHILD_OF parent, or None if it has no parent."""
    # Example:
    #
    # 'references': [{
    #       'refType': 'CHILD_OF',
    #       'traceID': 'f94df3c57603264e8247844c6c95d115',
    #       'spanID': '16737863fd381188'
    # }]
    for ref in span.get("references", []):
        if ref["refType"] == "CHILD_OF":
            return ref["spanID"]
    return None


def get_descendant_span_names(parent_children_map: dict[str, list[dict]], span_id: str) -> set[str]:
    """Recursively collect the operationNames of every span below span_id."""
    names = set()
    for child in parent_children_map.get(span_id, []):
        names.add(child["operationName"])
        names |= get_descendant_span_names(parent_children_map, child["spanID"])
    return names


def provided_child_spans_found_under_span(trace: dict, span_name: str, child_span_names: list[str]) -> bool:
    """Return whether every child span appears in a Jaeger trace as a descendant of a given span_name."""
    # Dictionary of every span id, mapped to its direct children spans for the provided trace.
    parent_children_map: dict[str, list[dict]] = defaultdict(list)
    parent_span_id = None
    for span in trace["spans"]:
        span_parent_id = get_parent_span_id(span)
        if span_parent_id is not None:
            parent_children_map[span_parent_id].append(span)
        if span["operationName"] == span_name:
            parent_span_id = span["spanID"]

    if parent_span_id is None:
        return False

    descendant_names = get_descendant_span_names(parent_children_map, parent_span_id)
    return all(name in descendant_names for name in child_span_names)
