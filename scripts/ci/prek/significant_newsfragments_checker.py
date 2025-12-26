#!/usr/bin/env python3
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "docutils>=0.21.2",
#   "jinja2>=3.1.5",
#   "pygments>=2.19.1",
# ]
# ///

from __future__ import annotations

import argparse
import csv
import glob
import re

import docutils.nodes
from docutils.core import publish_doctree
from jinja2 import BaseLoader, Environment

UNDONE_LIST_TEMPLATE = """
---Undone rules found in "{{ filename }}"---
{% if undone_ruff_rules -%}
======Ruff rules======
    {%- for ruld_id, rules in undone_ruff_rules.items() %}
* Code: {{ ruld_id }}
    {% for rule in rules %}* {{ rule }}
    {% endfor %}
    {%- endfor %}
{%- endif -%}
{%- if undone_config_rules %}
======airflow config lint rules======
{% for rule in undone_config_rules %}* {{ rule }}
{% endfor %}
{% endif %}
"""


class SignificantNewsFragmentVisitor(docutils.nodes.NodeVisitor):
    """Visitor to collect significant newsfragement content."""

    TYPES_OF_CHANGE_TITLE = "Types of change"
    EXPECTED_TYPE_OF_CHANGES = {
        "Dag changes",
        "Config changes",
        "API changes",
        "CLI changes",
        "Behaviour changes",
        "Plugin changes",
        "Dependency changes",
        "Code interface changes",
    }
    MIGRATION_RULE_TITLE = "Migration rules needed"
    CONFIG_RULE_TITLE = "airflow config lint"
    RUFF_RULE_TITLE = "ruff"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.types_of_change: dict[str, bool] = {}
        self.ruff_rules: dict[str, list[tuple[bool, str]]] = {}
        self.config_rules: list[tuple[bool, str]] = []

    def visit_list_item(self, node: docutils.nodes.list_item) -> None:
        list_title = node[0].astext()

        for title, extract_func in (
            (self.TYPES_OF_CHANGE_TITLE, self._extract_type_of_changes),
            (self.MIGRATION_RULE_TITLE, self._extract_migration_rules),
        ):
            if list_title == title:
                list_content = node[1]
                if not isinstance(list_content, docutils.nodes.bullet_list):
                    raise ValueError(f"Incorrect format {list_content.astext()}")

                extract_func(list_content)
                break

    def _extract_type_of_changes(self, node: docutils.nodes.bullet_list) -> None:
        for change in node:
            checked, change_type = self._extract_check_list_item(change)
            self.types_of_change[change_type] = checked

        change_types = set(self.types_of_change.keys())
        missing_keys = self.EXPECTED_TYPE_OF_CHANGES - change_types
        if missing_keys:
            raise ValueError(f"Missing type of changes: {missing_keys}")

        unexpected_keys = change_types - self.EXPECTED_TYPE_OF_CHANGES
        if unexpected_keys:
            raise ValueError(f"Unexpected type of changes: {unexpected_keys}")

    def _extract_migration_rules(self, node: docutils.nodes.bullet_list) -> None:
        for sub_node in node:
            if not isinstance(sub_node, docutils.nodes.list_item):
                raise ValueError(f"Incorrect format {sub_node.astext()}")

            list_title = sub_node[0].astext()
            list_content = sub_node[1]

            if list_title == self.CONFIG_RULE_TITLE:
                if not isinstance(list_content, docutils.nodes.bullet_list):
                    raise ValueError(f"Incorrect format {list_content.astext()}")

                self.config_rules = [self._extract_check_list_item(item) for item in list_content]
            elif list_title == self.RUFF_RULE_TITLE:
                if not isinstance(list_content, docutils.nodes.bullet_list):
                    raise ValueError("Incorrect format")

                self._extract_ruff_rules(list_content)

    def _extract_ruff_rules(self, node: docutils.nodes.bullet_list) -> None:
        for ruff_node in node:
            if not isinstance(ruff_node, docutils.nodes.list_item):
                raise ValueError(f"Incorrect format {ruff_node.astext()}")

            ruff_rule_id = ruff_node[0].astext()
            rules_node = ruff_node[1]

            if not isinstance(rules_node, docutils.nodes.bullet_list):
                raise ValueError(f"Incorrect format {rules_node.astext()}")

            self.ruff_rules[ruff_rule_id] = [self._extract_check_list_item(rule) for rule in rules_node]

    def unknown_visit(self, node: docutils.nodes.Node) -> None:
        """Handle other nodes."""

    @staticmethod
    def _extract_check_list_item(node: docutils.nodes.Node) -> tuple[bool, str]:
        if not isinstance(node, docutils.nodes.list_item):
            raise ValueError(f"Incorrect format {node.astext()}")

        text = node.astext()
        if text[0] != "[" or text[2] != "]":
            raise ValueError(
                f"{text} should be a checklist (e.g., * [ ] ``logging.dag_processor_manager_log_location``)"
            )
        return text[:3] == "[x]", text[4:]

    @property
    def formatted_ruff_rules(self) -> str:
        str_repr = ""
        for rule_id, rules in self.ruff_rules.items():
            str_repr += f"**{rule_id}**\n" + "\n".join(f"* {content}" for _, content in rules)
        return str_repr

    @property
    def formatted_config_rules(self) -> str:
        return "\n".join(f"* {content}" for _, content in self.config_rules)

    @property
    def undone_ruff_rules(self) -> dict[str, list[str]]:
        undone_ruff_rules = {}
        for rule_id, rules in self.ruff_rules.items():
            undone_rules = [rule for checked, rule in rules if checked is False]
            if undone_rules:
                undone_ruff_rules[rule_id] = undone_rules
        return undone_ruff_rules

    @property
    def undone_config_rules(self) -> list[str]:
        return [rule[1] for rule in self.config_rules if rule[0] is False]

    @property
    def has_undone_rules(self) -> bool:
        return bool(self.undone_ruff_rules) or bool(self.undone_config_rules)


def parse_significant_newsfragment(source: str) -> SignificantNewsFragmentVisitor:
    document = publish_doctree(source)
    visitor = SignificantNewsFragmentVisitor(document)
    document.walk(visitor)
    return visitor


def parse_newsfragment_file(filename: str, *, export: bool, list_todo: bool) -> None:
    content = newsfragment_file.read()
    description = content.split("* Types of change")[0]
    title = description.split("\n")[0]

    visitor = parse_significant_newsfragment(content)
    if not len(visitor.types_of_change):
        raise ValueError("Missing type of changes")

    if export:
        newsfragment_details.append(
            {
                "AIP or PR name": aip_pr_name,
                "Title": title,
                "Description": description,
            }
            | visitor.types_of_change
            | {
                "Ruff rules": visitor.formatted_ruff_rules,
                "Config rules": visitor.formatted_config_rules,
            }
        )

    if list_todo and visitor.has_undone_rules:
        jinja_loader = Environment(loader=BaseLoader(), autoescape=True)
        undone_msg = jinja_loader.from_string(UNDONE_LIST_TEMPLATE).render(
            filename=filename,
            undone_ruff_rules=visitor.undone_ruff_rules,
            undone_config_rules=visitor.undone_config_rules,
        )
        print(undone_msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize Significant Newsfragments")
    parser.add_argument("--list-todo", help="List undone migration rules", action="store_true")
    parser.add_argument("--export", help="Export the summarization to provided output path in csv format")
    args = parser.parse_args()

    newsfragment_details: list[dict] = []
    for filename in glob.glob("airflow-core/newsfragments/*.significant.rst"):
        if filename == "airflow-core/newsfragments/template.significant.rst":
            continue

        match = re.search(r"airflow-core/newsfragments/(.*)\.significant\.rst", filename)
        if not match:
            raise ValueError()
        aip_pr_name = match.group(1)

        with open(filename) as newsfragment_file:
            try:
                parse_newsfragment_file(filename, export=args.export, list_todo=args.list_todo)
            except ValueError as e:
                print(f'Error found in "{filename}"')
                raise e

    if args.export:
        with open(args.export, "w") as summary_file:
            writer = csv.DictWriter(summary_file, fieldnames=newsfragment_details[0].keys())
            writer.writeheader()
            writer.writerows(newsfragment_details)
