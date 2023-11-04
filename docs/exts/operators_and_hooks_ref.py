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

import ast
import os
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Iterator

import jinja2
import rich_click as click
import yaml
from docutils import nodes

# No stub exists for docutils.parsers.rst.directives. See https://github.com/python/typeshed/issues/5755.
from docutils.parsers.rst import Directive, directives  # type: ignore[attr-defined]
from docutils.statemachine import StringList
from provider_yaml_utils import get_provider_yaml_paths, load_package_data
from sphinx.util import nested_parse_with_titles
from sphinx.util.docutils import switch_source_input

if TYPE_CHECKING:
    from docutils.nodes import Element

CMD_OPERATORS_AND_HOOKS = "operators-and-hooks"

CMD_TRANSFERS = "transfers"

"""
Directives for rendering tables with operators.

To test the template rendering process, you can also run this script as a standalone program.

    PYTHONPATH=$PWD/../ python exts/operators_and_hooks_ref.py --help
"""
DEFAULT_HEADER_SEPARATOR = "="

CURRENT_DIR = Path(os.path.dirname(__file__))
TEMPLATE_DIR = CURRENT_DIR / "templates"
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir, os.pardir))
DOCS_DIR = os.path.join(ROOT_DIR, "docs")


@lru_cache(maxsize=None)
def _get_jinja_env():
    loader = jinja2.FileSystemLoader(TEMPLATE_DIR, followlinks=True)
    env = jinja2.Environment(loader=loader, undefined=jinja2.StrictUndefined)
    return env


def _render_template(template_name, **kwargs):
    return _get_jinja_env().get_template(template_name).render(**kwargs)


def _docs_path(filepath: str):
    if not filepath.startswith("/docs/"):
        raise Exception(f"The path must starts with '/docs/'. Current value: {filepath}")

    if not filepath.endswith(".rst"):
        raise Exception(f"The path must ends with '.rst'. Current value: {filepath}")

    if filepath.startswith("/docs/apache-airflow-providers-"):
        _, _, provider, rest = filepath.split("/", maxsplit=3)
        filepath = f"{provider}:{rest}"
    else:
        filepath = os.path.join(ROOT_DIR, filepath.lstrip("/"))
        filepath = os.path.relpath(filepath, DOCS_DIR)

    len_rst = len(".rst")
    filepath = filepath[:-len_rst]
    return filepath


def _prepare_resource_index(package_data, resource_type):
    return {
        integration["integration-name"]: {**integration, "package-name": provider["package-name"]}
        for provider in package_data
        for integration in provider.get(resource_type, [])
    }


def _prepare_operators_data(tags: set[str] | None):
    package_data = load_package_data()
    all_integrations = _prepare_resource_index(package_data, "integrations")
    if tags is None:
        to_display_integration = all_integrations.values()
    else:
        to_display_integration = [
            integration for integration in all_integrations.values() if tags.intersection(integration["tags"])
        ]

    all_operators_by_integration = _prepare_resource_index(package_data, "operators")
    all_hooks_by_integration = _prepare_resource_index(package_data, "hooks")
    all_sensors_by_integration = _prepare_resource_index(package_data, "sensors")
    results = []

    for integration in to_display_integration:
        item = {
            "integration": integration,
        }
        operators = all_operators_by_integration.get(integration["integration-name"])
        sensors = all_sensors_by_integration.get(integration["integration-name"])
        hooks = all_hooks_by_integration.get(integration["integration-name"])

        if "how-to-guide" in item["integration"]:
            item["integration"]["how-to-guide"] = [_docs_path(d) for d in item["integration"]["how-to-guide"]]
        if operators:
            item["operators"] = operators
        if sensors:
            item["sensors"] = sensors
        if hooks:
            item["hooks"] = hooks
        if operators or sensors or hooks:
            results.append(item)

    return sorted(results, key=lambda d: d["integration"]["integration-name"].lower())


def _render_operator_content(*, tags: set[str] | None, header_separator: str):
    tabular_data = _prepare_operators_data(tags)
    return _render_template(
        "operators_and_hooks_ref.rst.jinja2", items=tabular_data, header_separator=header_separator
    )


def _prepare_transfer_data(tags: set[str] | None):
    package_data = load_package_data()
    all_operators_by_integration = _prepare_resource_index(package_data, "integrations")
    # Add edge case
    for name in ["SQL", "Local"]:
        all_operators_by_integration[name] = {"integration-name": name}
    all_transfers = [
        {
            **transfer,
            "package-name": provider["package-name"],
            "source-integration": all_operators_by_integration[transfer["source-integration-name"]],
            "target-integration": all_operators_by_integration[transfer["target-integration-name"]],
        }
        for provider in package_data
        for transfer in provider.get("transfers", [])
    ]
    if tags is None:
        to_display_transfers = all_transfers
    else:
        to_display_transfers = [
            transfer
            for transfer in all_transfers
            if tags.intersection(transfer["source-integration"].get("tags", set()))
            or tags.intersection(transfer["target-integration"].get("tags", set()))
        ]

    for transfer in to_display_transfers:
        if "how-to-guide" in transfer:
            transfer["how-to-guide"] = _docs_path(transfer["how-to-guide"])
    return to_display_transfers


def _render_transfer_content(*, tags: set[str] | None, header_separator: str):
    tabular_data = _prepare_transfer_data(tags)
    return _render_template(
        "operators_and_hooks_ref-transfers.rst.jinja2", items=tabular_data, header_separator=header_separator
    )


def iter_deferrable_operators(module_filename: str) -> Iterator[tuple[str, str]]:
    ast_obj = ast.parse(open(module_filename).read())
    cls_nodes = (node for node in ast.iter_child_nodes(ast_obj) if isinstance(node, ast.ClassDef))
    init_method_nodes = (
        (cls_node, node)
        for cls_node in cls_nodes
        for node in ast.iter_child_nodes(cls_node)
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )
    for cls_node, node in init_method_nodes:
        args = node.args
        for argument in [*args.args, *args.kwonlyargs]:
            if argument.arg == "deferrable":
                module_name = module_filename.replace("/", ".")[:-3]
                op_name = cls_node.name
                yield (module_name, op_name)


def _render_deferrable_operator_content(*, header_separator: str):
    providers = []
    for provider_yaml_path in get_provider_yaml_paths():
        provider_parent_path = Path(provider_yaml_path).parent
        provider_info: dict[str, Any] = {"name": "", "operators": []}
        for root, _, file_names in os.walk(provider_parent_path):
            if "operators" in root or "sensors" in root:
                for file_name in file_names:
                    if file_name.endswith(".py") and file_name != "__init__.py":
                        provider_info["operators"].extend(
                            iter_deferrable_operators(f"{os.path.relpath(root)}/{file_name}")
                        )

        if provider_info["operators"]:
            provider_yaml_content = yaml.safe_load(Path(provider_yaml_path).read_text())
            provider_info["name"] = provider_yaml_content["package-name"]
            providers.append(provider_info)
    return _render_template("deferrable_operators_list.rst.jinja2", providers=providers)


class BaseJinjaReferenceDirective(Directive):
    """The base directive for OperatorsHooksReferenceDirective and TransfersReferenceDirective"""

    optional_arguments = 1
    option_spec = {"tags": directives.unchanged, "header-separator": directives.unchanged_required}

    def run(self):
        tags_arg = self.options.get("tags")
        tags = {t.strip() for t in tags_arg.split(",")} if tags_arg else None

        header_separator = self.options.get("header-separator")
        new_content = self.render_content(tags=tags, header_separator=header_separator)

        with switch_source_input(self.state, self.content):
            new_content = StringList(new_content.splitlines(), source="")
            node: Element = nodes.section()
            # necessary so that the child nodes get the right source/line set
            node.document = self.state.document
            nested_parse_with_titles(self.state, new_content, node)

        # record all filenames as dependencies -- this will at least
        # partially make automatic invalidation possible
        for filepath in get_provider_yaml_paths():
            self.state.document.settings.record_dependencies.add(filepath)

        return node.children

    def render_content(self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR):
        """Return content in RST format"""
        raise NotImplementedError("You need to override render_content method.")


def _common_render_list_content(*, header_separator: str, resource_type: str, template: str):
    tabular_data = {
        provider["package-name"]: {
            "name": provider["name"],
            resource_type: provider.get(resource_type, []),
        }
        for provider in load_package_data()
        if provider.get(resource_type) is not None
    }
    return _render_template(template, items=tabular_data, header_separator=header_separator)


class OperatorsHooksReferenceDirective(BaseJinjaReferenceDirective):
    """Generates a list of operators, sensors, hooks"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        return _render_operator_content(
            tags=tags,
            header_separator=header_separator,
        )


class TransfersReferenceDirective(BaseJinjaReferenceDirective):
    """Generate a list of transfer operators"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        return _render_transfer_content(
            tags=tags,
            header_separator=header_separator,
        )


class LoggingDirective(BaseJinjaReferenceDirective):
    """Generate list of logging handlers"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        return _common_render_list_content(
            header_separator=header_separator, resource_type="logging", template="logging.rst.jinja2"
        )


class AuthBackendDirective(BaseJinjaReferenceDirective):
    """Generate list of auth backend handlers"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        return _common_render_list_content(
            header_separator=header_separator,
            resource_type="auth-backends",
            template="auth_backend.rst.jinja2",
        )


class AuthConfigurations(BaseJinjaReferenceDirective):
    """Generate list of configurations"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        tabular_data = [
            (provider["name"], provider["package-name"])
            for provider in load_package_data()
            if provider.get("config") is not None
        ]
        return _render_template(
            "configuration.rst.jinja2", items=tabular_data, header_separator=header_separator
        )


class SecretsBackendDirective(BaseJinjaReferenceDirective):
    """Generate list of secret backend handlers"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        return _common_render_list_content(
            header_separator=header_separator,
            resource_type="secrets-backends",
            template="secret_backend.rst.jinja2",
        )


class ConnectionsDirective(BaseJinjaReferenceDirective):
    """Generate list of connections"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        return _common_render_list_content(
            header_separator=header_separator,
            resource_type="connection-types",
            template="connections.rst.jinja2",
        )


class ExtraLinksDirective(BaseJinjaReferenceDirective):
    """Generate list of extra links"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        return _common_render_list_content(
            header_separator=header_separator, resource_type="extra-links", template="extra_links.rst.jinja2"
        )


class NotificationsDirective(BaseJinjaReferenceDirective):
    """Generate list of notifiers"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        return _common_render_list_content(
            header_separator=header_separator,
            resource_type="notifications",
            template="notifications.rst.jinja2",
        )


class ExecutorsDirective(BaseJinjaReferenceDirective):
    """Generate list of executors"""

    def render_content(
        self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR
    ) -> str:
        return _common_render_list_content(
            header_separator=header_separator, resource_type="executors", template="executors.rst.jinja2"
        )


class DeferrableOperatorDirective(BaseJinjaReferenceDirective):
    """Generate list of deferrable operators"""

    def render_content(self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR):
        return _render_deferrable_operator_content(
            header_separator=header_separator,
        )


def setup(app):
    """Setup plugin"""
    app.add_directive("operators-hooks-ref", OperatorsHooksReferenceDirective)
    app.add_directive("transfers-ref", TransfersReferenceDirective)
    app.add_directive("airflow-logging", LoggingDirective)
    app.add_directive("airflow-auth-backends", AuthBackendDirective)
    app.add_directive("airflow-configurations", AuthConfigurations)
    app.add_directive("airflow-secrets-backends", SecretsBackendDirective)
    app.add_directive("airflow-connections", ConnectionsDirective)
    app.add_directive("airflow-extra-links", ExtraLinksDirective)
    app.add_directive("airflow-notifications", NotificationsDirective)
    app.add_directive("airflow-executors", ExecutorsDirective)
    app.add_directive("airflow-deferrable-operators", DeferrableOperatorDirective)

    return {"parallel_read_safe": True, "parallel_write_safe": True}


option_tag = click.option(
    "--tag",
    multiple=True,
    help="If passed, displays integrations that have a matching tag",
)

option_header_separator = click.option(
    "--header-separator", default=DEFAULT_HEADER_SEPARATOR, show_default=True
)


@click.group(context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 500})
def cli():
    """Render tables with integrations"""


@cli.command()
@option_tag
@option_header_separator
def operators_and_hooks(tag: Iterable[str], header_separator: str):
    """Renders Operators ahd Hooks content"""
    print(_render_operator_content(tags=set(tag) if tag else None, header_separator=header_separator))


@cli.command()
@option_tag
@option_header_separator
def transfers(tag: Iterable[str], header_separator: str):
    """Renders Transfers content"""
    print(_render_transfer_content(tags=set(tag) if tag else None, header_separator=header_separator))


@cli.command()
@option_header_separator
def logging(header_separator: str):
    """Renders Logger content"""
    print(
        _common_render_list_content(
            header_separator=header_separator, resource_type="logging", template="logging.rst.jinja2"
        )
    )


@cli.command()
@option_header_separator
def auth_backends(header_separator: str):
    """Renders Logger content"""
    print(
        _common_render_list_content(
            header_separator=header_separator,
            resource_type="auth-backends",
            template="auth_backend.rst.jinja2",
        )
    )


@cli.command()
@option_header_separator
def secret_backends(header_separator: str):
    """Renders Secret Backends content"""
    print(
        _common_render_list_content(
            header_separator=header_separator,
            resource_type="secrets-backends",
            template="secret_backend.rst.jinja2",
        )
    )


@cli.command()
@option_header_separator
def connections(header_separator: str):
    """Renders Connections content"""
    print(
        _common_render_list_content(
            header_separator=header_separator,
            resource_type="connection-types",
            template="connections.rst.jinja2",
        )
    )


@cli.command()
@option_header_separator
def extra_links(header_separator: str):
    """Renders Extra  links content"""
    print(
        _common_render_list_content(
            header_separator=header_separator, resource_type="extra-links", template="extra_links.rst.jinja2"
        )
    )


@cli.command()
@option_tag
@option_header_separator
def deferrable_operators(tag: Iterable[str], header_separator: str):
    """Renders Deferrable Operators content"""
    print(_render_deferrable_operator_content(header_separator=header_separator))


if __name__ == "__main__":
    cli()
