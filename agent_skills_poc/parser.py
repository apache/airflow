# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from io import StringIO
from pathlib import Path

from docutils import nodes
from docutils.frontend import OptionParser
from docutils.parsers.rst import Directive, Parser, directives
from docutils.utils import new_document

from agent_skills_poc.model import Workflow, WorkflowValidationError


class WorkflowParseError(ValueError):
    """Raised when agent-skill directives cannot be parsed from RST."""


class AgentSkillNode(nodes.General, nodes.Element):
    """Docutils node representing one ``agent-skill`` directive."""


class AgentSkillDirective(Directive):
    """Directive that captures workflow metadata from ``.. agent-skill::`` blocks."""

    required_arguments = 0
    optional_arguments = 0
    final_argument_whitespace = False
    has_content = False
    option_spec = {
        "id": directives.unchanged_required,
        "description": directives.unchanged_required,
        "local": directives.unchanged_required,
        "fallback": directives.unchanged_required,
    }

    def run(self) -> list[nodes.Node]:
        node = AgentSkillNode()
        for key, value in self.options.items():
            node[key] = value
        return [node]


def _build_document(source_text: str, source_path: str) -> nodes.document:
    directives.register_directive("agent-skill", AgentSkillDirective)

    parser = Parser()
    settings = OptionParser(components=(Parser,)).get_default_values()
    settings.report_level = 2
    settings.halt_level = 6
    settings.warning_stream = StringIO()

    document = new_document(source_path, settings=settings)
    parser.parse(source_text, document)
    return document


def _raise_if_system_messages(document: nodes.document) -> None:
    messages: list[str] = []
    for system_message in document.traverse(nodes.system_message):
        level = int(system_message.get("level", 0))
        if level >= 3:
            messages.append(system_message.astext().replace("\n", " ").strip())

    if messages:
        raise WorkflowParseError(f"invalid agent-skill syntax: {' | '.join(messages)}")


def parse_workflows_from_text(source_text: str, source_path: str = "<memory>") -> list[Workflow]:
    document = _build_document(source_text=source_text, source_path=source_path)
    _raise_if_system_messages(document)

    workflows: list[Workflow] = []
    for node in document.traverse(AgentSkillNode):
        try:
            workflows.append(
                Workflow(
                    id=node["id"],
                    description=node["description"],
                    local=node["local"],
                    fallback=node["fallback"],
                )
            )
        except (KeyError, WorkflowValidationError) as err:
            raise WorkflowParseError(f"invalid workflow block: {err}") from err

    return workflows


def parse_workflows(rst_path: str | Path) -> list[Workflow]:
    path = Path(rst_path)
    if not path.exists():
        raise WorkflowParseError(f"input file does not exist: {path}")
    return parse_workflows_from_text(path.read_text(encoding="utf-8"), source_path=str(path))
