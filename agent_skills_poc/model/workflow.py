# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import re
from dataclasses import dataclass

WORKFLOW_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


class WorkflowValidationError(ValueError):
    """Raised when a workflow extracted from documentation is invalid."""


@dataclass(frozen=True, slots=True)
class Workflow:
    """Internal model used between parser and skill generation layers."""

    id: str
    description: str
    local_command: str
    fallback_command: str

    def __post_init__(self) -> None:
        if not self.id or not WORKFLOW_ID_PATTERN.match(self.id):
            raise WorkflowValidationError("workflow id must match /^[A-Za-z0-9_-]+$/ and cannot be empty")
        if not self.description.strip():
            raise WorkflowValidationError("workflow description cannot be empty")
        if not self.local_command.strip():
            raise WorkflowValidationError("workflow local_command cannot be empty")
        if not self.fallback_command.strip():
            raise WorkflowValidationError("workflow fallback_command cannot be empty")

    def as_skill(self) -> dict[str, object]:
        return {
            "id": self.id,
            "steps": [
                {"type": "local", "command": self.local_command},
                {"type": "fallback", "command": self.fallback_command},
            ],
        }
