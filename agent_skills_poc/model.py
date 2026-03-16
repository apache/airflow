# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
import re


WORKFLOW_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")


class WorkflowValidationError(ValueError):
    """Raised when a workflow extracted from documentation is invalid."""


@dataclass(frozen=True, slots=True)
class Workflow:
    """Structured workflow model extracted from executable documentation."""

    id: str
    description: str
    local: str
    fallback: str

    def __post_init__(self) -> None:
        if not self.id or not WORKFLOW_ID_PATTERN.match(self.id):
            raise WorkflowValidationError("workflow id must match /^[a-z0-9][a-z0-9-]*$/ and cannot be empty")
        if not self.description.strip():
            raise WorkflowValidationError("workflow description cannot be empty")
        if not self.local.strip():
            raise WorkflowValidationError("workflow local command cannot be empty")
        if not self.fallback.strip():
            raise WorkflowValidationError("workflow fallback command cannot be empty")
