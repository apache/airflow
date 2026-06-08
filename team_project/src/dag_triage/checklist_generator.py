"""checklist_generator.py
Milestone Mavericks, CSS 566A, UW Bothell, Spring 2026.

Generates a categorized remediation checklist from RemediationKB
lookup results. Each checklist item is session-local with no
persistence in v1.
"""

from dataclasses import dataclass, field
from dag_triage.remediation_kb import RemediationKB, Remediation


@dataclass
class ChecklistItem:
    step: str
    tried: bool = False


@dataclass
class Checklist:
    category: str
    items: list[ChecklistItem] = field(default_factory=list)

    def mark_tried(self, index: int) -> None:
        if 0 <= index < len(self.items):
            self.items[index].tried = True


def generate_checklist(category: str, log_signature: str = "") -> Checklist:
    """Return a Checklist for the given failure category.

    Pulls remediation steps from RemediationKB and flattens
    all steps into ChecklistItems. Session-local only; no
    persistence in v1.
    """
    kb = RemediationKB()
    remediations: list[Remediation] = kb.lookup(category, log_signature)

    checklist = Checklist(category=category)
    for remediation in remediations:
        for step in remediation.steps:
            checklist.items.append(ChecklistItem(step=step))

    return checklist
