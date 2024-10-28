from typing import List, Optional

import ydb
from google.protobuf.message import Message


class Warning(Exception):
    pass


class Error(Exception):
    def __init__(
        self,
        message: str,
        original_error: Optional[ydb.Error] = None,
    ):
        super(Error, self).__init__(message)

        self.original_error = original_error
        if original_error:
            pretty_issues = _pretty_issues(original_error.issues)
            self.issues = original_error.issues
            self.message = pretty_issues or message
            self.status = original_error.status


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


def _pretty_issues(issues: List[Message]) -> str:
    if issues is None:
        return None

    children_messages = [_get_messages(issue, root=True) for issue in issues]

    if None in children_messages:
        return None

    return "\n" + "\n".join(children_messages)


def _get_messages(issue: Message, max_depth: int = 100, indent: int = 2, depth: int = 0, root: bool = False) -> str:
    if depth >= max_depth:
        return None

    margin_str = " " * depth * indent
    pre_message = ""
    children = ""

    if issue.issues:
        collapsed_messages = []
        while not root and len(issue.issues) == 1:
            collapsed_messages.append(issue.message)
            issue = issue.issues[0]

        if collapsed_messages:
            pre_message = f"{margin_str}{', '.join(collapsed_messages)}\n"
            depth += 1
            margin_str = " " * depth * indent

        children_messages = [
            _get_messages(iss, max_depth=max_depth, indent=indent, depth=depth + 1) for iss in issue.issues
        ]

        if None in children_messages:
            return None

        children = "\n".join(children_messages)

    return (
        f"{pre_message}{margin_str}{issue.message}\n{margin_str}"
        f"severity level: {issue.severity}\n{margin_str}"
        f"issue code: {issue.issue_code}\n{children}"
    )
