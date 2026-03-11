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

import os
import sys
from enum import Enum

from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.shared_options import get_forced_answer

STANDARD_TIMEOUT = 10


class Answer(Enum):
    YES = "y"
    NO = "n"
    QUIT = "q"


def user_confirm(
    message: str,
    timeout: float | None = None,
    default_answer: Answer | None = Answer.NO,
    quit_allowed: bool = True,
    forced_answer: str | None = None,
) -> Answer:
    """Ask the user for confirmation.

    :param message: message to display to the user (should end with the question mark)
    :param timeout: time given user to answer
    :param default_answer: default value returned on timeout. If no default - is set, the timeout is ignored.
    :param quit_allowed: whether quit answer is allowed
    :param forced_answer: explicit override for forced answer (bypasses global --answer)
    """
    from inputimeout import TimeoutOccurred, inputimeout

    allowed_answers = "y/n/q" if quit_allowed else "y/n"
    while True:
        try:
            force = forced_answer or get_forced_answer() or os.environ.get("ANSWER")
            if force:
                user_status = force
                print(f"Forced answer for '{message}': {force}")
            else:
                if default_answer:
                    # Capitalise default answer
                    allowed_answers = allowed_answers.replace(
                        default_answer.value, default_answer.value.upper()
                    )
                    timeout_answer = default_answer.value
                else:
                    timeout = None
                    timeout_answer = ""
                message_prompt = f"\n{message} \nPress {allowed_answers}"
                if default_answer and timeout:
                    message_prompt += (
                        f". Auto-select {timeout_answer} in {timeout} seconds "
                        f"(add `--answer {default_answer.value}` to avoid delay next time)"
                    )
                message_prompt += ": "
                user_status = inputimeout(
                    prompt=message_prompt,
                    timeout=timeout,
                )
                if user_status == "":
                    if default_answer:
                        return default_answer
                    continue
            if user_status.upper() in ["Y", "YES"]:
                return Answer.YES
            if user_status.upper() in ["N", "NO"]:
                return Answer.NO
            if user_status.upper() in ["Q", "QUIT"] and quit_allowed:
                return Answer.QUIT
            print(f"Wrong answer given {user_status}. Should be one of {allowed_answers}. Try again.")
        except TimeoutOccurred:
            if default_answer:
                return default_answer
            # timeout should only occur when default_answer is set so this should never happened
        except KeyboardInterrupt:
            if quit_allowed:
                return Answer.QUIT
            sys.exit(1)


def confirm_action(
    message: str,
    timeout: float | None = None,
    default_answer: Answer | None = Answer.NO,
    quit_allowed: bool = True,
    abort: bool = False,
) -> bool:
    answer = user_confirm(message, timeout, default_answer, quit_allowed)
    if answer == Answer.YES:
        return True
    if abort:
        sys.exit(1)
    elif answer == Answer.QUIT:
        sys.exit(1)
    return False


class TriageAction(Enum):
    DRAFT = "d"
    COMMENT = "a"
    CLOSE = "c"
    READY = "r"
    SKIP = "s"
    QUIT = "q"


def prompt_triage_action(
    message: str,
    default: TriageAction = TriageAction.DRAFT,
    timeout: float | None = None,
    forced_answer: str | None = None,
) -> TriageAction:
    """Prompt the user to choose a triage action for a flagged PR.

    :param message: message to display (should describe the PR)
    :param default: default action returned on Enter or timeout
    :param timeout: seconds before auto-selecting default (None = no timeout)
    :param forced_answer: explicit override for forced answer (bypasses global --answer)
    """
    from inputimeout import TimeoutOccurred, inputimeout

    _LABELS = {
        TriageAction.DRAFT: "draft",
        TriageAction.COMMENT: "add comment",
        TriageAction.CLOSE: "close",
        TriageAction.READY: "ready",
        TriageAction.SKIP: "skip",
        TriageAction.QUIT: "quit",
    }
    while True:
        try:
            force = forced_answer or get_forced_answer() or os.environ.get("ANSWER")
            if force:
                print(f"Forced answer for '{message}': {force}")
                upper = force.upper()
                if upper in ("Y", "YES"):
                    return default
                if upper in ("N", "NO"):
                    return TriageAction.SKIP
                if upper == "Q":
                    return TriageAction.QUIT
                for action in TriageAction:
                    if upper == action.value.upper():
                        return action
                return default

            # Build choice display: uppercase the default letter
            choices = []
            for action in TriageAction:
                letter = action.value
                label = _LABELS[action]
                if action == default:
                    choices.append(f"[{letter.upper()}]{label}")
                else:
                    choices.append(f"[{letter}]{label}")
            choices_str = " / ".join(choices)

            get_console().print(f"\n{message}")
            prompt_text = choices_str
            if timeout:
                prompt_text += (
                    f"  (auto-select {_LABELS[default]} in {timeout}s"
                    f" — add `--answer-triage {default.value}` to skip)"
                )
            prompt_text += ": "

            user_input = inputimeout(prompt=prompt_text, timeout=timeout)
            if user_input == "":
                return default

            upper = user_input.strip().upper()
            for action in TriageAction:
                if upper == action.value.upper():
                    return action
            print(f"Invalid input '{user_input}'. Please enter one of: d/a/c/r/s/q")
        except TimeoutOccurred:
            return default
        except KeyboardInterrupt:
            return TriageAction.QUIT
