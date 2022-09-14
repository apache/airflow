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
from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, NamedTuple, cast

import rich_click as click
from github import Github, UnknownObjectException
from github.Milestone import Milestone
from github.PullRequest import PullRequest
from github.Repository import Repository
from rich.console import Console
from rich.prompt import Confirm, Prompt

CHANGELOG_SKIP_LABEL = "changelog:skip"

TYPE_DOC_ONLY_LABEL = "type:doc-only"

logger = logging.getLogger(__name__)

console = Console(width=400, color_system="standard")

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir))
PR_PATTERN = re.compile(r".*\(#([0-9]+)\)")
ISSUE_MATCH_IN_BODY = re.compile(r" #([0-9]+)[^0-9]")

CHANGELOG_CHANGES_FILE = "changelog-changes.txt"
DOC_ONLY_CHANGES_FILE = "doc-only-changes.txt"
EXCLUDED_CHANGES_FILE = "excluded-changes.txt"


@click.group(context_settings={'help_option_names': ['-h', '--help'], 'max_content_width': 500})
def cli():
    ...


option_verbose = click.option(
    "--verbose",
    is_flag=True,
    help="Print verbose information about performed steps",
)

option_assume_yes = click.option(
    "--assume-yes",
    is_flag=True,
    help="Assume yes answer to question",
)


option_previous_release = click.option(
    "--previous-release",
    type=str,
    required=True,
    help="commit reference (for example hash or tag) of the previous release.",
)

option_current_release = click.option(
    "--current-release",
    type=str,
    required=True,
    help="commit reference (for example hash or tag) of the current release.",
)

option_github_token = click.option(
    "--github-token",
    type=str,
    required=True,
    help=textwrap.dedent(
        """
        GitHub token used to authenticate.
        You can set omit it if you have GITHUB_TOKEN env variable set
        Can be generated with:
        https://github.com/settings/tokens/new?description=Read%20Write%20isssues&scopes=repo"""
    ),
    envvar='GITHUB_TOKEN',
)

option_limit_pr_count = click.option(
    "--limit-pr-count",
    type=int,
    default=None,
    help="Limit PR count processes (useful for testing small subset of PRs).",
)

option_dry_run = click.option(
    "--dry-run",
    is_flag=True,
    help="Do not make any changes, just show what would have been done",
)

option_skip_assigned = click.option(
    "--skip-assigned",
    is_flag=True,
    help="Skip PRs already correctly assigned to the right milestone",
)

option_milestone_number = click.option(
    "--milestone-number",
    type=int,
    required=True,
    help="Milestone number to set. See https://github.com/apache/airflow/milestones to find milestone id",
)

option_print_summary = click.option(
    "--print-summary",
    is_flag=True,
    help="Produce summary of the changes cherry-picked in the file specified. Implies --skip-assigned",
)

option_output_folder = click.option(
    "--output-folder",
    type=str,
    help="Folder where files with commit hashes will be store. Implies --print-summary and --skip-assigned",
)


def render_template(
    template_name: str,
    context: dict[str, Any],
    autoescape: bool = False,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on its name. Reads the template from <name>.jinja2 in current dir.
    :param template_name: name of the template to use
    :param context: Jinja2 context
    :param autoescape: Whether to autoescape HTML
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """
    import jinja2

    template_loader = jinja2.FileSystemLoader(searchpath=MY_DIR_PATH)
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    )
    template = template_env.get_template(f"{template_name}.jinja2")
    content: str = template.render(context)
    return content


def get_git_log_command(
    verbose: bool, from_commit: str | None = None, to_commit: str | None = None
) -> list[str]:
    """
    Get git command to run for the current repo from the current folder (which is the package folder).
    :param verbose: whether to print verbose info while getting the command
    :param from_commit: if present - base commit from which to start the log from
    :param to_commit: if present - final commit which should be the start of the log
    :return: git command to run
    """
    git_cmd = [
        "git",
        "log",
        "--pretty=format:%H %h %cd %s",
        "--date=short",
    ]
    if from_commit and to_commit:
        git_cmd.append(f"{from_commit}...{to_commit}")
    elif from_commit:
        git_cmd.append(from_commit)
    git_cmd.extend(['--', '.'])
    if verbose:
        console.print(f"Command to run: '{' '.join(git_cmd)}'")
    return git_cmd


class Change(NamedTuple):
    """Stores details about commits"""

    full_hash: str
    short_hash: str
    date: str
    message: str
    message_without_backticks: str
    pr: int | None


def get_change_from_line(line: str) -> Change:
    split_line = line.split(" ", maxsplit=3)
    message = split_line[3]
    pr = None
    pr_match = PR_PATTERN.match(message)
    if pr_match:
        pr = pr_match.group(1)
    return Change(
        full_hash=split_line[0],
        short_hash=split_line[1],
        date=split_line[2],
        message=message,
        message_without_backticks=message.replace("`", "'").replace("&#39;", "'").replace('&amp;', "&"),
        pr=int(pr) if pr else None,
    )


def get_changes(verbose: bool, previous_release: str, current_release: str) -> list[Change]:
    change_strings = subprocess.check_output(
        get_git_log_command(verbose, from_commit=previous_release, to_commit=current_release),
        cwd=SOURCE_DIR_PATH,
        text=True,
    )
    return [get_change_from_line(line) for line in change_strings.split("\n")]


def update_milestone(r: Repository, pr: PullRequest, m: Milestone):
    # PR in GitHub API does not have a way to update milestone. It should be opened as issue,
    # and then it can be updated ¯\_(ツ)_/¯
    r.get_issue(pr.number).edit(milestone=m)


@cli.command()
@option_github_token
@option_previous_release
@option_current_release
@option_verbose
@option_limit_pr_count
@option_dry_run
@option_milestone_number
@option_skip_assigned
@option_print_summary
@option_assume_yes
@option_output_folder
def assign_prs(
    github_token: str,
    previous_release: str,
    current_release: str,
    verbose: bool,
    limit_pr_count: int | None,
    dry_run: bool,
    milestone_number: int,
    skip_assigned: bool,
    print_summary: bool,
    assume_yes: bool,
    output_folder: str,
):
    changes = get_changes(verbose, previous_release, current_release)
    changes = list(filter(lambda change: change.pr is not None, changes))
    prs = [change.pr for change in changes]

    g = Github(github_token)
    repo = g.get_repo("apache/airflow")

    if output_folder and not print_summary:
        console.print("\n[yellow]Implying --print-summary as output folder is enabled[/]\n")
        print_summary = True
    if print_summary and not skip_assigned:
        console.print("\n[yellow]Implying --skip-assigned as summary report is enabled[/]\n")
        skip_assigned = True
    milestone = repo.get_milestone(milestone_number)
    count_prs = len(prs)
    if limit_pr_count:
        count_prs = limit_pr_count
    console.print(f"\n[green]Applying Milestone: {milestone.title} to {count_prs} merged PRs[/]\n")
    if dry_run:
        console.print("[yellow]Dry run mode![/]\n")
    else:
        if not assume_yes and not Confirm.ask("Is this OK?"):
            sys.exit(1)

    doc_only_label = repo.get_label(TYPE_DOC_ONLY_LABEL)
    changelog_skip_label = repo.get_label(CHANGELOG_SKIP_LABEL)
    changelog_changes: list[Change] = []
    doc_only_changes: list[Change] = []
    excluded_changes: list[Change] = []
    for i in range(count_prs):
        pr_number = prs[i]
        if pr_number is None:
            # Should not happen but MyPy is not happy
            continue
        console.print('-' * 80)
        console.print(
            f"\n >>>> Retrieving PR#{pr_number}: https://github.com/apache/airflow/pull/{pr_number}"
        )
        pr: PullRequest
        try:
            pr = repo.get_pull(pr_number)
        except UnknownObjectException:
            # Fallback to issue if PR not found
            try:
                # PR has almost the same fields as Issue
                pr = cast(PullRequest, repo.get_issue(pr_number))
            except UnknownObjectException:
                console.print(f"[red]The PR #{pr_number} could not be found[/]")
                continue
        console.print(f"\nPR:{pr_number}: {pr.title}\n")
        label_names = [label.name for label in pr.labels]
        already_assigned_milestone_number = pr.milestone.number if pr.milestone else None
        if already_assigned_milestone_number == milestone.number:
            console.print(
                f"[green]The PR #{pr_number} is already "
                f"assigned to the milestone: {pr.milestone.title}[/]. Labels: {label_names}"
            )
            if TYPE_DOC_ONLY_LABEL in label_names:
                console.print("[yellow]It will be classified as doc-only change[/]\n")
                if skip_assigned:
                    doc_only_changes.append(changes[i])
            elif CHANGELOG_SKIP_LABEL in label_names:
                console.print("[yellow]It will be excluded from changelog[/]\n")
                if skip_assigned:
                    excluded_changes.append(changes[i])
            else:
                console.print("[green]The change will be included in changelog[/]\n")
                if skip_assigned:
                    changelog_changes.append(changes[i])
            if skip_assigned:
                continue
        elif already_assigned_milestone_number is not None:
            console.print(
                f"[yellow]The PR #{pr_number} is already "
                f"assigned to another milestone: {pr.milestone.title}[/]. Labels: {label_names}"
            )
        # Ignore doc-only and skipped PRs
        console.print(f"Marking the PR #{pr_number} as {milestone.title}")
        chosen_option = Prompt.ask(
            "Choose action:",
            choices=["a", "add", "d", "doc", "e", "exclude", "s", "skip", "q", "quit"],
            default="skip",
        ).lower()
        if chosen_option in ("add", "a"):
            console.print(f"Adding the PR #{pr_number} to {milestone.title}")
            if not dry_run:
                update_milestone(repo, pr, milestone)
            if skip_assigned:
                changelog_changes.append(changes[i])
        elif chosen_option in ("doc", "d"):
            console.print(f"Applying the label {doc_only_label} the PR #{pr_number}")
            if not dry_run:
                pr.add_to_labels(doc_only_label)
                update_milestone(repo, pr, milestone)
            if skip_assigned:
                doc_only_changes.append(changes[i])
        elif chosen_option in ("exclude", "e"):
            console.print(f"Applying the label {changelog_skip_label} the PR #{pr_number}")
            if not dry_run:
                pr.add_to_labels(changelog_skip_label)
                update_milestone(repo, pr, milestone)
            if skip_assigned:
                excluded_changes.append(changes[i])
        elif chosen_option in ("skip", "s"):
            console.print(f"Skipping the PR #{pr_number}")
        elif chosen_option in ("quit", "q"):
            sys.exit(2)

    if print_summary:
        context = {
            "changelog_changes": changelog_changes,
            "excluded_changes": excluded_changes,
            "doc_only_changes": doc_only_changes,
            "previous_release": previous_release,
            "current_release": current_release,
        }
        console.print(render_template("CHERRY_PICK_SUMMARY.txt", context=context))

    if output_folder:

        def write_commits(type: str, path: Path, changes_to_write: list[Change]):
            path.write_text("\n".join(change.short_hash for change in changes_to_write) + "\n")
            console.print(f"\n{type} commits written in {path}")

        write_commits("Changelog", Path(output_folder) / CHANGELOG_CHANGES_FILE, changelog_changes)
        write_commits("Doc only", Path(output_folder) / DOC_ONLY_CHANGES_FILE, doc_only_changes)
        write_commits("Excluded", Path(output_folder) / EXCLUDED_CHANGES_FILE, excluded_changes)
        console.print("\n")


if __name__ == "__main__":
    cli()
