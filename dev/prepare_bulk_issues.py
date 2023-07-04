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

import csv
import logging
import os
import textwrap
from collections import defaultdict
from time import sleep
from typing import Any

import rich_click as click
from github import Github, GithubException
from jinja2 import BaseLoader
from rich.console import Console
from rich.progress import Progress

logger = logging.getLogger(__name__)

console = Console(width=400, color_system="standard")

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir))


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#    NOTE! GitHub has secondary rate limits for issue creation, and you might be
#          temporarily blocked from creating issues and PRs if you create too many
#          issues in a short time
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


@click.group(context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 500})
def cli():
    ...


def render_template_file(
    template_name: str,
    context: dict[str, Any],
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on its name. Reads the template from <name> file in the current dir.
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
    template = template_env.get_template(template_name)
    content: str = template.render(context)
    return content


def render_template_string(
    template_string: str,
    context: dict[str, Any],
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on its name. Reads the template from <name> file in the current dir.
    :param template_string: string of the template to use
    :param context: Jinja2 context
    :param autoescape: Whether to autoescape HTML
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """
    import jinja2

    template = jinja2.Environment(
        loader=BaseLoader(),
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    ).from_string(template_string)
    content: str = template.render(context)
    return content


option_github_token = click.option(
    "--github-token",
    type=str,
    required=True,
    help=textwrap.dedent(
        """
        GitHub token used to authenticate.
        You can omit it if you have GITHUB_TOKEN env variable set
        Can be generated with:
        https://github.com/settings/tokens/new?description=Write%20issues&scopes=repo:status,public_repo"""
    ),
    envvar="GITHUB_TOKEN",
)

option_dry_run = click.option(
    "--dry-run",
    is_flag=True,
    help="Do not create issues, just print the issues to be created",
)

option_csv_file = click.option(
    "--csv-file",
    type=str,
    required=True,
    help="CSV file to bulk load. The first column is used to group the remaining rows and name them",
)

option_project = click.option(
    "--project",
    type=str,
    help="Project to create issues in",
)

option_repository = click.option(
    "--repository",
    type=str,
    default="apache/airflow",
    help="Repo to use",
)

option_title = click.option(
    "--title",
    type=str,
    required="true",
    help="Title of the issues to create (might contain {{ name }} to indicate the name of the group)",
)

option_labels = click.option(
    "--labels",
    type=str,
    help="Labels to assign to the issues (comma-separated)",
)


option_template_file = click.option(
    "--template-file", type=str, required=True, help="Jinja template file to use for issue content"
)

option_max_issues = click.option("--max-issues", type=int, help="Maximum number of issues to create")

option_start_from = click.option(
    "--start-from",
    type=int,
    default=0,
    help="Start from issue number N (useful if you are blocked by secondary rate limit)",
)


@option_repository
@option_labels
@option_dry_run
@option_title
@option_csv_file
@option_template_file
@option_github_token
@option_max_issues
@option_start_from
@cli.command()
def prepare_bulk_issues(
    github_token: str,
    max_issues: int | None,
    dry_run: bool,
    template_file: str,
    csv_file: str,
    repository: str,
    labels: str,
    title: str,
    start_from: int,
):
    issues: dict[str, list[list[str]]] = defaultdict(list)
    with open(csv_file) as f:
        read_issues = csv.reader(f)
        for index, row in enumerate(read_issues):
            if index == 0:
                continue
            issues[row[0]].append(row)
    names = sorted(issues.keys())[start_from:]
    total_issues = len(names)
    processed_issues = 0
    if dry_run:
        for name in names:
            issue_content, issue_title = get_issue_details(issues, name, template_file, title)
            console.print(f"[yellow]### {issue_title} #####[/]")
            console.print(issue_content)
            console.print()
            processed_issues += 1
            if max_issues is not None:
                max_issues -= 1
                if max_issues == 0:
                    break
        console.print()
        console.print(f"Displayed {processed_issues} issue(s).")
    else:
        labels_list: list[str] = labels.split(",") if labels else []
        issues_to_create = int(min(total_issues, max_issues if max_issues is not None else total_issues))
        with Progress(console=console) as progress:
            task = progress.add_task(f"Creating {issues_to_create} issue(s)", total=issues_to_create)
            g = Github(github_token)
            repo = g.get_repo(repository)
            try:
                for i in range(total_issues):
                    name = names[i]
                    issue_content, issue_title = get_issue_details(issues, name, template_file, title)
                    repo.create_issue(title=issue_title, body=issue_content, labels=labels_list)
                    progress.advance(task)
                    processed_issues += 1
                    sleep(2)  # avoid secondary rate limit!
                    if max_issues is not None:
                        max_issues -= 1
                        if max_issues == 0:
                            break
            except GithubException as e:
                console.print(f"[red]Error!: {e}[/]")
                console.print(
                    f"[yellow]Restart with `--start-from {processed_issues+start_from}` to continue.[/]"
                )
        console.print(f"Created {processed_issues} issue(s).")


def get_issue_details(issues, name, template_file, title):
    rows = issues[name]
    context = {"rows": rows, "name": name}
    issue_title = render_template_string(title, context)
    issue_content = render_template_file(template_name=template_file, context=context)
    return issue_content, issue_title


if __name__ == "__main__":
    cli()
