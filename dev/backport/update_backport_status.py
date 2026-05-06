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

import requests


def get_success_comment(branch: str, pr_url: str, pr_number: str):
    shield_url = f"https://img.shields.io/badge/PR-{pr_number}-blue"
    comment = f"""### Backport successfully created: {branch}

Note: As of [Merging PRs targeted for Airflow 3.X](https://github.com/apache/airflow/blob/main/dev/README_AIRFLOW3_DEV.md#merging-prs-targeted-for-airflow-3x)
the committer who merges the PR is responsible for backporting the PRs that are bug fixes (generally speaking) to the maintenance branches.

In matter of doubt please ask in [#release-management](https://apache-airflow.slack.com/archives/C03G9H97MM2) Slack channel.

<table>
        <tr>
        <th>Status</th>
        <th>Branch</th>
        <th>Result</th>
    </tr>
    <tr>
        <td>✅</td>
        <td>{branch}</td>
        <td><a href="{pr_url}"><img src="{shield_url}" alt="PR Link"></a></td>
    </tr>
</table>"""
    return comment


def get_failure_comment(branch: str, commit_sha_url: str, commit_sha: str):
    commit_shield_url = f"https://img.shields.io/badge/Commit-{commit_sha[:7]}-red"
    comment = f"""### Backport failed to create: {branch}. View the failure log <a href='https://github.com/{os.getenv("REPOSITORY")}/actions/runs/{os.getenv("RUN_ID")}'> Run details </a>

Note: As of [Merging PRs targeted for Airflow 3.X](https://github.com/apache/airflow/blob/main/dev/README_AIRFLOW3_DEV.md#merging-prs-targeted-for-airflow-3x)
the committer who merges the PR is responsible for backporting the PRs that are bug fixes (generally speaking) to the maintenance branches.

In matter of doubt please ask in [#release-management](https://apache-airflow.slack.com/archives/C03G9H97MM2) Slack channel.

<table>
    <tr>
        <th>Status</th>
        <th>Branch</th>
        <th>Result</th>
    </tr>
    <tr>
        <td>❌</td>
        <td>{branch}</td>
        <td><a href="{commit_sha_url}"><img src='{commit_shield_url}' alt='Commit Link'></a></td>
    </tr>
</table>

You can attempt to backport this manually by running:

```bash
cherry_picker {commit_sha[:7]} {branch}
```

This should apply the commit to the {branch} branch and leave the commit in conflict state marking
the files that need manual conflict resolution.

After you have resolved the conflicts, you can continue the backport process by running:

```bash
cherry_picker --continue
```

If you don't have cherry-picker installed, see the [installation guide](https://github.com/apache/airflow/blob/main/dev/README_AIRFLOW3_DEV.md#how-to-backport-pr-with-cherry-picker-cli).
"""
    return comment


def add_comments(backport_url: str, target_branch: str, commit_sha: str, source_pr_number: str):
    if backport_url.strip() != "EMPTY":
        pr_number = backport_url.split("/")[-1]
        comment = get_success_comment(branch=target_branch, pr_url=backport_url, pr_number=pr_number)
    else:
        commit_sha_url = f"https://github.com/{os.getenv('REPOSITORY')}/commit/{commit_sha}"
        comment = get_failure_comment(
            branch=target_branch, commit_sha_url=commit_sha_url, commit_sha=commit_sha
        )

    token = os.getenv("GH_TOKEN")
    comment_url = f"https://api.github.com/repos/{os.getenv('REPOSITORY')}/issues/{source_pr_number}/comments"

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    data = {"body": comment}

    try:
        response = requests.post(comment_url, headers=headers, json=data)
        if not (response.status_code == 201):
            raise requests.HTTPError(response=response)
    except requests.HTTPError as e:
        print(e)
        print(f"Error: Failed to add comments to pr {source_pr_number}")
        sys.exit(e.response.status_code)


if __name__ == "__main__":
    bp_url = sys.argv[1]
    commit = sys.argv[2]
    tg_branch = sys.argv[3]
    source_pr = sys.argv[4]
    add_comments(backport_url=bp_url, target_branch=tg_branch, commit_sha=commit, source_pr_number=source_pr)
