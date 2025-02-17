<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [Main branch is Airflow 3](#main-branch-is-airflow-3)
- [Contributors](#contributors)
  - [Developing for providers and Helm chart](#developing-for-providers-and-helm-chart)
  - [Developing for Airflow 3 and 2.10.x / 2.11.x](#developing-for-airflow-3-and-210x--211x)
  - [Developing for Airflow 3](#developing-for-airflow-3)
  - [Developing for Airflow 2.10.x](#developing-for-airflow-210x)
  - [Developing for Airflow 2.11](#developing-for-airflow-211)
- [Committers / PMCs](#committers--pmcs)
  - [Merging PRs for providers and Helm chart](#merging-prs-for-providers-and-helm-chart)
  - [Merging PR for Airflow 3 and 2.10.x / 2.11.x](#merging-pr-for-airflow-3-and-210x--211x)
  - [How to backport PR with GitHub Actions](#how-to-backport-pr-with-github-actions)
  - [How to backport PR with `cherry-picker` CLI](#how-to-backport-pr-with-cherry-picker-cli)
  - [Merging PRs 2.10.x](#merging-prs-210x)
  - [Merging PRs for Airflow 3](#merging-prs-for-airflow-3)
  - [Merging PRs for Airflow 2.11](#merging-prs-for-airflow-211)
- [Milestones for PR](#milestones-for-pr)
  - [Set 2.10.x milestone](#set-210x-milestone)
  - [Set 2.11 milestone](#set-211-milestone)
  - [Set 3 milestone](#set-3-milestone)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Main branch is Airflow 3

The `main` branch is for development of Airflow 3.
Airflow 2.10.x releases will be cut from `v2-10-stable` branch.
Airflow 2.11.x releases will be cut from `v2-11-stable` branch.

# Contributors

The following section explains to which branches you should target your PR.

## Developing for providers and Helm chart

PRs should target `main` branch.
Make sure your code is only about Providers or Helm chart.
Avoid mixing core changes into the same PR

> [!NOTE]
> Please note that providers have been relocated from `airflow/providers` to `providers/<provider_id>/src/airflow/providers`.

## Developing for Airflow 3 and 2.10.x / 2.11.x

If the PR is relevant for both Airflow 3 and 2, it should target `main` branch.

> [!IMPORTANT]
> The mental model of Airflow 2.11 is a bridge release for Airflow 3.
> As a result, Airflow 2.11 is not planned to introduce new features other than ones relevant to the bridge release for Airflow 3.
> That said, we recognize that there may be exceptions.
> If you believe a specific feature is a must-have for Airflow 2.11, you will need to raise this as a discussion thread on the mailing list.
> Points to address to make your case:
>
> 1. You must clarify the urgency, specifically why it can't wait for Airflow 3.
> 2. You need to be willing to deliver the feature for both the `main` branch and the Airflow 2.11 branch.
> 3. You must be willing to provide support for future bug fixes as needed.
>
> Points to consider on how PMC members evaluate the request for exception:
>
> 1. Feature impact - Is it really urgent? How many are affected?
> 2. Workarounds - Are there any?
> 3. Scope of change - Both in code lines / number of files and components changed.
> 4. Centrality - Is the feature at the heart of Airflow (scheduler, dag parser) or peripheral.
> 5. Identity of the requester - Is the request from/supported by a member of the community?
> 6. Approved cases with similar details in the past.
> 7. Other considerations that may be raised by PMC members depending on the case.

## Developing for Airflow 3

PRs should target `main` branch.

## Developing for Airflow 2.10.x

PR should target `v2-10-test` branch. When cutting a new release for 2.10 release manager
will sync `v2-10-test`  branch to `v2-10-stable` and cut the release from the stable branch.
PR should never target `v2-10-stable` unless specifically instructed by release manager.

## Developing for Airflow 2.11

Version 2.11 is planned to be cut from `v2-10-stable` branch.
The version will contain features relevant as bridge release for Airflow 3.
We will not backport other features from `main` branch to 2.11.

> [!WARNING]
> Airflow 2.11 policy may change as its release becomes closer.

# Committers / PMCs

The following sections explains the protocol for merging PRs.

## Merging PRs for providers and Helm chart

Make sure PR targets `main` branch.
Avoid merging PRs that involve (providers + core) or (helm chart + core).
Core parts should be extracted to a separate PR.
Exclusions should be pre-approved specifically with a comment by release manager.
Do not treat PR approval (Green V) as exclusion approval.

## Merging PR for Airflow 3 and 2.10.x / 2.11.x

The committer who merges the PR is responsible for backporting the PR to `v2-10-test`.
It means that they should create a new PR where the original commit from main is cherry-picked and take care for resolving conflicts.
If the cherry-pick is too complex, then ask the PR author / start your own PR against `v2-10-test` directly with the change.
Note: tracking that the PRs merged as expected is the responsibility of committer who merged the PR.

Committer may also request from PR author to raise 2 PRs one against `main` branch and one against `v2-10-test` prior to accepting the code change.

Mistakes happen, and such backport PR work might fall through cracks. Therefore, if the committer thinks that certain PRs should be backported, they should set 2.10.x milestone for them.

This way release manager can verify (as usual) if all the "expected" PRs have been backported and cherry-pick remaining PRS.


We are using `cherry-picker` - a [tool](https://github.com/python/cherry-picker) that has been developed by
Python developers. It allows to easily cherry-pick PRs from one branch to another. It works both - via
command line and via GitHub Actions interface.

## How to backport PR with GitHub Actions

When you want to backport commit via GitHub actions (you need to be a committer), you
should use "Backport commit" action. You need to know the commit hash of the commit you want to backport.
You can pin the workflow from the list of workflows for easy access to it.

> [!NOTE]
> It should be the commit hash of the commit in the `main` branch, not in the original PR - you can find it
> via `git log` or looking up main History.

![Backport commit](images/backport_commit_action.png)

Use `main` as source of the workflow and copy the commit hash and enter the target branch name
(e.g. `v2-10-test`).

The action should create a new PR with the cherry-picked commit and add a comment in the PR when it is
successful (or when it fails). If automatic backporting fails because of conflicts, you have to revert to
manual backporting using `cherry-picker` CLI.

## How to backport PR with `cherry-picker` CLI

Backporting via CLI might be more convenient for some users. Also it is necessary if you want to backport
PR that has conflicts. It also allows to backport commit to multiple branches in the same command.

To backport PRs to any branch (for example v2-10-test), you can use the following command:

It's easiest to install it (and keep cherry-picker up-to-date) using `uv tool`:

```bash
uv tool install cherry-picker
````

And upgrade it with:

```bash
uv tool upgrade cherry-picker
```

Then, in order to backport a commit to a branch, you can use the following command:

```bash
cherry-picker COMMIT_SHA BRANCH_NAME1 [BRANCH_NAME2 ...]
```

This will create a new branch with the cherry-picked commit and open a PR against the target branch in
your browser.

If the GH_AUTH environment variable is set in your command line, the cherry-picker automatically creates a new pull request when there are no conflicts. To set GH_AUTH, use the token from your GitHub repository.

To set GH_AUTH run this:

```bash
export GH_AUTH={token}
```

Sometimes it might result with conflict. In such case, you should manually resolve the conflicts.
Some IDEs like IntelliJ has a fantastic conflict resolution tool - just follow `Git -> Resolve conflicts`
menu after you get the conflict. But you can also resolve the conflicts manually (git adds `<<<<<<<`, `=======` and
`>>>>>>>` markers to the files with conflicts).

```bash
cherry_picker --status  # Should show if all conflicts are resolved
cherry_picker --continue  # Should continue cherry-picking process
```

> [!WARNING]
> Sometimes, when you stop cherry-picking process in the middle, you might end up with your repo in a bad
> state and cherry-picker might print this message:
>
> > 🐍 🍒 ⛏
> >
> > You're not inside a cpython repo right now! 🙅
>
> You should then run `cherry-picker --abort` to clean up the mess and start over. If that does not work
> you might need to run `git config --local --remove-section cherry-picker` to clean up the configuration
> stored in `.git/config`.

## Merging PRs 2.10.x

Make sure PR targets `v2-10-test` branch and merge it when ready.
Make sure your PRs target the `v2-10-test` branch, and it can be merged when ready.
All regular protocols of merging considerations are applied.

## Merging PRs for Airflow 3

Make sure PR target `main` branch.

### PRs that involve breaking changes

Our goal is to avoid breaking changes whenever possible. Therefore, we should allow time for community members to review PRs that contain such changes - please avoid rushing to merge them. Also, please make sure that such PRs contain a `significant` newsfragment that contains `**Breaking Change**`.

## Merging PRs for Airflow 2.11

TBD

# Milestones for PR

## Set 2.10.x milestone

Milestone will be added only to the original PR.

1. PR targeting `v2-10-test` directly - milestone will be on that PR.
2. PR targeting `main` with backport PR targeting `v2-10-test`. Milestone will be added only on the PR targeting `v2-10-main`.

## Set 2.11 milestone

TBD
For now, similar procedure as 2.10.x

## Set 3 milestone

Set for any feature that targets Airflow 3 only.
