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
  - [Merging PRs 2.10.x](#merging-prs-210x)
  - [Merging PRs for Airflow 3](#merging-prs-for-airflow-3)
  - [Merging PRs for Airflow 2.11](#merging-prs-for-airflow-211)
- [Milestones for PR](#milestones-for-pr)
  - [Set 2.10.x milestone](#set-210x-milestone)
  - [Set 2.11 milestone](#set-211-milestone)
  - [Set 3 milestone](#set-3-milestone)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Main branch is Airflow 3

The main branch is for development of Airflow 3.
Airflow 2.10.x releases will be cut from `v2-10-stable` branch.
Airflow 2.11.x releases will be cut from `v2-11-stable` branch.

# Contributors

The following section explains to which branches you should target your PR.

## Developing for providers and Helm chart

PRs should target `main` branch.
Make sure your code is only about Providers or Helm chart.
Avoid mixing core changes into the same PR

## Developing for Airflow 3 and 2.10.x / 2.11.x

If the PR is relevant for both Airflow 3 and 2, it should target `main` branch.

Note: The mental model of Airflow 2.11 is bridge release for Airflow 3.
As a result, Airflow 2.11 is not planned to introduce new features other than ones relevant as bridge release for Airflow 3.
That said, we recognize that there may be exceptions.
If you believe a specific feature is a must-have for Airflow 2.11, you will need to raise this as discussion thread on the mailing list.
Points to address to make your case:

1. You must clarify what is the urgency (i.e., why it can't wait for Airflow 3).
2. You need be willing to deliver the feature for both main branch and Airflow 2.11 branch.
3. You must be willing to provide support future bug fixes as needed.

Points to consider on how PMC members evaluate the request of exception:

1. Feature impact - Is it really urgent? How many are affected?
2. Workarounds - Are there any ?
3. Scope of change - Both in code lines / number of files and components changed.
4. Centrality - Is the feature at the heart of Airflow (scheduler, dag parser) or peripheral.
5. Identity of the requester - Is the request from/supported by a member of the community?
6. Similar previous cases approved.
7. Other considerations that may raise by PMC members depending on the case.

## Developing for Airflow 3

PRs should target `main` branch.

## Developing for Airflow 2.10.x

PR should target `v2-10-test` branch. When cutting a new release for 2.10 release manager
will sync `v2-10-test`  branch to `v2-10-stable` and cut the release from the stable branch.
PR should never target `v2-10-stable` unless specifically instructed by release manager.

## Developing for Airflow 2.11

Version 2.11 is planned to be cut from `v2-10-stable` branch.
The version will contain features relevant as bridge release for Airflow 3.
We will not backport otherwise features from main branch to 2.11
Note that 2.11 policy may change as 2.11 becomes closer.

# Committers / PMCs

The following sections explains the protocol for merging PRs.

## Merging PRs for providers and Helm chart

Make sure PR targets `main` branch.
Avoid merging PRs that involve providers + core / helm chart + core
Core part should be extracted to a  separated PR.
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

## Merging PRs 2.10.x

Make sure PR targets `v2-10-test` branch and merge it when ready.
Make sure your PRs target the `v2-10-test` branch, and it can be merged when ready.
All regular protocols of merging considerations are applied.

## Merging PRs for Airflow 3

Make sure PR target `main` branch.

### PRs that involve breaking changes

Make sure it has newsfragment, please allow time for community members to review.
Our goal is to avoid breaking changes whenever possible. Therefore, we should allow time for community members to review PRs that contain such changes - please avoid rushing to merge them. In addition, ensure that these PRs include a newsfragment.

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
