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
Airflow 2.10.x releases will be cut from `v-2-10-stable` branch.

# Contributors

The following section explains which branches you should target your PR to

## Developing for providers and Helm chart

PRs should target `main` branch.
Make sure your code is only about Providers or Helm chart.
Avoid mixing core changes into the same PR

## Developing for Airflow 3 and 2.10.x / 2.11.x

If the PR is relevant for both Airflow 3 and 2 it should target `main` branch.

## Developing for Airflow 3

PRs should target `main` branch.

## Developing for Airflow 2.10.x

PR should target `v-2-10-test` branch. When cutting a new release for 2.10 release manager
will sync `v-2-10-test`  branch to `v-2-10-stable` and cut the release from the stable branch.
PR should never target `v-2-10-stable` unless specifically instructed by release manager.

## Developing for Airflow 2.11

Version 2.11 is planned to be cut from `v-2-10-stable` branch.
The version will contain features relevant as bridge release for Airflow 3.
We will not backport otherwise features from main branch to 2.11
Note that 2.11 policy may change as 2.11 become closer

# Committers / PMCs

The following section explains the protocol for merging PRs.

## Merging PRs for providers and Helm chart

Make sure PR target `main` branch.
Avoid merging PRs that involves providers + core / helm chart + core
Core part should be extracted to separated PR.
Exclusions should be pre-approved specifically with a comment by release manager.
Do not treat PR approval (Green V) as exclusion approval.

## Merging PR for Airflow 3 and 2.10.x / 2.11.x

The committer who merge the PR is responsible for back-porting the PR to `v-2-10-test`.
This means start a new PR where the original commit from main is cherry-picked and conflicts resolved.
If cherry-pick is too complex then ask the PR author / start your own PR against `v-2-10-test` directly with the change.
Note: tracking that the PRs merged as expected is the responsibility of committer who merged the PR.

Committer may also request from PR author to raise 2 PRs one against `main` branch and one against `v-2-10-test` prior to accepting the code change.

## Merging PRs 2.10.x

Suggested change
Make sure PR target `v-2-10-test` branch and merge it when ready.
Make sure your PRs target the `v-2-10-test` branch, and it can be merged when ready.
All regular protocols of merging considerations are applied.

## Merging PRs for Airflow 3

Make sure PR target `main` branch.

### PRs that involve breaking changes

Make sure it has newsfragment, please allow time for community members to review.
Our goal is to avoid breaking changes if we can so we should allow time to review, don't rush to merge.

## Merging PRs for Airflow 2.11

TBD

# Milestones for PR

## Set 2.10.x milestone

TBD

## Set 2.11 milestone

TBD

## Set 3 milestone

TBD
