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
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Do we have a process defined here?](#do-we-have-a-process-defined-here)
- [How does the selection process for cherry-picking work?](#how-does-the-selection-process-for-cherry-picking-work)
- [What's the release manager's role ?](#whats-the-release-managers-role-)
- [Is this process following the ASF rules?](#is-this-process-following-the-asf-rules)
- [What's the role of individual maintainers?](#whats-the-role-of-individual-maintainers)
- [When proposed PRs are rejected?](#when-proposed-prs-are-rejected)
- [Why are provider changes not cherry-picked?](#why-are-provider-changes-not-cherry-picked)
- [What's the purpose of patch releases?](#whats-the-purpose-of-patch-releases)
- [Do we intentionally skip over some changes when releasing a patch release?](#do-we-intentionally-skip-over-some-changes-when-releasing-a-patch-release)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

This page describes the context and the process we follow when we create a patch release.

# Do we have a process defined here?

We have a well defined and well tested process that we follow.,
We follow it since the beginning of Airflow 2.0 actually - it's been also done the same in 1.10 -
but with some variations, we do it the same way since the beginning of 2.0,
but it has been refined and improved over time - by those who volunteered their time in the release
process (a lot of ad-hoc discussion have been traditionally happening in #release-management slack channel)
and we continue to do so.

The succinct form of it is described in a prominent place in our most important
[README](../README.md#what-goes-into-the-next-release).

# How does the selection process for cherry-picking work?

In short (and this is the most important thing that every maintainer should be aware of):
**maintainers who think that issue should be included should mark it with the next patch milestone**

It's up to individual maintainers who want to include certain changes to take care about it
and mark the issues they think are bug fixes, to go into the next release

This is the only thing that the maintainer has to do to get the PR proposed to be considered in
the next patch release. Sometimes - if controversial - maintainers discuss the proposals in
the #release-management channel in Slack, sometimes in #contributors or in the PR itself -
especially if the release manager decides to not include it and changes the milestone (and explains why).

# What's the release manager's role ?

Release manager's job is purely mechanical (as also mandated by the Apache Software Foundation release
manager role description) to assess cherry-pick ability of those changes. Release manager -
at the sole discretion and individual decision can reject some of those changes that other maintainers think
should be included. But the release manager on his own does not make proposals on what should be included.

This is the only place in the whole ASF process setup where a single person has such power to
make individual decisions and the main reason for it is to make the release process as smooth as possible.

# Is this process following the ASF rules?

We think so. The release manager's role is nicely described in
[Release manager chapter of release publishing ASF infra docs](https://infra.apache.org/release-publishing.html#releasemanager).
There is far more complete description here that describes the whole process
[Release management policy](https://www.apache.org/legal/release-policy.html#management) - also mentioning
that it's the PMC member's responsibility (and particularly PMC chair's) to adhere to the process.

# What's the role of individual maintainers?

The role of maintainers (collectively) to propose things for the next release.
In our case it happens with setting the milestone on a PR.

# When proposed PRs are rejected?

There are various reasons to reject those - if too complex to cherry-pick or when the release manager
assesses it's a new feature, not a bugfix. Essentially (according to [SemVer](https://semver.org/) when
it comes to the user-facing changes, the patch release should contain only bug-fixes. and may
contain docs changes if they are fixing/improving docs (not about new features) and also environment/build
script changes (so non-user-facing changes) as they are pretty much always needed to keep the things
nicely building - those are usually skipped from the changelog as non-user facing).

# Why are provider changes not cherry-picked?

In our case - basically none of the provider changes are cherry-picked - unless they are needed to
make the builds work well (sometimes happen). Providers are ALWAYS released from the latest `main` code
not from the `v2-*-stable` branch. In fact all the tests and ci checks for providers are skipped in the
non-main (v2* branches). So yes - not seeing provider changes cherry-picked is absolutely expected.

# What's the purpose of patch releases?

The purpose of that release is as described in SemVer - to give the users bug-fix-only release that has no
new features. Of course it's sometimes debatable whether changes are features/bug-fixes, but we usually use
the #release-management on Airflow's slack to quickly chat about it, and eventually the release manager
always makes a comment in the PR when the milestone is changed and explains the reasoning.

Sometimes we also include technically breaking changes in patch release (for example when we fix a security
issue it often is done in a "breaking" way).

We have to remember that SemVer is a statement of intention and not a technical definition of breaking vs.
not breaking. As [Hyrum's Law](https://www.hyrumslaw.com/) correctly states: "With a sufficient number
of users of an API, it does not matter what you promise in the contract: all observable behaviors of
your system will be depended on by somebody.". Our intention is to keep the patch releases following
SemVer intentions as much as possible, but we also have to be pragmatic and sometimes we have to break the
[Public Interface of Airflow](https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html)
to fix things. Those should be rare and deliberate decisions and we should always try to avoid them,
but sometimes they are needed to either protect our users or to make the code maintainable and we asses
the likelihood of breaking our user's workflow is low.

# Do we intentionally skip over some changes when releasing a patch release?

Skipping is not intentional because we never "skip" things when cherry-picking, It's **reverse** -
those maintainer who think that certain bug fixes (or internal changes or sometimes even feature changes
that we classify really as "bugfix" SHOULD intentionally mark those PRs they want with the next `patch`
milestone to be included.  So there is no skipping, if maintainer did not deliberately mark PR as
upcoming milestone, it will just not be included (not skipped). By default all the changes merged to main
are included in the next `minor` release. See [README](../README.md#what-goes-into-the-next-release) for
a bit more detailed description of transition period when we branch-off and start working on
new `minor` release.
