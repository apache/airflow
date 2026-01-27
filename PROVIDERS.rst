 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

************************
Apache Airflow Providers
************************

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

What is a provider?
===================

Airflow 2.0 introduced the concept of providers. Providers are packages that contain integrations with
external systems. They are meant to extend capabilities of the core "Apache Airflow". Thus they are
part of the vision of Airflow-as-a-Platform - where the Airflow Core provides basic data-workflow scheduling
and management capabilities and can be extended by implementing Open APIs Airflow supports, adding
Plugins that can add new features to the Core, and adding Providers that allow to interact with external
systems.

The providers are released separately from the core Airflow and they are versioned independently. The
ways how providers can extend the Airflow Core, including the types of providers, can be found at the
`Providers page <https://airflow.apache.org/docs/apache-airflow-providers/index.html>`_. You can also find
out there, how you can create your own provider.

Providers can be maintained and released by the Airflow community or by 3rd-party teams. In any case -
whether community-managed, or 3rd-party managed - they are released independently of the Airflow Core package.

When community releases the Airflow Core, it is released together with constraints, those constraints use
the latest released version of providers, and our published convenience images contain a subset of most
popular community providers. However our users are free to upgrade and downgrade providers independently of
the Airflow Core version as they see fit, as long as it does not cause conflicting dependencies.

You can read more about it in the
`Installation and upgrade scenarios <https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#installation-and-upgrade-scenarios>`_
chapter of our user documentation.

Community managed providers
===========================

When providers are accepted by the community, the process of managing and releasing them must follow the
Apache Software Foundation rules and policies. This is especially, about accepting contributions and
releasing new versions of the providers. This means that the code changes in the providers must be
reviewed by Airflow committers and merged when they are accepted by them. Also we must have sufficient
test coverage and documentation that allow us to maintain the providers, and our users to use them.

The providers - their latest version in "main" branch of Airflow repository - are installed and tested together
with other community providers and one of the key properties of the community providers is that the latest
version of providers contribute their dependencies to constraints of Airflow, published when Airflow Core is
released. This means that when users are using constraints published by Airflow, they can install all
the providers together and they are more likely to not interfere with each other, especially they should
be able to be installed together, without conflicting dependencies. This allows to add an optional
"extra" to Airflow for each provider, so that the providers can be installed together with Airflow by
specifying the "extra" in the installation command.

Because of the constraint and potential conflicting dependencies, the community providers have to be regularly
updated and the community might decide to suspend releases of a provider if we find out that we have trouble
with updating the dependencies, or if we find out that the provider is not compatible with other more
popular providers and when the popular providers are limited by the constraints of the less popular ones.
See the section below for more details on suspending releases of the community providers.

List of all available community providers is available at the `Providers index <https://airflow.apache.org/docs/>`_.


Community providers lifecycle
=============================

This document describes the complete life-cycle of community providers - from inception and approval to
Airflow main branch to being decommissioned and removed from the main branch in Airflow repository.

.. note::

   Technical details on how to manage lifecycle of providers are described in the document:

   `Managing provider's lifecycle <https://github.com/apache/airflow/blob/main/providers/MANAGING_PROVIDERS_LIFECYCLE.rst>`_


Provider Governance Framework
-----------------------------

This section describes the governance framework for community providers, including lifecycle stages,
stewardship model, and quantitative health metrics.

Airflow's success is built on its extensive ecosystem of community-supported integrations—over 1,600
hooks, operators, and sensors released as part of 90+ provider packages. These integrations are critical
for "ubiquitous orchestration." This governance framework establishes a scalable method to grow the
number of integrations while ensuring quality. Actual code acceptance and release governance remains with the Airflow PMC.

Provider Stewardship Model
^^^^^^^^^^^^^^^^^^^^^^^^^^

Each provider or integration component must have designated **stewards** responsible for ensuring
the health criteria described below are met:

* **Minimum stewards**: At least two unique individuals must serve as stewards for each provider
* **Role definition**: Stewards are subject matter experts for the integration. This could be expertise in the
  service being integrated, the language being supported by the provider, or the framework being integrated.
  Stewardship is a responsibility, not an additional authority or privilege
* **Committer sponsorship**: Each steward must be sponsored by at least one Airflow Committer. The
  sponsor ensures that stewards fulfill their responsibilities and provides guidance on maintaining the
  provider according to best practices. This includes regular dependency updates, issue resolution, and
  monitoring that the provider meets the health metrics required for its current lifecycle stage (that is, incubation
  or production). The sponsor is responsible for PR reviews and merges (including ensuring coding standards are met), but
  is NOT responsible for active maintenance of the provider's codebase, which remains the responsibility of the stewards.
  While sponsors should be accountable when it comes to reviews and merges, it's also OK and welcome that other committers merge code providing it fulfill the criteria.
* **Accountability**: Ultimate accountability remains with the Airflow PMC and Committers
* **Transitions**: Neither sponsorship nor stewardship are roles in perpetuity; they can be
  transitioned to others based on mutual agreement and PMC approval

.. note::

   The quantitative criteria described below are aspirational. The PMC will revisit these metrics
   based on actual experience 6 months from the date of the first quarterly review of the provider metrics being published.

Provider Lifecycle Stages
^^^^^^^^^^^^^^^^^^^^^^^^^

Providers generally follow a three-stage lifecycle: **Incubation**, **Production**, and **Attic/Deprecation**. However,
not all providers will move through all stages. Additionally, providers may be designated as **Mature** under specific circumstances.

**Incubation Stage**

All new providers or integration modules (such as Notifiers, Message components, etc.) should start
in incubation unless specifically accelerated by the PMC. Incubation ensures that code, licenses, and
processes align with ASF guidelines and community principles such as community, collaborative
development, and openness.

Requirements for incubation:

* Working codebase to bootstrap credibility and attract contributions
* Visibility in the provider health dashboard (The provider health dashboard is to be added)
* Designated stewards with committer sponsorship

Quantitative graduation criteria:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Metric
     - Threshold
   * - PRs submitted
     - Minimum of 10 PRs in the last 6 months
   * - PR review time
     - PRs reviewed within 14 days
   * - Issues reported
     - Minimum of 15 unique issues filed in the last 6 months
   * - Contributors
     - At least 3 unique individuals making contributions (code or documentation) in the last 6 months
   * - Issue resolution rate
     - At least 50% of reported issues closed within 90 days
   * - Security/release issues
     - All release and security related issues closed within 60 days
   * - Governance participation
     - Demonstrated participation in project governance channels including quarterly updates
   * - Quality standards
     - Meet quality criteria for code, documentation, and tests as listed in the Contributing Guide

**Production Stage**

All modules in production are expected to be well managed with prompt resolution of issues and
support for a consistent release cadence (at least monthly, but typically every 2 weeks when changes
exist). Production providers must:

* Stay consistent with the main branch
* Pass tests for main + all supported Airflow versions
* Follow Airflow support guidelines, staying current with main Airflow releases

Exceptions can be granted based on a PMC/devlist vote (PMC members only having binding votes) for
valid and one-off criteria.

Quantitative criteria to maintain production status:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Metric
     - Threshold
   * - PRs submitted
     - Minimum of 10 PRs in the last 6 months
   * - PR review time
     - PRs reviewed within 14 days
   * - Issues reported
     - Minimum of 20 unique issues filed in the last 6 months
   * - Contributors
     - At least 5 unique individuals making contributions (code or documentation) in the last 6 months
   * - Issue resolution rate
     - At least 60% of reported issues closed within 90 days
   * - Security/release issues
     - All release and security related issues closed within 30 days
   * - Feature releases
     - At least 1 feature release every 6 months
   * - User engagement
     - Maintain support activity with response to questions within 2 weeks on average
   * - Governance participation
     - Demonstrated participation in project governance channels including quarterly updates

**Attic / Deprecation Stage**

Modules should be moved into the Attic when relevance wanes, typically measured by declining
activity. This commonly occurs when the integrated solution has faded in popularity and is replaced
by more modern alternatives.

Movement to the Attic must be communicated on the dev list and voted on by the PMC. Exceptions can
be granted based on the vote.

Quantitative criteria triggering attic consideration:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Metric
     - Threshold
   * - PRs submitted
     - Fewer than 5 PRs in the last 6 months
   * - PR review time
     - PRs not being reviewed in more than a month
   * - Issues reported
     - Fewer than 10 unique issues filed in the last 6 months
   * - Contributors
     - Fewer than 3 unique individuals making contributions (code or documentation) in the last 6 months
   * - Issue resolution rate
     - Less than 30% of reported issues closed within 90 days
   * - Security/release issues
     - Release and security related issues not getting closed within 30 days
   * - Feature releases
     - No feature releases in the last 6 months

Consequences of attic status:

* Modules remain readable but do not receive active maintenance
* Module is not actively tested in "main" in Airflow CI, its dependencies are not checked for conflicts
  with other main providers, and common refactorings are not applied to it.
* After a period of at least 6 months in the attic, modules can be chosen for removal with
  appropriate communication (see `Removing community providers`_ below)
* It is possible for the provider to be resurrected from the attic as long as there is confidence that there is a
  clear need for the provider and the (possibly new) stewards are able to maintain it actively on this go around.
  It should be noted that significant effort may be required to resurrect a provider from the attic.

**Mature Providers**

Some providers may have very stable interfaces requiring minimal changes on a regular basis (e.g.,
HTTP provider integration). At the discretion of the Airflow PMC, certain providers can be tagged
as **"mature providers"**, which will not automatically be deprecated and moved into the attic due
to lack of activity alone.

Periodic Reviews
^^^^^^^^^^^^^^^^

The Airflow PMC is responsible for reviewing the health status of integrations on a **quarterly
basis** and making decisions such as:

* Graduating providers from incubation to production
* Moving providers from production to the attic
* Granting exceptions for specific providers

These discussions will be held in public and subsequently summarized and shared on the Airflow devlist.


Accepting new community providers
---------------------------------

The Airflow community welcomes new provider contributions. All new providers enter through the
**Incubation** stage (unless specifically accelerated by the PMC) to ensure alignment with ASF
guidelines and community principles.

**Prerequisites for proposing a new provider:**

1. **Working codebase**: A functional implementation demonstrating the integration
2. **Stewardship commitment**: At least two individuals willing to serve as stewards
3. **Committer sponsorship**: At least one existing Airflow Committer willing to sponsor the stewards
4. **Quality standards**: Code, tests, and documentation meeting the Contributing Guide standards

**Approval process:**

Accepting new community providers requires a ``[DISCUSSION]`` followed by ``[VOTE]`` thread at the
Airflow `devlist <https://airflow.apache.org/community/#mailing-list>`_.

For integrations with well-established open-source software (Apache Software Foundation, Linux
Foundation, or similar organizations with established governance), a ``[LAZY CONSENSUS]`` process
may be sufficient, provided the PR includes comprehensive test coverage and documentation.

The ``[DISCUSSION]`` thread should include:

* Description of the integration and its value to the Airflow ecosystem
* Identification of the proposed stewards and their sponsoring committer(s)
* Commitment to meet incubation health metrics within 6 months
* Plan for participating in quarterly governance updates

The ``[VOTE]`` follows the usual Apache Software Foundation voting rules concerning
`Votes on Code Modification <https://www.apache.org/foundation/voting.html#votes-on-code-modification>`_

**Alternative: 3rd-party managed providers**

For service providers or systems integrators with dedicated teams to manage their provider and who wish to not participate
in the Airflow community, we encourage considering 3rd-party management of providers. The
`Ecosystem page <https://airflow.apache.org/ecosystem/#third-party-airflow-plugins-and-providers>`_
provides visibility for 3rd-party providers, and this approach allows service providers or systems integrators to:

* Synchronize releases with their service updates
* Maintain direct control over the integration
* Support older Airflow versions if needed

There is no difference in technical capabilities between community and 3rd-party providers.

**Historical examples:**

* Huawei Cloud provider - `Discussion <https://lists.apache.org/thread/f5tk9c734wlyv616vyy8r34ymth3dqbc>`_
* Cloudera provider - `Discussion <https://lists.apache.org/thread/2z0lvgj466ksxxrbvofx41qvn03jrwwb>`_, `Vote <https://lists.apache.org/thread/8b1jvld3npgzz2z0o3gv14lvtornbdrm>`_


Community providers release process
-----------------------------------

The community providers are released regularly (usually every 2 weeks) in batches consisting of any providers
that need to be released because they changed since last release. The release manager decides which providers
to include and whether some or all providers should be released (see the next chapter about upgrading the
minimum version of Airflow for example the case where we release all active meaning non-suspended providers,
together in a single batch). Also Release Manager decides on the version bump of the provider (depending on
classification, whether there are breaking changes, new features or just bugs comparing to previous version).

Upgrading Minimum supported version of Airflow
----------------------------------------------

.. note::

   The following policy applies for Airflow 2. It has not yet been finalized for Airflow 3 and is subject to changes.

One of the important limitations of the Providers released by the community is that we introduce the limit
of a minimum supported version of Airflow. The minimum version of Airflow is the ``MINOR`` version (2.4, 2.5 etc.)
indicating that the providers might use features that appeared in this release. The default support timespan
for the minimum version of Airflow (there could be justified exceptions) is that we increase the minimum
Airflow version to the next MINOR release, when 12 months passed since the first release for the
MINOR version of Airflow.

For example this means that by default we upgrade the minimum version of Airflow supported by providers
to 3.1.0 in the first Provider's release after 20th of May 2026. 20th of May 2025 was the date when the
first ``PATCHLEVEL``  version of 2.11 (2.11.0) was released and since Airflow 3.0 was released in April 2025,
we go straight to Airflow 3.1 as minimum supported version of Airflow for providers in May 2026.

When we increase the minimum Airflow version, this is not a reason to bump ``MAJOR`` version of the providers
(unless there are other breaking changes in the provider). The reason for that is that people who use
older version of Airflow will not be able to use that provider (so it is not a breaking change for them)
and for people who are using supported version of Airflow this is not a breaking change on its own - they
will be able to use the new version without breaking their workflows. When we upgraded min-version to
2.2+, our approach was different but as of 2.3+ upgrade (November 2022) we only bump ``MINOR`` version of the
provider when we increase minimum Airflow version.

Increasing the minimum version of the Providers is one of the reasons why 3rd-party provider maintainers
might want to maintain their own providers - as they can decide to support older versions of Airflow.

3rd-parties relation to community providers
-------------------------------------------

Providers, can also be maintained and released by 3rd parties (service providers or systems integrators).
There is no difference between the community and 3rd party providers - they have all the same capabilities
and limitations.

This is especially in case the provider concerns 3rd-party service that has a team that can manage provider
on their own, has a rapidly evolving live service, and believe they need a faster release cycle than the community
can provide.

Information about such 3rd-party providers are usually published at the
`Ecosystem: plugins and providers <https://airflow.apache.org/ecosystem/#third-party-airflow-plugins-and-providers>`_
page of the Airflow website and we encourage the service providers to publish their providers there. You can also
find a 3rd-party registries of such providers, that you can use if you search for existing providers (they
are also listed at the "Ecosystem" page in the same chapter)

While we already have - historically - a number of 3rd-party service providers managed by the community,
most of those services have dedicated teams that keep an eye on the community providers and not only take
active part in managing them (see mixed-governance model below), but also provide a way that we can
verify whether the provider works with the latest version of the service via dashboards that show
status of System Tests for the provider. This allows us to have a high level of confidence that when we
release the provider it works with the latest version of the service. System Tests are part of the Airflow
code, but they are executed and verified by those 3rd party service teams. We are working with the 3rd
party service teams (who are often important stakeholders of the Apache Airflow project) to add dashboards
for the historical providers that are managed by the community, and current set of Dashboards can be also
found at the
`Ecosystem: system test dashboards <https://airflow.apache.org/ecosystem/#airflow-provider-system-test-dashboards>`_

Mixed governance model for 3rd-party related community providers
----------------------------------------------------------------

Providers are often connected with some stakeholders that are vitally interested in maintaining backwards
compatibilities in their integrations (for example cloud providers, or specific service providers). But,
we are also bound with the `Apache Software Foundation release policy <https://www.apache.org/legal/release-policy.html>`_
which describes who releases, and how to release the ASF software. The provider's governance model is something we name
``mixed governance`` - where we follow the release policies, while the burden of maintaining and testing
the cherry-picked versions is on those who commit to perform the cherry-picks and make PRs to older
branches.

The "mixed governance" (optional, per-provider) means that:

* The Airflow Community and release manager decide when to release those providers.
  This is fully managed by the community and the usual release-management process following the
  `Apache Software Foundation release policy <https://www.apache.org/legal/release-policy.html>`_
* The contributors (who might or might not be direct stakeholders in the provider) will carry the burden
  of cherry-picking and testing the older versions of providers.
* There is no "selection" and acceptance process to determine which version of the provider is released.
  It is determined by the actions of contributors raising the PR with cherry-picked changes and it follows
  the usual PR review process where maintainer approves (or not) and merges (or not) such PR. Simply
  speaking - the completed action of cherry-picking and testing the older version of the provider make
  it eligible to be released. Unless there is someone who volunteers and perform the cherry-picking and
  testing, the provider is not released.
* Branches to raise PR against are created when a contributor commits to perform the cherry-picking
  (as a comment in PR to cherry-pick for example)

Usually, community effort is focused on the most recent version of each provider. The community approach is
that we should rather aggressively remove deprecations in "major" versions of the providers - whenever
there is an opportunity to increase major version of a provider, we attempt to remove all deprecations.
However, sometimes there is a contributor (who might or might not represent stakeholder),
willing to make their effort on cherry-picking and testing the non-breaking changes to a selected,
previous major branch of the provider. This results in releasing at most two versions of a
provider at a time:

* potentially breaking "latest" major version
* selected past major version with non-breaking changes applied by the contributor

Cherry-picking such changes follows the same process for releasing Airflow
patch-level releases for a previous minor Airflow version. Usually such cherry-picking is done when
there is an important bugfix and the latest version contains breaking changes that are not
coupled with the bugfix. Releasing them together in the latest version of the provider effectively couples
them, and therefore they're released separately. The cherry-picked changes have to be merged by the committer following the usual rules of the
community.

There is no obligation to cherry-pick and release older versions of the providers.
The community continues to release such older versions of the providers for as long as there is an effort
of the contributors to perform the cherry-picks and carry-on testing of the older provider version.

The availability of stakeholder that can manage "service-oriented" maintenance and agrees to such a
responsibility, will also drive our willingness to accept future, new providers to become community managed.

Suspending releases for providers
---------------------------------

In case a provider is found to require old dependencies that are not compatible with upcoming versions of
the Apache Airflow or with newer dependencies required by other providers, the provider's release
process can be suspended.

This means:

* The provider's state in ``provider.yaml`` is set to "suspended"
* No new releases of the provider will be made until the problem with dependencies is solved
* Sources of the provider remain in the repository for now (in the future we might add process to remove them)
* No new changes will be accepted for the provider (other than the ones that fix the dependencies)
* The provider will be removed from the list of Apache Airflow extras in the next Airflow release
  (including patch-level release if it is possible/easy to cherry-pick the suspension change)
* Tests of the provider will not be run on our CI (in main branch)
* Dependencies of the provider will not be installed in our main branch CI image nor included in constraints
* We can still decide to apply security fixes to released providers - by adding fixes to the main branch
  but cherry-picking, testing and releasing them in the patch-level branch of the provider similar to the
  mixed governance model described above.

The suspension may be triggered by any committer after the following criteria are met:

* The maintainers of dependencies of the provider are notified about the issue and are given a reasonable
  time to resolve it (at least 1 week)
* Other options to resolve the issue have been exhausted and there are good reasons for upgrading
  the old dependencies in question
* Explanation why we need to suspend the provider is stated in a public discussion in the devlist. Followed
  by ``[LAZY CONSENSUS]`` or ``[VOTE]`` discussion at the devlist (with the majority of the binding votes
  agreeing that we should suspend the provider)

The suspension will be lifted when the dependencies of the provider are made compatible with the Apache
Airflow and with other providers - by merging a PR that removes the suspension and succeeds.

Removing community providers
----------------------------

After a Provider has been deprecated, as described above with a ``[VOTE]`` thread, it can
be removed from main branch of Airflow when the community agrees that there should be no
more updates to the providers done by the community - except maybe potentially security fixes found. There
might be various reasons for the providers to be removed:

* the service they connect to is no longer available
* the dependencies for the provider are not maintained anymore and there is no viable alternative
* there is another, more popular provider that supersedes community provider
* etc. etc.

Generally speaking a discussion thread ``[DISCUSS]`` is advised before such removal and
sufficient time should pass (at least a week) to give a chance for community members to express their
opinion on the removal.

There are the following consequences (or lack of them) of removing the provider:

* One last release of the provider is done with documentation updated informing that the provider is no
  longer maintained by the Apache Airflow community - linking to this page. This information should also
  find its way to the package documentation and consequently - to the description of the package in PyPI.
* An ``[ANNOUNCE]`` thread is sent to the devlist and user list announcing removal of the provider
* The released providers remain available on PyPI and in the
   `Archives <https://archive.apache.org/dist/airflow/providers/>`_ of the Apache
   Software Foundation, while they are removed from the
   `Downloads <https://downloads.apache.org/airflow/providers/>`_ .
   Also it remains in the Index of the Apache Airflow Providers documentation at
   `Airflow Documentation <https://airflow.apache.org/docs/>`_ with note ``(not maintained)`` next to it.
* The code of the provider is removed from ``main`` branch of the Apache Airflow repository - including
  the tests and documentation. It is no longer built in CI and dependencies of the provider no longer
  contribute to the CI image/constraints of Apache Airflow for development and future ``MINOR`` release.
* The provider is removed from the list of Apache Airflow extras in the next ``MINOR`` Airflow release
* The dependencies of the provider are removed from the constraints of the Apache Airflow
  (and the constraints are updated in the next ``MINOR`` release of Airflow)
* In case of confirmed security issues that need fixing that are reported to the provider after it has been
  removed, there are two options:
  * in case there is a viable alternative or in case the provider is anyhow not useful to be installed, we
    might issue advisory to the users to remove the provider (and use alternatives if applicable)
  * in case the users might still need the provider, we still might decide to release new version of the
    provider with security issue fixed, starting from the source code in Git history where the provider was
    last released. This however, should only be done in case there are no viable alternatives for the users.
* Removed provider might be re-instated as maintained provider, but it needs to go through the regular process
  of accepting new provider described above.

Provider distributions versioning
---------------------------------

We are using the `SEMVER <https://semver.org/>`_ versioning scheme for the Provider distributions. This is in order
to give the users confidence about maintaining backwards compatibility in the new releases of those
packages.

Details about maintaining the SEMVER version are going to be discussed and implemented in
`the related issue <https://github.com/apache/airflow/issues/11425>`_

Possible states of Provider distributions
-----------------------------------------

The Provider distributions can be in one of several states.

* The ``not-ready`` state is used when the provider has some in-progress changes (usually API changes) that
  we do not  want to release yet as part of the regular release cycle. Providers in this state are excluded
  from being  released as part of the regular release cycle (including documentation building).
  The ``not-ready`` providers are treated as regular providers when it comes to running tests and preparing
  and releasing packages in ``CI`` - as we want to make sure they are properly releasable any time and we
  want them to contribute to dependencies and we want to test them. Also in case of preinstalled providers,
  the ``not-ready`` providers are contributing their dependencies rather than the provider package to
  requirements of Airflow.
* The ``ready`` state is the usual state of the provider that is released in the regular release cycle
  (including the documentation, package building and publishing). This is the state most providers are in.
* The ``suspended``` state is used when we have a good reason to suspend such provider, following the devlist
  discussion and vote or "lazy consensus". The process of suspension is described above.
  The ``suspended`` providers are excluded from being released as part of the regular release cycle (including
  documentation building) but also they do not contribute dependencies to the CI image and their tests are
  not run in CI process. The ``suspended`` providers are not released as part of the regular release cycle.
* The ``removed`` state is a temporary state after the provider has been voted (or agreed in "lazy consensus")
  to be removed and it is only used for exactly one release cycle - in order to produce the final version of
  the package - identical to the previous version with the exception of the removal notice. The process
  of removal is described in [Provider's docs](../PROVIDERS.rst).  The difference between ``suspended``
  and ``removed`` providers is that additional information is added to their documentation about the provider
  not being maintained any more by the community.
