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

.. contents:: :local:

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

Thy providers - their latest version in "main" branch of airflow repository - are installed and tested together
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

Accepting new community providers
=================================

Accepting new community providers should be a deliberate process that requires ``[DISCUSSION]``
followed by ``[VOTE]`` thread at the airflow `devlist <https://airflow.apache.org/community/#mailing-list>`_.

In case the provider is integration with an open-source software rather than service we can relax the vote
procedure a bit. Particularly if the open-source software is an Apache Software Foundation,
Linux Software Foundation or similar organisation with well established governance processes that are not
strictly vendor-controlled, and when the software is well established an popular, it might be enough to
have a good and complete PR of the provider, ideally with a great test coverage, including integration tests,
and documentation. Then it should be enough to request the provider acceptance by a ``[LAZY CONSENSUS]`` mail
on the devlist and assuming such lazy consensus is not objected by anyone in the community, the provider
might be merged.

For service providers, the ``[DISCUSSION]`` thread is aimed to gather information about the reasons why
the one who proposes the new provider thinks it should be accepted by the community. Maintaining the provider
in the community is a burden. Contrary to many people's beliefs, code is often liability rather than asset,
and accepting the code to be managed by the community, especially when it involves significant effort on
maintenance is often undesired, especially that the community consists of volunteers. There must be a really
good reason why we would believe that the provider is better to be maintained by the community if there
are 3rd-party teams that can be paid to manage it on their own. We have to believe that the current
community interest is in managing the provider and that enough volunteers in the community will be
willing to maintain it in the future in order to accept the provider.

The ``[VOTE]`` thread is aimed to gather votes from the community on whether the provider should be accepted
or not and it follows the usual Apache Software Foundation voting rules concerning
`Votes on Code Modification <https://www.apache.org/foundation/voting.html#votes-on-code-modification>`_

The Ecosystem page and registries, and own resources of the 3rd-party teams are the best places to increase
visibility that such providers exist, so there is no "great" visibility achieved by getting the provider in
the community. Also it is often easier to advertise and promote usage of the provider by the service providers
themselves when they own, manage and release their provider, especially when they can synchronize releases
of their provider with new feature, the service might get added.

Minimum supported version of Airflow for Community managed providers
====================================================================

One of the important limitations of the Providers released by the community is that we introduce the limit
of a minimum supported version of Airflow. The minimum version of Airflow is the ``MINOR`` version (2.4, 2.5 etc.)
indicating that the providers might use features that appeared in this release. The default support timespan
for the minimum version of Airflow (there could be justified exceptions) is that we increase the minimum
Airflow version to the next MINOR release, when 12 months passed since the first release for the
MINOR version of Airflow.

For example this means that by default we upgrade the minimum version of Airflow supported by providers
to 2.5.0 in the first Provider's release after 19th of September 2023. The 19th of September 2022 is the date when the
first ``PATCHLEVEL`` of 2.4 (2.4.0) has been released.

When we increase the minimum Airflow version, this is not a reason to bump ``MAJOR`` version of the providers
(unless there are other breaking changes in the provider). The reason for that is that people who use
older version of Airflow will not be able to use that provider (so it is not a breaking change for them)
and for people who are using supported version of Airflow this is not a breaking change on its own - they
will be able to use the new version without breaking their workflows. When we upgraded min-version to
2.2+, our approach was different but as of 2.3+ upgrade (November 2022) we only bump ``MINOR`` version of the
provider when we increase minimum Airflow version.

Increasing the minimum version ot the Providers is one of the reasons why 3rd-party provider maintainers
might want to maintain their own providers - as they can decide to support older versions of Airflow.

3rd-party providers
===================

Providers, can (and it is recommended for 3rd-party providers) also be maintained and releases by 3rd parties.

There is no difference between the community and 3rd party providers - they have all the same capabilities
and limitations. The consensus in the Airflow community is that usually it is better for the community and
for the health of the provider to be managed by the 3rd party team, rather than by the Airflow community.
This is especially in case the provider concerns 3rd-party service that has a team that can manage provider
on their own. For the Airflow community, managing and releasing a 3rd-party provider that we cannot test
and verify is a lot of effort and uncertainty, especially including the cases where the external service is
live and going to evolve in the future, and it is better to let the 3rd party team manage it,
as they can better keep pace with the changes in the service.

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

Mixed governance model
======================

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
=================================

In case a provider is found to require old dependencies that are not compatible with upcoming versions of
the Apache Airflow or with newer dependencies required by other providers, the provider's release
process can be suspended.

This means:

* The provider's status is set to "suspended"
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
