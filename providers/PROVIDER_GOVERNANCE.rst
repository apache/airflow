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

Provider Governance
====================

.. contents:: Table of Contents
   :depth: 3
   :local:

This document describes the governance framework for community providers, including lifecycle stages,
stewardship model, and quantitative health metrics.

For an overview of what providers are and how they fit into the Airflow ecosystem, see
`Apache Airflow Providers <../PROVIDERS.rst>`_.

Governance framework
--------------------

Airflow's success is built on its extensive ecosystem of community-supported integrations---over 1,600
hooks, operators, and sensors released as part of 90+ provider packages. These integrations are critical
for "ubiquitous orchestration." This governance framework establishes a scalable method to grow the
number of integrations while ensuring quality. Actual code acceptance and release governance remains with the Airflow PMC.

Stewardship model
^^^^^^^^^^^^^^^^^

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

Lifecycle stages
^^^^^^^^^^^^^^^^

Providers generally follow a three-stage lifecycle: **Incubation**, **Production**, and **Attic/Deprecation**. However,
not all providers will move through all stages. Additionally, providers may be designated as **Mature** under specific circumstances.

Incubation stage
""""""""""""""""

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

Production stage
""""""""""""""""

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

Attic / Deprecation stage
"""""""""""""""""""""""""

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
  appropriate communication (see `Removing community providers <SUSPENDING_AND_REMOVING_PROVIDERS.rst#removing-community-providers>`_)
* It is possible for the provider to be resurrected from the attic as long as there is confidence that there is a
  clear need for the provider and the (possibly new) stewards are able to maintain it actively on this go around.
  It should be noted that significant effort may be required to resurrect a provider from the attic.

Mature providers
""""""""""""""""

Some providers may have very stable interfaces requiring minimal changes on a regular basis (e.g.,
HTTP provider integration). At the discretion of the Airflow PMC, certain providers can be tagged
as **"mature providers"**, which will not automatically be deprecated and moved into the attic due
to lack of activity alone.

Suspending providers
^^^^^^^^^^^^^^^^^^^^

When a provider's dependencies prevent the broader Airflow project from upgrading shared
dependencies or otherwise block CI builds and releases, the provider may be **suspended**. Suspension
removes the provider from CI builds and releases until the underlying dependency issues are resolved.

Suspension may also be applied selectively for **individual Python versions** when a dependency
problem only affects a specific Python version rather than all supported versions.

It is the responsibility of the provider's **stewards** to:

* Work with upstream maintainers to resolve the dependency issues
* Ensure that unsuspending the provider results in a green CI across all affected Python versions
* Communicate progress and resolution on the dev list

Suspension is not a lifecycle stage---it is a temporary operational measure that can apply to providers
at any lifecycle stage (incubation, production, or mature). Once the dependency issues are resolved and
CI is green, the provider is unsuspended and returns to normal operations.

Periodic reviews
^^^^^^^^^^^^^^^^

The Airflow PMC is responsible for reviewing the health status of integrations on a **quarterly
basis** and making decisions such as:

* Graduating providers from incubation to production
* Moving providers from production to the attic
* Granting exceptions for specific providers

These discussions will be held in public and subsequently summarized and shared on the Airflow devlist.
