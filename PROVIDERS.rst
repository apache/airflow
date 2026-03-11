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

.. contents:: Table of Contents
   :depth: 2
   :local:

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

List of all available community providers is available at the `Providers index <https://airflow.apache.org/docs/>`_.


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
See `Suspending releases for providers`_ below for more details.


Detailed documents
==================

The following documents provide detailed information about specific aspects of provider management:

.. list-table::
   :widths: 30 70

   * - `Provider Governance <providers/PROVIDER_GOVERNANCE.rst>`_
     - Governance framework, stewardship model, lifecycle stages (incubation, production,
       attic/deprecation), health metrics, and periodic reviews
   * - `3rd-Party Providers <providers/THIRD_PARTY_PROVIDERS.rst>`_
     - Relation to community providers, system test dashboards, and mixed governance model
   * - `Accepting New Providers <providers/ACCEPTING_PROVIDERS.rst>`_
     - Prerequisites, approval process, and historical examples for proposing new community providers
   * - `Provider Releases and Versioning <providers/PROVIDER_RELEASES.rst>`_
     - Release process, SEMVER versioning, provider distribution states, and minimum Airflow
       version policy
   * - `Suspending and Removing Providers <providers/SUSPENDING_AND_REMOVING_PROVIDERS.rst>`_
     - Criteria and process for suspending or removing community providers
   * - `Managing Provider's Lifecycle <providers/MANAGING_PROVIDERS_LIFECYCLE.rst>`_
     - Technical how-to for creating, suspending, resuming, and removing providers


.. _provider-governance-framework:

Community providers lifecycle
=============================

This document describes the complete life-cycle of community providers - from inception and approval to
Airflow main branch to being decommissioned and removed from the main branch in Airflow repository.

For full details, see `Provider Governance <providers/PROVIDER_GOVERNANCE.rst>`_.

Governance framework
--------------------

See `Provider Governance: Governance framework <providers/PROVIDER_GOVERNANCE.rst#governance-framework>`_.


Accepting new community providers
----------------------------------

See `Accepting New Community Providers <providers/ACCEPTING_PROVIDERS.rst>`_.


Releases and versioning
=======================

See `Provider Releases and Versioning <providers/PROVIDER_RELEASES.rst>`_.

Release process
---------------

See `Provider Releases: Release process <providers/PROVIDER_RELEASES.rst#release-process>`_.

Versioning scheme
-----------------

See `Provider Releases: Versioning scheme <providers/PROVIDER_RELEASES.rst#versioning-scheme>`_.

Provider distribution states
-----------------------------

See `Provider Releases: Provider distribution states <providers/PROVIDER_RELEASES.rst#provider-distribution-states>`_.

Upgrading minimum supported version of Airflow
-----------------------------------------------

See `Provider Releases: Upgrading minimum supported version of Airflow <providers/PROVIDER_RELEASES.rst#upgrading-minimum-supported-version-of-airflow>`_.


Suspending releases for providers
----------------------------------

See `Suspending and Removing Providers: Suspending releases <providers/SUSPENDING_AND_REMOVING_PROVIDERS.rst#suspending-releases-for-providers>`_.


Removing community providers
------------------------------

See `Suspending and Removing Providers: Removing providers <providers/SUSPENDING_AND_REMOVING_PROVIDERS.rst#removing-community-providers>`_.

3rd-party providers
===================

See `3rd-Party Providers <providers/THIRD_PARTY_PROVIDERS.rst>`_.
