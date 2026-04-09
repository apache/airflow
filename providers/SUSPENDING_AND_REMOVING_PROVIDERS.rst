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

Suspending and Removing Providers
==================================

.. contents:: Table of Contents
   :depth: 3
   :local:

For technical details on how to perform these operations, see
`Managing provider's lifecycle <MANAGING_PROVIDERS_LIFECYCLE.rst>`_.

Suspending releases for providers
----------------------------------

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
  mixed governance model described in `3rd-party providers <THIRD_PARTY_PROVIDERS.rst#mixed-governance-model>`_.

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
------------------------------

After a Provider has been deprecated, as described in the
`Provider Governance <PROVIDER_GOVERNANCE.rst#attic-deprecation-stage>`_ document with a ``[VOTE]`` thread, it can
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

Consequences of removal:

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
  of accepting new provider described in `Accepting new community providers <ACCEPTING_PROVIDERS.rst>`_.
