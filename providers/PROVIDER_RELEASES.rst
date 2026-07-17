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

Provider Releases and Versioning
=================================

.. contents:: Table of Contents
   :depth: 3
   :local:

Release process
---------------

The community providers are released regularly (usually every 2 weeks) in batches consisting of any providers
that need to be released because they changed since last release. The release manager decides which providers
to include and whether some or all providers should be released (see the next chapter about upgrading the
minimum version of Airflow for example the case where we release all active meaning non-suspended providers,
together in a single batch). Also Release Manager decides on the version bump of the provider (depending on
classification, whether there are breaking changes, new features or just bugs comparing to previous version).

Versioning scheme
-----------------

We are using the `SEMVER <https://semver.org/>`_ versioning scheme for the Provider distributions. This is in order
to give the users confidence about maintaining backwards compatibility in the new releases of those
packages.

Details about maintaining the SEMVER version are going to be discussed and implemented in
`the related issue <https://github.com/apache/airflow/issues/11425>`_

Provider distribution states
-----------------------------

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
* The ``suspended`` state is used when we have a good reason to suspend such provider, following the devlist
  discussion and vote or "lazy consensus". The process of suspension is described in
  `Suspending releases for providers <SUSPENDING_AND_REMOVING_PROVIDERS.rst#suspending-releases-for-providers>`_.
  The ``suspended`` providers are excluded from being released as part of the regular release cycle (including
  documentation building) but also they do not contribute dependencies to the CI image and their tests are
  not run in CI process. The ``suspended`` providers are not released as part of the regular release cycle.
* The ``removed`` state is a temporary state after the provider has been voted (or agreed in "lazy consensus")
  to be removed and it is only used for exactly one release cycle - in order to produce the final version of
  the package - identical to the previous version with the exception of the removal notice. The process
  of removal is described in `Removing community providers <SUSPENDING_AND_REMOVING_PROVIDERS.rst#removing-community-providers>`_.
  The difference between ``suspended`` and ``removed`` providers is that additional information is added to
  their documentation about the provider not being maintained any more by the community.

Upgrading minimum supported version of Airflow
-----------------------------------------------

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
