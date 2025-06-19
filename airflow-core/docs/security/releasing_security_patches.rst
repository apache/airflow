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

Releasing security patches
==========================

Apache Airflow® uses a consistent and predictable approach for releasing security patches - both for
the Apache Airflow package and Apache Airflow providers (security patches in providers are treated
separately from security patches in Airflow core package).

Releasing Airflow with security patches
---------------------------------------

Apache Airflow uses a strict `SemVer <https://semver.org>`_ versioning policy, which means that we strive for
any release of a given ``MAJOR`` Version (version "2" currently) to be backwards compatible. When we
release a ``MINOR`` version, the development continues in the ``main`` branch where we prepare the next
``MINOR`` version, but we release ``PATCHLEVEL`` releases with selected bugfixes (including security
bugfixes) cherry-picked to the latest released ``MINOR`` line of Apache Airflow. At the moment, when we
release a new ``MINOR`` version, we stop releasing ``PATCHLEVEL`` releases for the previous ``MINOR`` version.

For example, once we released ``2.6.0`` version on April 30, 2023 all the security patches will be cherry-picked and released in ``2.6.*`` versions until we release ``2.7.0`` version. There will be no
``2.5.*`` versions released after ``2.6.0`` has been released.

This means that in order to apply security fixes in Apache Airflow, you
MUST upgrade to the latest ``MINOR`` and ``PATCHLEVEL`` version of Airflow.

Releasing Airflow providers with security patches
-------------------------------------------------

Similarly to Airflow, providers uses a strict `SemVer <https://semver.org>`_ versioning policy, and the same
policies apply for providers as for Airflow itself. This means that you need to upgrade to the latest
``MINOR`` and ``PATCHLEVEL`` version of the provider to get the latest security fixes.
Airflow providers are released independently from Airflow itself and the information about vulnerabilities
is published separately. You can upgrade providers independently from Airflow itself, following the
instructions found in :ref:`installing-from-pypi-managing-providers-separately-from-airflow-core`.
