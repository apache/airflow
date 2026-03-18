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
--------------------------

Airflow providers are released independently from Airflow itself and the information about vulnerabilities
is published separately. You can upgrade providers independently from Airflow itself, following the
instructions found in :doc:`apache-airflow:installation/installing-from-pypi`.

When we release Provider version, the development is always done from the ``main`` branch where we prepare
the next version. The provider uses strict `SemVer <https://semver.org>`_ versioning policy. Depending on
the scope of the change, Provider will get ''MAJOR'' version upgrade when there are
breaking changes, ``MINOR`` version upgrade when there are new features or ``PATCHLEVEL`` version upgrade
when there are only bug fixes (including security bugfixes) - and this is the only version that receives
security fixes by default, so you should upgrade to latest version of the provider if you want to receive
all released security fixes.

The only exception to that rule is when we have a critical security fix and good reason to provide an
out-of-band release for the provider, in which case stakeholders in the provider might decide to cherry-pick
and prepare a branch for an older version of the provider following the
`mixed governance model <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#mixed-governance-model>`_
and requires interested parties to cherry-pick and test the fixes.
