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

Vulnerabilities in 3rd party dependencies
=========================================

How users should treat 3rd-party dependencies with known CVEs
-------------------------------------------------------------

Apache Airflow has a rather large number of dependencies, and we invest a lot of effort to keep Airflow updated
to latest versions of those dependencies. We have automation, that checks for new versions of dependencies,
and attempts to upgrade and test them automatically, we also have security scans that indicate if we have
minimum versions of dependencies, that are not vulnerable to known, important, exploitable CVEs. Every
version of Airflow has a set of constraints - i.e. latest tested versions of dependencies, that
are passing our tests and that we know allow to install Airflow and it's providers together.

However, sometimes due to complex dependency trees or conflicting requirements, we are not
always able to upgrade and test dependencies to the latest versions. Sometimes we can only upgrade to newer
versions of dependencies in the "development" branch - i.e. for the next "MINOR" version of Airflow,
and we are not able to backport those upgrades to the latest released "MINOR" version of Airflow.

This means that sometimes we have to keep older versions of dependencies in the latest released "MINOR"
version of Airflow, even if those versions are vulnerable to some CVEs. Since Airflow is a volunteer-driven
project, we do not provide any guarantees that we will upgrade to dependencies that are CVE-free.

Contrary to a common belief, the presence of a CVE in a 3rd-party dependency does not automatically mean
that Airflow or your deployment - and we do not accept reports and issues stating that we **should upgrade**
those because they have a known CVE. We need to have a proof that the CVE is exploitable in Airflow
and that it can be used to attack Airflow or your deployment and such proofs should be responsibly
disclosed to us, in private, following our `Security policy <https://github.com/apache/airflow/security/policy>`_.
If you have such proof, please share it with us and we will do our best to upgrade the dependency
to a non-vulnerable version as soon as possible and back-port it to latest released minor version,
following our :doc:`releasing_security_patches` policy. We expect our commercial users who need to fulfill
their obligation to manage and disclose CVEs in their environment to invest time and effort into
responsible disclosure, and investment of their security teams if they think Airflow is vulnerable to
third-party CVEs.

This general approach is described in the `Apache Software Foundation's approach <https://security.apache.org/report-dependency/>`_.
This document provides more detailed instructions that are specific to Apache Airflow.

Constraints in released airflow versions
----------------------------------------

The constraints we publish for released versions of Airflow are the latest tested versions of dependencies,
that we know worked with Airflow at the time of release. However, in many cases, if there are newer versions
of those dependencies released later, they have a chance to work with Airflow as well, as we usually do not
upper-bind those dependencies and you should be able to test and upgrade to newer versions even if the
constraint files have an earlier version of the dependency.

We almost never (except in very exceptional cases which do not allow users to use constraints to install
airflow) update those constraint files after specific version of Airflow is released, and we do not re-publish
Airflow reference container images with updated dependencies, so users are on their own to update selected
dependencies if they want to, and test if they work with Airflow. What you can do in case your scans
show some CVEs that you need to update is described in :ref:`docker-stack:fixing-image-at-release-time`.

The easiest way to get latest, CVE-free dependencies is to upgrade to the latest released version
of Airflow and keep doing it frequently as we release, this will make it overall easier for the users
to handle the upgrade process when they do it incrementally and more often, rather than jump a number
of versions at once.

What you do if you detect a CVE in a 3rd-party dependency
---------------------------------------------------------

There is a chance that you are using some scans that will show you CVE in a 3rd-party dependency that is
used by Airflow and you would like to get rid of those. There are a few things you can do in such case:

* First of all - resist an urge to open a public issue - especially if you have no idea if Airflow and your
  deployment is impacted. Such issues will be usually closed with reference to this document and you
  will be expected to follow it.

* Similar to GitHub issues, do not open a PR that exposes details of a potential CVE or security vulnerability before it has been properly reviewed and responsibly disclosed to the security team, and explicitly approved by them to proceed with a fix PR according to their instructions.
* Check if you can upgrade yourself to the newer version of dependency that is not vulnerable to the CVE, and
  test if it works with Airflow. If it does, you can use it in your deployment even if the constraints for
  your version of Airflow show an older version of the dependency. Feel free to start a public discussion (in
  `GitHub Discussions <https://github.com/apache/airflow/discussions>`_ ) in "Show and Tell" section where
  you will share it with others and encourage them to do the same if you successfully tested and upgraded the
  dependency. That helps other community members to be more confident about upgrading and might make them
  aware of some of 3rd-party vulnerabilities that they were not aware of.

* In case your version of Airflow or some of it's providers, prevent you from upgrading to a non-vulnerable version of the
  dependency, you can see if latest versions of Airflow or providers removed any limitations - you can check
  constraint files and :doc:`sbom` we publish for all Airflow versions (industry-standard way of capturing
  inventory of dependencies). If you see that you can upgrade, upgrade - ideally - to latest versions of
  Airflow and providers, but if you cannot - upgrade to the latest version that allows you to upgrade the
  dependency to a non-vulnerable version.  Again, if you successfully do it, please share your experience in
  GitHub Discussions.

* Check if the dependency that you have problem with is already upgraded in the ``main`` branch of Airflow -
  `Main constraints <https://github.com/apache/airflow/tree/constraints-main>`_. If it is, this means that
  the dependency will be upgraded in the next "MINOR" version of Airflow, and you can prepare for the upgrade
  by testing the "main" branch with the newer version of the dependency, participate in the release candidate.
  testing that is announced at airflow Dev list (see `Community page <https://airflow.apache.org/community/>`_
  for instructions on how you can subscribe. Helping to test such release candidates and testing upgrade
  process is by far the fastest way to speed up the process of releasing the next "MINOR" version of Airflow
  with the non-vulnerable version of the dependency. Also contributing to such release in general is a
  great way to give back to the community and help it to grow and speed up such release process. Also
  check related changes and see if the change that allowed it is already back-ported to the latest released
  "MINOR" version of Airflow and planned for the next "PATCHLEVEL" release (such PR will have future
  "PATCHLEVEL" version assigned as milestone, and the dependency should be upgraded in the constraints file
  for that version (For example 3.1.* versions has latest constraint files in the
  `3-1 constraints <https://github.com/apache/airflow/tree/constraints-3-1>`_ ).

* If such dependency change is not yet back-ported to the latest released "MINOR" version of Airflow, but you want
  to help with it - we recommend you to open PR to the ``v3-N-test`` branch - it can usually be done following
  the cherry-pick process described in the
  `Developer documentation <https://github.com/apache/airflow/blob/main/dev/README_AIRFLOW3_DEV.md#merging-prs-targeted-for-airflow-3x>`_
  Note, that we never back-port things to older "MINOR" versions of Airflow, so if the commit is not
  back-ported to the latest released "MINOR" version of Airflow, it will not be back-ported to any
  older "MINOR" version of Airflow.

* If you really want to upgrade to a non-vulnerable version of the dependency and cannot provide a Proof Of
  Concept showing that Airflow is affected, and you cannot see it in latest versions of Airflow - you can
  attempt to investigate the issue yourself and prepare a PR that will allow airflow to upgrade to the
  new dependency. Avoid mentioning that the reason for the upgrade is a CVE, as we do not want to have public
  discussions about CVEs that are not proven to be exploitable in Airflow. Instead, you can just say that you
  want to upgrade to a newer version of the dependency and you want to share it with others in case
  they want to do the same. This can be a contribution to Airflow or to the dependency or even to other
  dependencies. Airflow contributors have a good tool that checks why specific dependency cannot be upgraded
  and it provides a ``uv`` resolution track that you can analyse to understand which dependencies are
  blocking the upgrade, you can start GitHub Discussions showing output of the tool and asking for help from
  maintainers to explain you what needs to be done.

  This is how to run the tool (you need to have ``uv`` installed, you can install it with ``pip install uv``):

  .. code-block:: bash

      git clone git@github.com:apache/airflow.git
      uv tool install -e ./dev/breeze --force
      breeze release-management constraints-version-check --python 3.10 --package PACKAGE_NAME --explain-why

  Fragment of example output of such tool is shown below - indicating that ``apache-beam`` blocks upgrade of
  ``grpcio`` package to version 1.56.0:

  .. image:: ../img/checking_dependencies.png
     :align: center
     :alt: Output of uv tool for ``grpcio`` package upgradability status

  Output of that tool is quite technical and is provided by the ``uv`` tool but it shows all the conflicts
  that are preventing the upgrade of the dependency and it can be a good starting point to understand what needs
  to be done to allow the upgrade. Sharing the output in GitHub Discussions can be a good way to get help
  from maintainers and other contributors to understand what needs to be done.

* When you have proofs that Airflow is impacted by the CVE (you need to understand what the issue is about and
  understand how it can be exploited in Airflow and your deployment), you should prepare a Proof Of Concept (PoC)
  showing how the CVE can be exploited, and share it with us in private, following our
  `Security policy <https://github.com/apache/airflow/security/policy>`_.
