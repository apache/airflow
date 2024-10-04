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


**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Creating a new community provider
=================================

This document gathers the necessary steps to create a new community provider and also guidelines for updating
the existing ones. You should be aware that providers may have distinctions that may not be covered in
this guide. The sequence described was designed to meet the most linear flow possible in order to develop a
new provider.

Another recommendation that will help you is to look for a provider that works similar to yours. That way it will
help you to set up tests and other dependencies.

First, you need to set up your local development environment. See
`Contributors Quick Start <../../contributing-docs/03_contributors_quick_start.rst>`_
if you did not set up your local environment yet. We recommend using ``breeze`` to develop locally. This way you
easily be able to have an environment more similar to the one executed by GitHub CI workflow.

  .. code-block:: bash

      ./breeze

Using the code above you will set up Docker containers. These containers your local code to internal volumes.
In this way, the changes made in your IDE are already applied to the code inside the container and tests can
be carried out quickly.

In this how-to guide our example provider name will be ``<NEW_PROVIDER>``.
When you see this placeholder you must change for your provider name.


Initial Code and Unit Tests
---------------------------

Most likely you have developed a version of the provider using some local customization and now you need to
transfer this code to the Airflow project. Below is described all the initial code structure that
the provider may need. Understand that not all providers will need all the components described in this structure.
If you still have doubts about building your provider, we recommend that you read the initial provider guide and
open a issue on GitHub so the community can help you.

The folders are optional: example_dags, hooks, links, logs, notifications, operators, secrets, sensors, transfers,
triggers (and the list changes continuously).

  .. code-block:: bash

      airflow/
             ├── providers/<NEW_PROVIDER>/
             │              ├── __init__.py
             │              ├── executors/
             │              │   ├── __init__.py
             │              │   └── *.py
             │              ├── hooks/
             │              │   ├── __init__.py
             │              │   └── *.py
             │              ├── operators/
             │              │   ├── __init__.py
             │              │   └── *.py
             │              ├── transfers/
             │              │   ├── __init__.py
             │              │   └── *.py
             │              └── triggers/
             │                  ├── __init__.py
             │                  └── *.py
             └── tests
                     ├── providers/<NEW_PROVIDER>/
                     │              ├── __init__.py
                     │              ├── executors/
                     │              │   ├── __init__.py
                     │              │   └── test_*.py
                     │              ├── hooks/
                     │              │   ├── __init__.py
                     │              │   └── test_*>.py
                     │              ├── operators/
                     │              │   ├── __init__.py
                     │              │   ├── test_*.py
                     │              ...
                     │              ├── transfers/
                     │              │   ├── __init__.py
                     │              │   └── test_*.py
                     │              └── triggers/
                     │                  ├── __init__.py
                     │                  └── test_*.py
                     └── system/providers/<NEW_PROVIDER>/
                                           ├── __init__.py
                                           └── example_*.py


Considering that you have already transferred your provider's code to the above structure, it will now be necessary
to create unit tests for each component you created. The example below I have already set up an environment using
breeze and I'll run unit tests for my Hook.

  .. code-block:: bash

      root@fafd8d630e46:/opt/airflow# python -m pytest tests/providers/<NEW_PROVIDER>/hook/test_*.py

Adding chicken-egg providers
----------------------------

Sometimes we want to release provider that depends on the version of airflow that has not yet been released
- for example when we released ``common.io`` provider it had ``apache-airflow>=2.8.0`` dependency.

Add chicken-egg-provider to compatibility checks
................................................

Providers that have "min-airflow-version" set to the new, upcoming versions should be excluded in
all previous versions of compatibility check matrix in ``BASE_PROVIDERS_COMPATIBILITY_CHECKS`` in
``src/airflow_breeze/global_constants.py``. Please add it to all previous versions

Add chicken-egg-provider to constraint generation
..................................................

This is controlled by ``chicken_egg_providers`` property in Selective Checks - and our CI will automatically
build and use those chicken-egg providers during the CI process if pre-release version of Airflow is built.

The short ``provider id`` (``common.io`` for example) for such a provider should be added
to ``CHICKEN_EGG_PROVIDERS`` list in ``src/airflow_breeze/utils/selective_checks.py``:

This list will be kept here until the official version of Airflow the chicken-egg-providers depend on
is released and the version of airflow is updated in the ``main`` and ``v2-X-Y`` branch to ``2.X+1.0.dev0``
and ``2.X.1.dev0`` respectively. After that the chicken-egg providers will be correctly installed because
both ``2.X.1.dev0`` and ``2.X+1.0.dev0`` are considered by ``pip`` as ``>2.X.0`` (unlike ``2.X.0.dev0``).

The release process for Airflow includes cleaning the list after Airflow release is published, so the
provider will be removed from the list by release manager.


Why do we need to add chicken-egg providers to constraints generation
.....................................................................

The problem when generating constraints with chicken-eggo providers and building docker image for
pre-release versions of Airflow - because ``pip`` does not recognize the ``.dev0`` or ``.b1``
suffixes of those packages as valid in the ``>=X.Y.Z`` comparison.

When you want to install a provider package with ``apache-airflow>=2.8.0`` requirement and you have
``2.9.0.dev0`` airflow package, ``pip`` will not install the package, because it does not recognize
``2.9.0.dev0`` as a valid version for ``>=2.8.0`` dependency. This is because ``pip``
currently implements the minimum version selection algorithm requirement specified in packaging as
described in the packaging version specification
https://packaging.python.org/en/latest/specifications/version-specifiers/#handling-of-pre-releases

Currently ``pip`` only allows to include pre-release versions for all installed packages using ``--pre``
flag, but it does not have the possibility of selectively using this flag to only one package.
In order to implement our desired behaviour, we need the case where only ``apache-airflow`` is considered
as pre-release version while all the other dependencies only have stable versions and this is currently
not possible.

To work around this limitation, we have introduced the concept of "chicken-egg" providers. Those providers
are providers that are released together with the version of Airflow they depend on. They are released
with the same version number as the Airflow version they depend on, but with a different suffix. For example
``apache-airflow-providers-common-io==2.9.0.dev0`` is a chicken-egg provider for ``apache-airflow==2.9.0.dev0``.

However - we should not release providers with such exclusion to ``pypi``, so in order to allow our
CI to work with pre-release versions and perform both - constraint generation and image releasing,
we introduced workarounds in our tooling where in case we build a pre-release version of Airflow,
we will locally build the chicken-egg providers from sources and they are installed from local
directory instead of from PyPI.

This workaround might be removed if ``pip`` implements the possibility of selectively using ``--pre`` flag
for only one package (Which is foreseen as a possibility in the packaging specification but not implemented
by ``pip``).

.. note::

   The current solution of building pre-release images will not work well if the chicken-egg-provider is
   pre-installed package because slim imges will not use the chicken-egg-provider. This could be solved
   by adding ``--chicken-egg-providers`` flag to slim image building step in ``released_dockerhub_image.yml``
   but it would also require filtering out the non-pre-installed packages from it, so the current solution
   is to assume pre-installed packages are not chicken-egg providers.

Integration tests
-----------------

See `Airflow Integration Tests <../../contributing-docs/testing/integration-tests.rst>`_


Documentation
-------------

An important part of building a new provider is the documentation.
Some steps for documentation occurs automatically by ``pre-commit`` see
`Installing pre-commit guide <../../contributing-docs/03_contributors_quick_start.rst#pre-commit>`_

Those are important files in the airflow source tree that affect providers. The ``pyproject.toml`` in root
Airflow folder is automatically generated based on content of ``provider.yaml`` file in each provider
when ``pre-commit`` is run. Files such as ``extra-packages-ref.rst`` should be manually updated because
they are manually formatted for better layout and ``pre-commit`` will just verify if the information
about provider is updated there. Files like ``commit.rst`` and ``CHANGELOG`` are automatically updated
by ``breeze release-management`` command by release manager when providers are released.

  .. code-block:: bash

     ├── pyproject.toml
     ├── airflow/
     │   └── providers/
     │       └── <NEW_PROVIDER>/
     │           ├── provider.yaml
     │           └── CHANGELOG.rst
     │
     └── docs/
         ├── apache-airflow/
         │   └── extra-packages-ref.rst
         ├── integration-logos/<NEW_PROVIDER>/
         │   └── <NEW_PROVIDER>.png
         └── apache-airflow-providers-<NEW_PROVIDER>/
             ├── index.rst
             ├── commits.rst
             ├── connections.rst
             └── operators/
                 └── <NEW_PROVIDER>.rst


There is a chance that your provider's name is not a common English word.
In this case is necessary to add it to the file ``docs/spelling_wordlist.txt``.

Add your provider dependencies into ``provider.yaml`` under ``dependencies`` key..
If your provider doesn't have any dependency add a empty list.

In the ``docs/apache-airflow-providers-<NEW_PROVIDER>/connections.rst``:

- add information how to configure connection for your provider.

In the ``docs/apache-airflow-providers-<NEW_PROVIDER>/operators/<NEW_PROVIDER>.rst`` add information
how to use the Operator. It's important to add examples and additional information if your
Operator has extra-parameters.

  .. code-block:: RST

      .. _howto/operator:NewProviderOperator:

      NewProviderOperator
      ===================

      Use the :class:`~airflow.providers.<NEW_PROVIDER>.operators.NewProviderOperator` to do something
      amazing with Airflow!

      Using the Operator
      ^^^^^^^^^^^^^^^^^^

      The NewProviderOperator requires a ``connection_id`` and this other awesome parameter.
      You can see an example below:

      .. exampleinclude:: /../../airflow/providers/<NEW_PROVIDER>/example_dags/example_<NEW_PROVIDER>.py
          :language: python
          :start-after: [START howto_operator_<NEW_PROVIDER>]
          :end-before: [END howto_operator_<NEW_PROVIDER>]


Copy from another, similar provider the docs: ``docs/apache-airflow-providers-<NEW_PROVIDER>/*.rst``:

At least those docs should be present

* security.rst
* changelog.rst
* commits.rst
* index.rst
* installing-providers-from-sources.rst
* configurations-ref.rst - if your provider has ``config`` element in provider.yaml with configuration options
  specific for your provider

Make sure to update/add all information that are specific for the new provider.

In the ``airflow/providers/<NEW_PROVIDER>/provider.yaml`` add information of your provider:

  .. code-block:: yaml

      package-name: apache-airflow-providers-<NEW_PROVIDER>
      name: <NEW_PROVIDER>
      description: |
        `<NEW_PROVIDER> <https://example.io/>`__
      versions:
        - 1.0.0

      integrations:
        - integration-name: <NEW_PROVIDER>
          external-doc-url: https://www.example.io/
          logo: /integration-logos/<NEW_PROVIDER>/<NEW_PROVIDER>.png
          how-to-guide:
            - /docs/apache-airflow-providers-<NEW_PROVIDER>/operators/<NEW_PROVIDER>.rst
          tags: [service]

      operators:
        - integration-name: <NEW_PROVIDER>
          python-modules:
            - airflow.providers.<NEW_PROVIDER>.operators.<NEW_PROVIDER>

      hooks:
        - integration-name: <NEW_PROVIDER>
          python-modules:
            - airflow.providers.<NEW_PROVIDER>.hooks.<NEW_PROVIDER>

      sensors:
        - integration-name: <NEW_PROVIDER>
          python-modules:
            - airflow.providers.<NEW_PROVIDER>.sensors.<NEW_PROVIDER>

      connection-types:
        - hook-class-name: airflow.providers.<NEW_PROVIDER>.hooks.<NEW_PROVIDER>.NewProviderHook
        - connection-type: provider-connection-type

After changing and creating these files you can build the documentation locally. The two commands below will
serve to accomplish this. The first will build your provider's documentation. The second will ensure that the
main Airflow documentation that involves some steps with the providers is also working.

  .. code-block:: bash

    breeze build-docs <provider id>
    breeze build-docs apache-airflow

Additional changes needed for cross-dependent providers
=======================================================

Those steps above are usually enough for most providers that are "standalone" and not imported or used by
other providers (in most cases we will not suspend such providers). However some extra steps might be needed
for providers that are used by other providers, or that are part of the default PROD Dockerfile:

* Most of the tests for the suspended provider, will be automatically excluded by pytest collection. However,
  in case a provider is dependent on by another provider, the relevant tests might fail to be collected or
  run by ``pytest``. In such cases you should skip the whole test module failing to be collected by
  adding ``pytest.importorskip`` at the top of the test module.
  For example if your tests fail because they need to import ``apache.airflow.providers.google``
  and you have suspended it, you should add this line at the top of the test module that fails.

Example failing collection after ``google`` provider has been suspended:

  .. code-block:: txt

    _____ ERROR collecting tests/providers/apache/beam/operators/test_beam.py ______
    ImportError while importing test module '/opt/airflow/tests/providers/apache/beam/operators/test_beam.py'.
    Hint: make sure your test modules/packages have valid Python names.
    Traceback:
    /usr/local/lib/python3.8/importlib/__init__.py:127: in import_module
        return _bootstrap._gcd_import(name[level:], package, level)
    tests/providers/apache/beam/operators/test_beam.py:25: in <module>
        from airflow.providers.apache.beam.operators.beam import (
    airflow/providers/apache/beam/operators/beam.py:35: in <module>
        from airflow.providers.google.cloud.hooks.dataflow import (
    airflow/providers/google/cloud/hooks/dataflow.py:32: in <module>
        from google.cloud.dataflow_v1beta3 import GetJobRequest, Job, JobState, JobsV1Beta3AsyncClient, JobView
    E   ModuleNotFoundError: No module named 'google.cloud.dataflow_v1beta3'
    _ ERROR collecting tests/providers/microsoft/azure/transfers/test_azure_blob_to_gcs.py _


The fix is to add this line at the top of the ``tests/providers/apache/beam/operators/test_beam.py`` module:

  .. code-block:: python

    pytest.importorskip("apache.airflow.providers.google")


* Some of the other providers might also just import unconditionally the suspended provider and they will
  fail during the provider verification step in CI. In this case you should turn the provider imports
  into conditional imports. For example when import fails after ``amazon`` provider has been suspended:

  .. code-block:: txt

      Traceback (most recent call last):
        File "/opt/airflow/scripts/in_container/verify_providers.py", line 266, in import_all_classes
          _module = importlib.import_module(modinfo.name)
        File "/usr/local/lib/python3.8/importlib/__init__.py", line 127, in import_module
          return _bootstrap._gcd_import(name, package, level)
        File "<frozen importlib._bootstrap>", line 1006, in _gcd_import
        File "<frozen importlib._bootstrap>", line 983, in _find_and_load
        File "<frozen importlib._bootstrap>", line 967, in _find_and_load_unlocked
        File "<frozen importlib._bootstrap>", line 677, in _load_unlocked
        File "<frozen importlib._bootstrap_external>", line 728, in exec_module
        File "<frozen importlib._bootstrap>", line 219, in _call_with_frames_removed
        File "/usr/local/lib/python3.8/site-packages/airflow/providers/mysql/transfers/s3_to_mysql.py", line 23, in <module>
          from airflow.providers.amazon.aws.hooks.s3 import S3Hook
      ModuleNotFoundError: No module named 'airflow.providers.amazon'

or:

  .. code-block:: txt

  Error: The ``airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs`` object in transfers list in
  airflow/providers/microsoft/azure/provider.yaml does not exist or is not a module:
  No module named 'gcloud.aio.storage'

The fix for that is to turn the feature into an optional provider feature (in the place where the excluded
``airflow.providers`` import happens:

  .. code-block:: python

    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    except ImportError as e:
        from airflow.exceptions import AirflowOptionalProviderFeatureException

        raise AirflowOptionalProviderFeatureException(e)


* In case we suspend an important provider, which is part of the default Dockerfile you might want to
  update the tests for PROD docker image in ``docker_tests/test_prod_image.py``.

* Some of the suspended providers might also fail ``breeze`` unit tests that expect a fixed set of providers.
  Those tests should be adjusted (but this is not very likely to happen, because the tests are using only
  the most common providers that we will not be likely to suspend).

Bumping min airflow version
===========================

We regularly bump min airflow version for all providers we release. This bump is done according to our
`Provider policies <https://github.com/apache/airflow/blob/main/PROVIDERS.rst>`_ and it is only applied
to non-suspended/removed providers. We are running basic import compatibility checks in our CI and
the compatibility checks should be updated when min airflow version is updated.

Details on how this should be done are described in
`Provider policies <https://github.com/apache/airflow/blob/main/dev/README_RELEASE_PROVIDER_PACKAGES.md>`_

Releasing pre-installed providers for the first time
====================================================

When releasing providers for the first time, you need to release them in state ``not-ready``.
This will make it available for release management commands, but it will not be added to airflow's
preinstalled providers list - allowing airflow in main ``CI`` builds to be built without expecting the
provider to be available in PyPI.

You need to add ``--include-not-ready-providers`` if you want to add them to the list of providers
considered by the release management commands.

As soon as the provider is released, you should update the provider to ``state: ready``.

Suspending providers
====================

As of April 2023, we have the possibility to suspend individual providers, so that they are not holding
back dependencies for Airflow and other providers. The process of suspending providers is described
in `description of the process <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#suspending-releases-for-providers>`_

Technically, suspending a provider is done by setting ``state: suspended``, in the provider.yaml of the
provider. This should be followed by committing the change and either automatically or manually running
pre-commit checks that will either update derived configuration files or ask you to update them manually.
Note that you might need to run pre-commit several times until all the static checks pass,
because modification from one pre-commit might impact other pre-commits.

If you have pre-commit installed, pre-commit will be run automatically on commit. If you want to run it
manually after commit, you can run it via ``breeze static-checks --last-commit`` some of the tests might fail
because suspension of the provider might cause changes in the dependencies, so if you see errors about
missing dependencies imports, non-usable classes etc., you will need to build the CI image locally
via ``breeze build-image --python 3.9 --upgrade-to-newer-dependencies`` after the first pre-commit run
and then run the static checks again.

If you want to be absolutely sure to run all static checks you can always do this via
``pre-commit run --all-files`` or ``breeze static-checks --all-files``.

Some of the manual modifications you will have to do (in both cases ``pre-commit`` will guide you on what
to do.

* You will have to run  ``breeze setup regenerate-command-images`` to regenerate breeze help files
* you will need to update ``extra-packages-ref.rst`` and in some cases - when mentioned there explicitly -
  ``pyproject.toml`` to remove the provider from list of dependencies.

What happens under-the-hood as a result, is that ``pyproject.toml`` file is updated with
the information about available providers and their dependencies and it is used by our tooling to
exclude suspended providers from all relevant parts of the build and CI system (such as building CI image
with dependencies, building documentation, running tests, etc.)

Resuming providers
==================

Resuming providers is done by reverting the original change that suspended it. In case there are changes
needed to fix problems in the reverted provider, our CI will detect them and you will have to fix them
as part of the PR reverting the suspension.

Removing providers
==================

When removing providers from Airflow code, we need to make one last release where we mark the provider as
removed - in documentation and in description of the PyPI package. In order to that release manager has to
add "state: removed" flag in the provider yaml file and include the provider in the next wave of the
providers (and then remove all the code and documentation related to the provider).

The "removed: removed" flag will cause the provider to be available for the following commands (note that such
provider has to be explicitly added as selected to the package - such provider will not be included in
the available list of providers or when documentation is built unless --include-removed-providers
flag is used):

* ``breeze build-docs``
* ``breeze release-management prepare-provider-documentation``
* ``breeze release-management prepare-provider-packages``
* ``breeze release-management publish-docs``

For all those commands, release manager needs to specify ``--include-removed-providers`` when all providers
are built or must add the provider id explicitly during the release process.
Except the changelog that needs to be maintained manually, all other documentation (main page of the provider
documentation, PyPI README), will be automatically updated to include removal notice.
