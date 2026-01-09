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
`Contributors Quick Start <../contributing-docs/03b_contributors_quick_start_seasoned_developers.rst>`_
if you did not set up your local environment yet. We recommend using ``breeze`` to develop locally. This way you
easily be able to have an environment more similar to the one executed by GitHub CI workflow.

  .. code-block:: bash

      ./breeze

Using the code above you will set up Docker containers. These containers your local code to internal volumes.
In this way, the changes made in your IDE are already applied to the code inside the container and tests can
be carried out quickly.

In this how-to guide our example provider name will be ``<PROVIDER>``.
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

    GIT apache/airflow/
    └── providers/
                 └── <PROVIDER>/
                               ├── pyproject.toml
                               ├── provider.yaml
                               ├── src/
                               │   └── airflow/
                               │       └── providers/<PROVIDER>/
                               │                               ├── __init__.py
                               │                               ├── executors/
                               │                               │   ├── __init__.py
                               │                               │   └── *.py
                               │                               ├── hooks/
                               │                               │   ├── __init__.py
                               │                               │   └── *.py
                               │                               ├── notifications/
                               │                               │   ├── __init__.py
                               │                               │   └── *.py
                               │                               ├── operators/
                               │                               │   ├── __init__.py
                               │                               │   └── *.py
                               │                               ├── transfers/
                               │                               │   ├── __init__.py
                               │                               │   └── *.py
                               │                               └── triggers/
                               │                                   ├── __init__.py
                               │                                   └── *.py
                               └── tests/
                                        ├── unit\
                                        │       └── <PROVIDER>/
                                        │                     ├── __init__.py
                                        │                     ├── executors/
                                        │                     │   ├── __init__.py
                                        │                     │   └── test_*.py
                                        │                     ├── hooks/
                                        │                     │   ├── __init__.py
                                        │                     │   └── test_*.py
                                        │                     ├── notifications/
                                        │                     │   ├── __init__.py
                                        │                     │   └── test_*.py
                                        │                     ├── operators/
                                        │                     │   ├── __init__.py
                                        │                     │   └── test_*.py
                                        │                     ├── transfers/
                                        │                     │   ├── __init__.py
                                        │                     │   └── test_*.py
                                        │                     └── triggers/
                                        │                         ├── __init__.py
                                        │                         └── test_*.py
                                        ├── integration/<PROVIDER>/
                                        │                         ├── __init__.py
                                        │                         └── test_integration_*.py
                                        └── system/<PROVIDER>/
                                                             ├── __init__.py
                                                             └── example_*.py

Considering that you have already transferred your provider's code to the above structure, it will now be necessary
to create unit tests for each component you created. The example below I have already set up an environment using
breeze and I'll run unit tests for my Hook.

  .. code-block:: bash

      [Breeze:3.10.19] root@fafd8d630e46:/opt/airflow# python -m pytest providers/<PROVIDER>/tests/<PROVIDER>/hook/test_*.py

Integration tests
-----------------

See `Airflow Integration Tests <../contributing-docs/testing/integration_tests.rst>`_


Documentation
-------------

An important part of building a new provider is the documentation.
Some steps for documentation occurs automatically by ``prek`` see
`Installing prek guide <../contributing-docs/03b_contributors_quick_start_seasoned_developers.rst#prek>`_

Those are important files in the Airflow source tree that affect providers. The ``pyproject.toml`` in root
Airflow folder is automatically generated based on content of ``provider.yaml`` file in each provider
when ``prek hook`` is run. Files such as ``extra-packages-ref.rst`` should be manually updated because
they are manually formatted for better layout and ``prek hook`` will just verify if the information
about provider is updated there. Files like ``commit.rst`` and ``CHANGELOG`` are automatically updated
by ``breeze release-management`` command by release manager when providers are released.

  .. code-block:: bash

     ├── pyproject.toml
     └── providers/<PROVIDER>/src/airflow/providers/
                                                   ├── provider.yaml
                                                   ├── pyproject.toml
                                                   ├── CHANGELOG.rst
                                                   │
                                                   └── docs/
                                                       ├── integration-logos
                                                       │                   └── <PROVIDER>.png
                                                       ├── index.rst
                                                       ├── commits.rst
                                                       ├── connections.rst
                                                       └── operators/
                                                           └── <PROVIDER>.rst

There is a chance that your provider's name is not a common English word.
In this case is necessary to add it to the file ``docs/spelling_wordlist.txt``.

Add your provider dependencies into ``provider.yaml`` under ``dependencies`` key..
If your provider doesn't have any dependency add a empty list.

In the ``docs/apache-airflow-providers-<PROVIDER>/connections.rst``:

- add information how to configure connection for your provider.

In the provider's ``docs/operators/<PROVIDER>.rst`` add information
how to use the Operator. It's important to add examples and additional information if your
Operator has extra-parameters.

  .. code-block:: RST

      .. _howto/operator:NewProviderOperator:

      NewProviderOperator
      ===================

      Use the :class:`~airflow.providers.<PROVIDER>.operators.NewProviderOperator` to do something
      amazing with Airflow!

      Using the Operator
      ^^^^^^^^^^^^^^^^^^

      The NewProviderOperator requires a ``connection_id`` and this other awesome parameter.
      You can see an example below:

      .. exampleinclude:: /../../<PROVIDER>/example_dags/example_<PROVIDER>.py
          :language: python
          :start-after: [START howto_operator_<PROVIDER>]
          :end-before: [END howto_operator_<PROVIDER>]


Copy from another, similar provider the docs: ``docs/*.rst``:

At least those docs should be present

* security.rst
* changelog.rst
* commits.rst
* index.rst
* installing-providers-from-sources.rst
* configurations-ref.rst - if your provider has ``config`` element in provider.yaml with configuration options
  specific for your provider

Make sure to update/add all information that are specific for the new provider.

In the ``providers/<PROVIDER>/src/airflow/providers/<PROVIDER>/provider.yaml`` add information of your provider:

  .. code-block:: yaml

      package-name: apache-airflow-providers-<PROVIDER>
      name: <PROVIDER>
      description: |
        `<PROVIDER> <https://example.io/>`__
      versions:
        - 1.0.0

      integrations:
        - integration-name: <PROVIDER>
          external-doc-url: https://www.example.io/
          logo: /docs/integration-logos/<PROVIDER>.png
          how-to-guide:
            - /docs/apache-airflow-providers-<PROVIDER>/operators/<PROVIDER>.rst
          tags: [service]

      operators:
        - integration-name: <PROVIDER>
          python-modules:
            - airflow.providers.<PROVIDER>.operators.<PROVIDER>

      hooks:
        - integration-name: <PROVIDER>
          python-modules:
            - airflow.providers.<PROVIDER>.hooks.<PROVIDER>

      sensors:
        - integration-name: <PROVIDER>
          python-modules:
            - airflow.providers.<PROVIDER>.sensors.<PROVIDER>

      connection-types:
        - hook-class-name: airflow.providers.<PROVIDER>.hooks.<PROVIDER>.NewProviderHook
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

    _____ ERROR collecting providers/apache/beam/tests/apache/beam/operators/test_beam.py ______
    ImportError while importing test module '/opt/airflow/providers/apache/beam/tests/apache/beam/operators/test_beam.py'.
    Hint: make sure your test modules/packages have valid Python names.
    Traceback:
    /usr/local/lib/python3.8/importlib/__init__.py:127: in import_module
        return _bootstrap._gcd_import(name[level:], package, level)
    providers/apache/beam/tests/apache/beam/operators/test_beam.py:25: in <module>
        from airflow.providers.apache.beam.operators.beam import (
    airflow/providers/apache/beam/operators/beam.py:35: in <module>
        from airflow.providers.google.cloud.hooks.dataflow import (
    airflow/providers/google/cloud/hooks/dataflow.py:32: in <module>
        from google.cloud.dataflow_v1beta3 import GetJobRequest, Job, JobState, JobsV1Beta3AsyncClient, JobView
    E   ModuleNotFoundError: No module named 'google.cloud.dataflow_v1beta3'
    _ ERROR collecting providers/microsoft/azure/tests/microsoft/azure/transfers/test_azure_blob_to_gcs.py _


The fix is to add this line at the top of the ``providers/apache/beam/tests/apache/beam/operators/test_beam.py`` module:

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
  update the tests for PROD docker image in ``docker-tests/tests/docker_tests/test_prod_image.py``.

* Some of the suspended providers might also fail ``breeze`` unit tests that expect a fixed set of providers.
  Those tests should be adjusted (but this is not very likely to happen, because the tests are using only
  the most common providers that we will not be likely to suspend).

Bumping min Airflow version
===========================

We regularly bump min Airflow version for all providers we release. This bump is done according to our
`Provider policies <https://github.com/apache/airflow/blob/main/PROVIDERS.rst>`_ and it is only applied
to non-suspended/removed providers. We are running basic import compatibility checks in our CI and
the compatibility checks should be updated when min Airflow version is updated.

Details on how this should be done are described in
`Provider policies <https://github.com/apache/airflow/blob/main/dev/README_RELEASE_PROVIDER_PACKAGES.md>`_

Conditional provider variants
=============================

Sometimes providers need to have different variants for different versions of Airflow. This is done by:

* copying ``version_compat.py`` from one of the providers that already have conditional variants to
  the root package of the provider you are working on

* importing the ``AIRFLOW_V_X_Y_PLUS`` that you need from that imported ``version_compat.py`` file.

The main reasons we are doing it in this way:

* checking version >= in Python has a non-obvious problem that the pre-release version is always considered
  lower than the final version. This is why we are using ``AIRFLOW_V_X_Y_PLUS`` to check for the version
  that is greater or equal to the version we are checking against - because we want the RC candidates
  to be considered as equal to the final version (because those RC candidates already contain the feature
  that is added in the final version).
* We do not want to add dependencies to another provider (say ``common.compat``) without strong need
* Even if the code is duplicated, it is just one ``version_compat.py`` file that is wholly copied
  and it is not a big deal to maintain it.
* There is a potential risk of one provider importing the same ``AIRFLOW_V_X_Y_PLUS`` from another provider
  (and introduce accidental dependency) or from test code (which should not happen), but we are preventing it
  via prek hook ``check-imports-in-providers`` that will fail if the
  ``version_compat`` module is imported from another provider or from test code.

Releasing pre-installed providers for the first time
====================================================

When releasing providers for the first time, you need to release them in state ``not-ready``.
This will make it available for release management commands, but it will not be added to airflow's
preinstalled providers list - allowing Airflow in main ``CI`` builds to be built without expecting the
provider to be available in PyPI.

You need to add ``--include-not-ready-providers`` if you want to add them to the list of providers
considered by the release management commands.

As soon as the provider is released, you should update the provider to ``state: ready``.

Releasing providers for past releases
=====================================

Sometimes we might want to release provider for previous MAJOR when new release is already
released (or bumped in main). This is done by releasing them from ``providers-<PROVIDER>/vX-Y`` branch
- for example ``providers-fab/v1-5`` can be used to release the ``1.5.2`` when ``2.0.0`` is already being
released or voted on.

The release process looks like usual, the only difference is that the specific branch is used to release
the provider and update all documentation, the changes and cherry-picking should be targeting that
branch.

Suspending providers
====================

As of April 2023, we have the possibility to suspend individual providers, so that they are not holding
back dependencies for Airflow and other providers. The process of suspending providers is described
in `description of the process <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#suspending-releases-for-providers>`_

Technically, suspending a provider is done by setting ``state: suspended``, in the provider.yaml of the
provider. This should be followed by committing the change and either automatically or manually running
prek hooks that will either update derived configuration files or ask you to update them manually.
Note that you might need to run prek several times until all the static checks pass,
because modification from one prek hook might impact other prek hooks.

If you have prek installed, it will be run automatically on commit. If you want to run it
manually after commit, you can run it via ``prek --last-commit`` some of the tests might fail
because suspension of the provider might cause changes in the dependencies, so if you see errors about
missing dependencies imports, non-usable classes etc., you will need to build the CI image locally
via ``breeze build-image --python 3.9 --upgrade-to-newer-dependencies`` after the first prek
and then run the static checks again.

If you want to be absolutely sure to run all static checks you can always do this via
``prek --all-files``.

Some of the manual modifications you will have to do (in both cases ``prek`` will guide you on what to do

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
* ``breeze release-management prepare-provider-distributions``
* ``breeze release-management publish-docs``

For all those commands, release manager needs to specify ``--include-removed-providers`` when all providers
are built or must add the provider id explicitly during the release process.
Except the changelog that needs to be maintained manually, all other documentation (main page of the provider
documentation, PyPI README), will be automatically updated to include removal notice.
