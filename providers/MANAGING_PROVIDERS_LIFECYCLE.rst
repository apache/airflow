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

Managing Provider's Lifecycle
==============================

.. contents:: Table of Contents
   :depth: 3
   :local:

This document covers the **technical steps** for creating, releasing, suspending, and removing
community providers. For governance policies, lifecycle stages, and health metrics, see
`Provider Governance <PROVIDER_GOVERNANCE.rst>`_.

Before proposing a new provider, review the `acceptance process <ACCEPTING_PROVIDERS.rst>`_
and ensure you have:

1. At least two individuals willing to serve as stewards
2. Sponsorship from at least one existing Airflow Committer (if none of the stewards are a committer)
3. A commitment to meeting the incubation health metrics within 6 months
4. A plan to participate in quarterly governance updates on the devlist

Include this information in your ``[DISCUSSION]`` thread when proposing a new provider.


Creating a new community provider
===================================

This section gathers the necessary steps to create a new community provider and also guidelines for
updating existing ones. Providers may have distinctions not covered in this guide. The sequence
described was designed to meet the most linear flow possible in order to develop a new provider.

We recommend looking at an existing provider similar to yours for reference on tests, dependencies,
and structure.


Setting up the development environment
----------------------------------------

First, set up your local development environment. See
`Contributors Quick Start <../contributing-docs/03b_contributors_quick_start_seasoned_developers.rst>`_
if you have not done so yet. We recommend using ``breeze`` to develop locally, as it provides an
environment similar to the one used by GitHub CI workflows.

.. code-block:: bash

    ./breeze

This sets up Docker containers that mount your local code to internal volumes. Changes made in your
IDE are immediately available inside the container, allowing tests to be run quickly.

In this guide, ``<PROVIDER>`` is used as a placeholder for your provider name.


Code structure
---------------

Below is the directory structure a provider may need. Not all providers require all components ---
the folders are optional: example_dags, hooks, links, logs, notifications, operators, secrets,
sensors, transfers, triggers (and the list changes continuously).

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
                                        ├── unit/
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


Unit tests
-----------

Create unit tests for each component of your provider. Example of running unit tests inside breeze:

.. code-block:: bash

    [Breeze:3.10.19] root@fafd8d630e46:/opt/airflow# python -m pytest providers/<PROVIDER>/tests/<PROVIDER>/hook/test_*.py


Integration tests
------------------

See `Airflow Integration Tests <../contributing-docs/testing/integration_tests.rst>`_


Documentation
--------------

An important part of building a new provider is the documentation. Some steps for documentation
occur automatically by ``prek`` --- see
`Installing prek guide <../contributing-docs/03b_contributors_quick_start_seasoned_developers.rst#prek>`_.

Key files in the Airflow source tree that affect providers:

* The ``pyproject.toml`` in root Airflow folder is automatically generated based on content of
  ``provider.yaml`` file in each provider when ``prek hook`` is run.
* Files such as ``extra-packages-ref.rst`` should be manually updated because they are manually
  formatted for better layout and ``prek hook`` will just verify if the information about provider
  is updated there.
* Files like ``commit.rst`` and ``CHANGELOG`` are automatically updated by
  ``breeze release-management`` command by release manager when providers are released.

Documentation file structure:

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

If your provider's name is not a common English word, add it to ``docs/spelling_wordlist.txt``.

Add your provider dependencies into ``provider.yaml`` under ``dependencies`` key.
If your provider doesn't have any dependency add an empty list.

In the ``docs/apache-airflow-providers-<PROVIDER>/connections.rst``:

- add information how to configure connection for your provider.

In the provider's ``docs/operators/<PROVIDER>.rst`` add information how to use the Operator.
It's important to add examples and additional information if your Operator has extra-parameters.

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

Copy from another, similar provider the docs: ``docs/*.rst``. At least these docs should be present:

* security.rst
* changelog.rst
* commits.rst
* index.rst
* installing-providers-from-sources.rst
* configurations-ref.rst - if your provider has ``config`` element in provider.yaml with configuration
  options specific for your provider

Make sure to update/add all information that are specific for the new provider.


provider.yaml configuration
-----------------------------

In the ``providers/<PROVIDER>/src/airflow/providers/<PROVIDER>/provider.yaml`` add information of
your provider:

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


Building documentation locally
-------------------------------

After creating and updating the files, build the documentation locally. The first command builds your
provider's documentation, the second ensures the main Airflow documentation is also working:

.. code-block:: bash

    breeze build-docs <provider id>
    breeze build-docs apache-airflow


Conditional provider variants
==============================

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


Releasing providers
====================

Releasing pre-installed providers for the first time
-----------------------------------------------------

When releasing providers for the first time, you need to release them in state ``not-ready``.
This will make it available for release management commands, but it will not be added to airflow's
preinstalled providers list - allowing Airflow in main ``CI`` builds to be built without expecting the
provider to be available in PyPI.

You need to add ``--include-not-ready-providers`` if you want to add them to the list of providers
considered by the release management commands.

As soon as the provider is released, you should update the provider to ``state: ready``.


Releasing providers for past releases
---------------------------------------

Sometimes we might want to release provider for previous MAJOR when new release is already
released (or bumped in main). This is done by releasing them from ``providers-<PROVIDER>/vX-Y`` branch
- for example ``providers-fab/v1-5`` can be used to release the ``1.5.2`` when ``2.0.0`` is already being
released or voted on.

The release process looks like usual, the only difference is that the specific branch is used to release
the provider and update all documentation, the changes and cherry-picking should be targeting that
branch.


Bumping min Airflow version
-----------------------------

We regularly bump min Airflow version for all providers we release. This bump is done according to our
`Provider policies <PROVIDER_RELEASES.rst#upgrading-minimum-supported-version-of-airflow>`_ and it is only applied
to non-suspended/removed providers. We are running basic import compatibility checks in our CI and
the compatibility checks should be updated when min Airflow version is updated.

Details on how this should be done are described in
`Provider policies <https://github.com/apache/airflow/blob/main/dev/README_RELEASE_PROVIDER_PACKAGES.md>`_


Suspending providers
=====================

As of April 2023, we have the possibility to suspend individual providers, so that they are not holding
back dependencies for Airflow and other providers. The criteria and process for suspending providers is
described in `Suspending releases for providers <SUSPENDING_AND_REMOVING_PROVIDERS.rst#suspending-releases-for-providers>`_.

Technical steps
----------------

Suspending a provider is done by setting ``state: suspended`` in the provider.yaml of the
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

Some of the manual modifications you will have to do (in both cases ``prek`` will guide you on what to do):

* You will have to run ``breeze setup regenerate-command-images`` to regenerate breeze help files
* you will need to update ``extra-packages-ref.rst`` and in some cases - when mentioned there explicitly -
  ``pyproject.toml`` to remove the provider from list of dependencies.

What happens under-the-hood as a result, is that ``pyproject.toml`` file is updated with
the information about available providers and their dependencies and it is used by our tooling to
exclude suspended providers from all relevant parts of the build and CI system (such as building CI image
with dependencies, building documentation, running tests, etc.)


Handling cross-dependent providers
------------------------------------

The steps above are usually enough for most standalone providers. However, extra steps might be needed
for providers that are used by other providers, or that are part of the default PROD Dockerfile.

**Test collection failures**

Most tests for the suspended provider will be automatically excluded by pytest collection. However,
if another provider depends on the suspended one, its tests might fail to be collected. In such cases,
add ``pytest.importorskip`` at the top of the failing test module.

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

Fix by adding this line at the top of the failing test module:

.. code-block:: python

    pytest.importorskip("apache.airflow.providers.google")


**Import failures in provider verification**

Some providers might unconditionally import the suspended provider and fail during the provider
verification step in CI. Turn these imports into conditional imports.

Example when import fails after ``amazon`` provider has been suspended:

.. code-block:: txt

    Traceback (most recent call last):
      File "/opt/airflow/scripts/in_container/verify_providers.py", line 266, in import_all_classes
        _module = importlib.import_module(modinfo.name)
      ...
      File "/usr/local/lib/python3.8/site-packages/airflow/providers/mysql/transfers/s3_to_mysql.py", line 23, in <module>
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    ModuleNotFoundError: No module named 'airflow.providers.amazon'

or:

.. code-block:: txt

    Error: The ``airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs`` object in transfers list in
    airflow/providers/microsoft/azure/provider.yaml does not exist or is not a module:
    No module named 'gcloud.aio.storage'

Fix by turning the feature into an optional provider feature:

.. code-block:: python

    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    except ImportError as e:
        from airflow.exceptions import AirflowOptionalProviderFeatureException

        raise AirflowOptionalProviderFeatureException(e)


**Other potential impacts**

* If the suspended provider is part of the default Dockerfile, you might want to update the tests for
  PROD docker image in ``docker-tests/tests/docker_tests/test_prod_image.py``.
* Some suspended providers might also fail ``breeze`` unit tests that expect a fixed set of providers.
  Those tests should be adjusted (but this is not very likely, because the tests use only the most
  common providers that we will not be likely to suspend).


Resuming providers
===================

Resuming providers is done by reverting the original change that suspended it. In case there are changes
needed to fix problems in the reverted provider, our CI will detect them and you will have to fix them
as part of the PR reverting the suspension.


Removing providers
===================

When removing providers from Airflow code, we need to make one last release where we mark the provider as
removed - in documentation and in description of the PyPI package. For the criteria and process, see
`Removing community providers <SUSPENDING_AND_REMOVING_PROVIDERS.rst#removing-community-providers>`_.

Technical steps
----------------

The release manager has to add ``state: removed`` in the provider yaml file and include the provider in
the next wave of the providers (and then remove all the code and documentation related to the provider).

The ``state: removed`` flag will cause the provider to be available for the following commands (note that
such provider has to be explicitly added as selected to the package - it will not be included in
the available list of providers or when documentation is built unless ``--include-removed-providers``
flag is used):

* ``breeze build-docs``
* ``breeze release-management prepare-provider-documentation``
* ``breeze release-management prepare-provider-distributions``
* ``breeze release-management publish-docs``

For all those commands, release manager needs to specify ``--include-removed-providers`` when all providers
are built or must add the provider id explicitly during the release process.
Except the changelog that needs to be maintained manually, all other documentation (main page of the provider
documentation, PyPI README), will be automatically updated to include removal notice.
