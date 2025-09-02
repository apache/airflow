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

Provider distributions
======================

Airflow is split into core and providers. They are delivered as separate distributions:

* ``apache-airflow`` - meta distributions of Apache Airflow installing airflow, task_sdk, providers and airflow-core together
* ``apache-airflow-core`` - core of Apache Airflow (there are few more s separated)
* ``apache-airflow-task-sdk`` - task-sdk distribution that are imported by the providers
* ``apache-airflow-providers-*`` - More than 90 providers to communicate with external services

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Where providers are kept in our repository
------------------------------------------

Airflow Providers are stored in a separate tree other than the Airflow Core (under ``providers`` directory).
Airflow's repository is a monorepo, that keeps multiple distributions in a single repository. This has a number
of advantages, because code and CI infrastructure and tests can be shared. Also contributions are happening to a
single repository - so no matter if you contribute to Airflow or Providers, you are contributing to the same
repository and project.

The tool ``uv`` is recommended to help manage this through it's ``workspace`` feature. While developing,
dependencies and extras for a provider can be installed using ``uv``'s ``sync`` command. Here is an example
for the amazon provider:

.. code:: bash

    uv sync --package apache-airflow-providers-amazon

This will synchronize all extras that you need for development and testing of Airflow and the Amazon provider
dependencies including runtime dependencies and all providers it depends on and its dependencies.

See `local virtualenv <../07_local_virtualenv.rst>`_ or the uv project for more information.

Each provider is a separate python project, with its own ``pyproject.toml`` file and similar structure:

.. code-block:: text

  PROVIDER
         |- pyproject.toml   # project configuration
         |- provider.yaml    # additional metadata for provider
         |- src
         |.   \- airflow.providers.PROVIDER
         |                                \ # here are hooks, operators, sensors, transfers
         |- docs   # docs for provider are stored here
         \- tests
                | -unit
                |      | PROVIDER
                |               \ # here unit test code is present
                | - integration
                |             | PROVIDER
                |                      \ # here integration test code is present
                \- system
                         | PROVIDER
                                  \ # here system test code is present

PROVIDER is the name of the provider distribution. It might be single directory (google, amazon, smtp) or in some
cases we have a nested structure one level down (``apache/cassandra``, ``apache/druid``, ``microsoft/winrm``,
``common.io`` for example).

What are the pyproject.toml and provider.yaml files
---------------------------------------------------

On top of the standard ``pyproject.toml`` file where we keep project information,
we have ``provider.yaml`` file in the provider's module of the ``providers``.

This file contains:

* user-facing name of the provider distribution
* description of the distribution that is available in the documentation
* list of versions of distribution that have been released so far
* list of integrations, operators, hooks, sensors, transfers provided by the provider (useful for documentation generation)
* list of connection types, extra-links, secret backends, auth backends, and logging handlers (useful to both
  register them as they are needed by Airflow and to include them in documentation automatically).

Note that the ``provider.yaml`` file is regenerated automatically when the provider is released so you should
not modify it - except updating dependencies, as your changes will be lost.

Eventually we might migrate ``provider.yaml`` fully to ``pyproject.toml`` file but it would require custom
``tool.airflow`` toml section to be added to the ``pyproject.toml`` file.

How to manage provider's dependencies
-------------------------------------

If you want to add dependencies to the provider, you should add them to the corresponding ``pyproject.toml``
file. For regular local virtualenv development you also need to run ``uv sync --extra PROVIDER`` after you
do it or run ``pip install -e ./providers/PROVIDER`` to install the provider in development mode in your venv
and IDE. If you are using breeze you should also run the below command to regenerate all files that
needs to be updated after you change dependencies:

.. code:: bash

    prek update-providers-dependencies --all-files

If you have ``prek`` installed, this will be done automatically for you when you commit the changes and
you should do it before you make a PR with such changed dependency changes

Also, you should rebuild the image ``breeze ci-image build`` or answer ``y`` when you are asked to rebuild the
image for the new dependencies to be used in the Breeze CI environment.

Provider's cross-dependencies
-----------------------------

Some of the providers have cross-dependencies with other providers distributions.
This typically happens for transfer operators where operators use hooks from the other providers
in case they are transferring data between the providers. The list of dependencies is maintained
(automatically with the ``update-providers-dependencies`` prek hook) in the
``generated/provider_dependencies.json``.

Cross-dependencies between providers are converted into optional dependencies (extras) - if
you need functionality from the other provider distribution you can install it adding [extra] after the
``apache-airflow-providers-PROVIDER`` for example:
``pip install apache-airflow-providers-google[amazon]`` in case you want to use GCP
transfer operators from Amazon ECS.

How to reuse code between tests in different providers
------------------------------------------------------

When you develop providers, you might want to reuse some of the code between tests in different providers.
This is possible by placing the code in ``test_utils`` in the ``devel-common/src`` directory.
The ``tests_common`` module is installed automatically by uv in the uv workspace.

Developing community managed providers
--------------------------------------

While you can develop your own providers, Apache Airflow has 60+ providers that are managed by the community.
They are part of the same repository as Apache Airflow (we use monorepo approach where different
parts of the system are developed in the same repository but then they are packaged and released separately).
All the community-managed providers are in ``providers`` folder and their code is placed as sub-directories of
``providers`` directory.

In order to allow the same Python Airflow sub-packages to be present in different distributions of the source tree,
we are heavily utilising `namespace packages <https://packaging.python.org/en/latest/guides/packaging-namespace-packages/>`_.
For now we have a bit of mixture of native (no ``__init__.py`` namespace packages) and pkgutil-style
namespace packages (with ``__init__.py`` and path extension) but we are moving
towards using only native namespace packages.

All the providers are available as ``apache-airflow-providers-<PROVIDER_ID>``
distributions when installed by users, but when you contribute to providers you can work on Airflow main
and install provider dependencies via ``editable`` extras (using uv workspace) - without
having to manage and install providers separately, you can easily run tests for the providers
and when you run Airflow from the ``main`` sources, all community providers are
automatically available for you.

The capabilities of the community-managed providers are the same as the third-party ones. When
the providers are installed from PyPI, they provide the entry-point containing the metadata as described
in the previous chapter. However when they are locally developed, together with Airflow, the mechanism
of discovery of the providers is based on ``provider.yaml`` file that is placed in the top-folder of
the provider. The ``provider.yaml`` is the single source of truth for the provider metadata and it is
there where you should add and remove dependencies for providers (following by running
``update-providers-dependencies`` prek hook to synchronize the dependencies with ``pyproject.toml``
of Airflow).

The ``provider.yaml`` file is compliant with the schema that is available in
`json-schema specification <https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/provider.yaml.schema.json>`_.

Thanks to that mechanism, you can develop community managed providers in a seamless way directly from
Airflow sources, without preparing and releasing them as distributions separately, which would be rather
complicated.

Regardless if you plan to contribute your provider, when you are developing your own, custom providers,
you can use the above functionality to make your development easier. You can add your provider
as a sub-folder of the ``airflow.providers`` Python package, add the ``provider.yaml`` file and install airflow
in development mode - then capabilities of your provider will be discovered by Airflow and you will see
the provider among other providers in ``airflow providers`` command output.


Local Release of a Specific Provider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When you develop a provider, you can release it locally and test it in your Airflow environment. Each provider
distribution is a standard Python distribution with its own ``pyproject.toml`` file and it should be as easy
as getting into the provider's distribution top directory and running the following command (you can
use any PEP-517 compliant build tool, but we recommend using ``flit`` for providers as it has very small
number of dependencies and is very fast. You can install flit for example with ``uv tool install flit``):

.. code-block:: bash

    flit build

Naming Conventions for providers
--------------------------------

In Airflow we standardized and enforced naming for providers, modules and classes.
those rules (introduced as AIP-21) were not only introduced but enforced using automated checks
that verify if the naming conventions are followed. Here is a brief summary of the rules, for
detailed discussion you can go to `AIP-21 Changes in import paths <https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths>`_

The rules are as follows:

* Provider distributions are all placed in ``providers`` folder and each provider provides
``airflow.providers.<PROVIDER_ID>`` Python namespace package.

* Providers are usually direct sub-folders of the 'providers' directory but in some cases they can be
  further split into sub-folders (for example 'apache' folder has 'cassandra', 'druid' ... provider folders ) out
  of which several different providers are produced (apache.cassandra, apache.druid). This is
  case when the providers are connected under common umbrella but very loosely coupled on the code level.
  Please note the separator of the provider distribution ID is a period, not a dash like the distribution
  names in PyPI ( ``microsoft.azure`` vs ``apache-airflow-providers-microsoft-azure``).

* In some cases the providers can have Python sub-packages but they are all delivered as single provider
  distributions (for example 'google' package contains 'ads', 'cloud' etc. sub-packages). This is in case
  the providers are connected under common umbrella and they are also tightly coupled on the code level.

* Typical structure of provider sources:
  * src
     *  airflow.providers.PROVIDER_ID
          * hooks -> hooks are stored here
          * operators -> operators are stored here
          * sensors -> sensors are stored here
          * secrets -> secret backends are stored here
          * transfers -> transfer operators are stored here
  * docs
  * tests
    * unit
       * PROVIDER
    * integration
       * PROVIDER
    * system
      * PROVIDER
          * example_dags -> example Dags are stored here (used for documentation and System Tests)

* Module names do not contain word "hooks", "operators" etc. The right type comes from
  the python package. For example 'hooks.datastore' module contains DataStore hook and
  'operators.datastore' contains DataStore operators.

* Class names contain 'Operator', 'Hook', 'Sensor' - for example DataStoreHook, DataStoreExportOperator

* Operator name usually follows the convention: ``<Subject><Action><Entity>Operator``
  (BigQueryExecuteQueryOperator) is a good example

* Transfer Operators are those that actively push data from one service/provider and send it to another
  service (might be for the same or another provider). This usually involves two hooks. The convention
  for those ``<Source>To<Destination>Operator``. They are not named *TransferOperator nor *Transfer.

* Operators that use external service to perform transfer (for example CloudDataTransferService operators
  are not placed in "transfers" Python package and do not have to follow the naming convention for
  transfer operators.

* It is often debatable where to put transfer operators but we agreed to the following criteria:

  * We use "maintainability" of the operators as the main criteria - so the transfer operator
    should be kept at the provider which has highest "interest" in the transfer operator

  * For Cloud Providers or Service providers that usually means that the transfer operators
    should land at the "target" side of the transfer

* Secret Backend name follows the convention: ``<SecretEngine>Backend``.

* Init Tests are grouped in parallel Python packages under "tests.providers" top level package.
   Module name is usually ``test_<object_to_test>.py``,

* System tests (not yet fully automatically run in CI but allowing to run e2e testing of particular provider)
  are named with ``example_*`` prefix.

Documentation for the community managed providers
-------------------------------------------------

When you are developing a community-managed provider, you are supposed to make sure it is well tested
and documented. Part of the documentation is ``provider.yaml`` file ``integration`` information and
``version`` information. This information is stripped-out from provider info available at runtime,
however it is used to automatically generate documentation for the provider.

If you have prek installed, it will warn you and let you know what changes need to be
done in the ``provider.yaml`` file when you add a new Operator, Hooks, Sensor or Transfer. You can
also take a look at the other ``provider.yaml`` files as examples.

Well documented provider contains those:

* index.rst with references to providers, Python package API used and example dags
* configuration reference
* class documentation generated from PyDoc in the code
* example dags
* how-to guides

You can see for example ``google`` provider which has very comprehensive documentation:

* `Documentation <../../providers/google/docs>`_
* `System tests/Example Dags <../providers/google/tests/system/google/>`_

Part of the documentation are example dags (placed in the ``tests/system`` folder). The reason why
they are in ``tests/system`` is because we are using the example dags for various purposes:

* showing real examples of how your provider classes (Operators/Sensors/Transfers) can be used
* snippets of the examples are embedded in the documentation via ``exampleinclude::`` directive
* examples are executable as system tests and some of our stakeholders run them regularly to
  check if ``system`` level integration is still working, before releasing a new version of the provider.

Testing the community managed providers
---------------------------------------

We have high requirements when it comes to testing the community managed providers. We have to be sure
that we have enough coverage and ways to tests for regressions before the community accepts such
providers.

* Unit tests have to be comprehensive and they should tests for possible regressions and edge cases
  not only "green path"

* Integration tests where 'local' integration with a component is possible (for example tests with
  MySQL/Postgres DB/Trino/Kerberos all have integration tests which run with real, dockerized components

* System Tests which provide end-to-end testing, usually testing together several operators, sensors,
  transfers connecting to a real external system

Breaking changes in the community managed providers
---------------------------------------------------

Sometimes we have to introduce breaking changes in the providers. We have to be very careful with that
and we have to make sure that we communicate those changes properly.

Generally speaking breaking change in provider is not a huge problem for our users. They can individually
downgrade the providers to lower version if they are not ready to upgrade to the new version and then
incrementally upgrade to the new versions of providers. This is because providers are installed as
separate providers and they are not tightly coupled with the core of Airflow and because we have a very
generous policy of supporting multiple versions of providers at the same time. All providers are in theory
backward compatible with future versions of Airflow, so you can upgrade Airflow and keep the providers
at the same version.

When you introduce a breaking change in the provider, you have to make sure that you communicate it
properly. You have to update ``changelog.rst`` file in the ``docs`` folder of the provider distribution.
Ideally you should provide a migration path for the users to follow in the``changelog.rst``.

If in doubt, you can always look at ``changelog.rst``  in other providers to see how we communicate
breaking changes in the providers.

It's important to note that the marking release as breaking / major is subject to the
judgment of release manager upon preparing the release.

Bumping minimum version of dependencies in providers
----------------------------------------------------

Generally speaking we are rather relaxed when it comes to bumping minimum versions of dependencies in the
providers. If there is a good reason to bump the minimum version of the dependency, you should simply do it.
This is because user might always install previous version of the provider if they are not ready to upgrade
the dependency (because for example another library of theirs is not compatible with the new version of the
dependency). In most case this will be actually transparent for the user because ``pip`` in most cases will
find and install a previous version of the provider that is compatible with your dependencies that conflict
with latest version of the provider.

------

You can read about Airflow `dependencies and extras <13_airflow_dependencies_and_extras.rst>`_ .
