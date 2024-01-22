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

.. contents:: :local:

Provider packages
-----------------

Airflow 2.0 is split into core and providers. They are delivered as separate packages:

* ``apache-airflow`` - core of Apache Airflow
* ``apache-airflow-providers-*`` - More than 70 provider packages to communicate with external services

The information/meta-data about the providers is kept in ``provider.yaml`` file in the right sub-directory
of ``airflow\providers``. This file contains:

* package name (``apache-airflow-provider-*``)
* user-facing name of the provider package
* description of the package that is available in the documentation
* list of versions of package that have been released so far
* list of dependencies of the provider package
* list of additional-extras that the provider package provides (together with dependencies of those extras)
* list of integrations, operators, hooks, sensors, transfers provided by the provider (useful for documentation generation)
* list of connection types, extra-links, secret backends, auth backends, and logging handlers (useful to both
  register them as they are needed by Airflow and to include them in documentation automatically).
* and more ...

If you want to add dependencies to the provider, you should add them to the corresponding ``provider.yaml``
and Airflow pre-commits and package generation commands will use them when preparing package information.

In Airflow 1.10 all those providers were installed together within one single package and when you installed
airflow locally, from sources, they were also installed. In Airflow 2.0, providers are separated out,
and not packaged together with the core when you build "apache-airflow" package, however when you install
airflow project locally with ``pip install -e ".[devel]"`` they are available on the same
environment as Airflow.

You should only update dependencies for the provider in the corresponding ``provider.yaml`` which is the
source of truth for all information about the provider.

Some of the packages have cross-dependencies with other providers packages. This typically happens for
transfer operators where operators use hooks from the other providers in case they are transferring
data between the providers. The list of dependencies is maintained (automatically with the
``update-providers-dependencies`` pre-commit) in the ``generated/provider_dependencies.json``.
Same pre-commit also updates generate dependencies in ``pyproject.toml``.

Cross-dependencies between provider packages are converted into extras - if you need functionality from
the other provider package you can install it adding [extra] after the
``apache-airflow-providers-PROVIDER`` for example:
``pip install apache-airflow-providers-google[amazon]`` in case you want to use GCP
transfer operators from Amazon ECS.

If you add a new dependency between different providers packages, it will be detected automatically during
and pre-commit will generate new entry in ``generated/provider_dependencies.json`` and update
``pyproject.toml`` so that the package extra dependencies are properly handled when package
might be installed when breeze is restarted or by your IDE or by running ``pip install -e ".[devel]"``.

Developing community managed provider packages
----------------------------------------------

While you can develop your own providers, Apache Airflow has 60+ providers that are managed by the community.
They are part of the same repository as Apache Airflow (we use ``monorepo`` approach where different
parts of the system are developed in the same repository but then they are packaged and released separately).
All the community-managed providers are in 'airflow/providers' folder and they are all sub-packages of
'airflow.providers' package. All the providers are available as ``apache-airflow-providers-<PROVIDER_ID>``
packages when installed by users, but when you contribute to providers you can work on airflow main
and install provider dependencies via ``editable`` extras - without having to manage and install providers
separately, you can easily run tests for the providers and when you run airflow from the ``main``
sources, all community providers are automatically available for you.

The capabilities of the community-managed providers are the same as the third-party ones. When
the providers are installed from PyPI, they provide the entry-point containing the metadata as described
in the previous chapter. However when they are locally developed, together with Airflow, the mechanism
of discovery of the providers is based on ``provider.yaml`` file that is placed in the top-folder of
the provider. The ``provider.yaml`` is the single source of truth for the provider metadata and it is
there where you should add and remove dependencies for providers (following by running
``update-providers-dependencies`` pre-commit to synchronize the dependencies with ``pyproject.toml``
of Airflow).

The ``provider.yaml`` file is compliant with the schema that is available in
`json-schema specification <https://github.com/apache/airflow/blob/main/airflow/provider.yaml.schema.json>`_.

Thanks to that mechanism, you can develop community managed providers in a seamless way directly from
Airflow sources, without preparing and releasing them as packages separately, which would be rather
complicated.

Regardless if you plan to contribute your provider, when you are developing your own, custom providers,
you can use the above functionality to make your development easier. You can add your provider
as a sub-folder of the ``airflow.providers`` package, add the ``provider.yaml`` file and install airflow
in development mode - then capabilities of your provider will be discovered by airflow and you will see
the provider among other providers in ``airflow providers`` command output.

Documentation for the community managed providers
-------------------------------------------------

When you are developing a community-managed provider, you are supposed to make sure it is well tested
and documented. Part of the documentation is ``provider.yaml`` file ``integration`` information and
``version`` information. This information is stripped-out from provider info available at runtime,
however it is used to automatically generate documentation for the provider.

If you have pre-commits installed, pre-commit will warn you and let you know what changes need to be
done in the ``provider.yaml`` file when you add a new Operator, Hooks, Sensor or Transfer. You can
also take a look at the other ``provider.yaml`` files as examples.

Well documented provider contains those:

* index.rst with references to packages, API used and example dags
* configuration reference
* class documentation generated from PyDoc in the code
* example dags
* how-to guides

You can see for example ``google`` provider which has very comprehensive documentation:

* `Documentation <docs/apache-airflow-providers-google>`_
* `Example DAGs <airflow/providers/google/cloud/example_dags>`_

Part of the documentation are example dags (placed in the ``tests/system`` folder). The reason why
they are in ``tests/system`` is because we are using the example dags for various purposes:

* showing real examples of how your provider classes (Operators/Sensors/Transfers) can be used
* snippets of the examples are embedded in the documentation via ``exampleinclude::`` directive
* examples are executable as system tests and some of our stakeholders run them regularly to
  check if ``system`` level instagration is still working, before releasing a new version of the provider.

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

You can read more about out approach for tests in `testing.rst <testing.rst>`_ but here
are some highlights.
