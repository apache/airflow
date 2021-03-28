
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

Community Providers
===================

.. contents:: :local:

How-to creating a new community provider
----------------------------------------

This document gathers the necessary steps to create a new community provider and also guidelines for updating
the existing ones. You should be aware that providers may have distinct distinctions that may not be covered in
this guide. The sequence described was designed to meet the most linear flow possible in order to develop a
new provider.

Another recommendation that will help you is to look for a provider that works similar to yours. That way it will
help you to set up tests and other dependencies.

First, you need to set up your local development environment. See `Contribution Quick Start <https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst>`_
if you didn't set up your local environment yet. We recommend using the set up through ``breeze``. This way you
will easily be able to have an environment more similar to the one executed by the validation code of Github CI
workflow.

  .. code-block:: bash

      ./breeze

Using the code above you will set up Docker containers. These containers your local code to internal volumes.
In this way, the changes made in your IDE are already applied to the code inside the container and tests can
be carried out quickly.


Initial Code and Unit Tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most likely you have developed a version of the provider using some local customization and now you need to
transfer this code to the Airflow project. Below is described all the initial code structure that
the provider may need. Understand that not all providers will need all the components described in this structure.
If you still have doubts about building your provider, we recommend that you read the initial provider guide and
open a issue on Github so the community can help you.

  .. code-block:: bash

      airflow/
      ├── providers/new_provider/
      │   ├── __init__.py
      │   ├── example_dags/
      │   │   ├── __init__.py
      │   │   └── example_new_provider.py
      │   ├── hooks/
      │   │   ├── __init__.py
      │   │   └── new_provider.py
      │   ├── operators/
      │   │   ├── __init__.py
      │   │   └── new_provider.py
      │   ├── sensors/
      │   │   ├── __init__.py
      │   │   └── new_provider.py
      │   └── transfers/
      │       ├── __init__.py
      │       └── new_provider.py
      └── tests/providers/new_provider/
          ├── __init__.py
          ├── hooks/
          │   ├── __init__.py
          │   └── test_new_provider.py
          ├── operators/
          │   ├── __init__.py
          │   ├── test_new_provider.py
          │   └── test_new_provider_system.py
          ├── sensors/
          │   ├── __init__.py
          │   └── test_new_provider.py
          └── transfers/
              ├── __init__.py
              └── test_new_provider.py

Considering that you have already transferred your provider's code to the above structure, it will now be necessary
to create unit tests for each component you created. The example below I have already set up an environment using
breeze and I'll run unit tests for my Hook.

  .. code-block:: bash

      root@fafd8d630e46:/opt/airflow# python -m pytest tests/providers/new_provider/hook/new_provider.py

Update Airflow validation tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are some tests that Airflow performs to ensure consistency that is related to the providers.

  .. code-block:: bash

      airflow/scripts/in_container/
      └── run_install_and_test_provider_packages.sh
      tests/core/
      └── test_providers_manager.py

Change expected number of providers, hooks and connections if needed in ``run_install_and_test_provider_packages.sh`` file.

Add your provider information in the following variables in ``test_providers_manager.py``:

  - add your provider to ``ALL_PROVIDERS`` list;
  - add your provider into ``CONNECTIONS_LIST`` if your provider create a new connection type.


Integration tests
^^^^^^^^^^^^^^^^^

[to do]
See `Airflow Integration Tests <https://github.com/apache/airflow/blob/master/TESTING.rst#airflow-integration-tests>`_


Documentation
^^^^^^^^^^^^^

An important part of building a new provider is the documentation. The better documented the easier the users
will be able to use the new provider and also helped to add new features.

  .. code-block:: bash

      airflow/
      ├── INSTALL
      ├── CONTRIBUTING.rst
      ├── setup.py
      ├── docs/
      │   ├── spelling_wordlist.txt
      │   ├── apache-airflow/
      │   │   └── extra-packages-ref.rst
      │   ├── integration-logos/new_provider/
      │   │   └── New_Provider.png
      │   └── apache-airflow-providers-new_provider/
      │       ├── index.rst
      │       ├── commits.rst
      │       ├── connections.rst
      │       └── operators/
      │           └── new_provider.rst
      └── providers/
          ├── dependencies.json
          └── new_provider/
              ├── provider.yaml
              └── CHANGELOG.rst


The ``airflow/providers/dependencies.json`` is automatically updated by pre-commit.

There is a chance that your provider's name is not an common English word.
In this case is necessary to add it to the file ``docs/spelling_wordlist.txt``. This file begin with capitalize words and
lowercase in the second block.

  .. code-block:: bash

    Namespace
    Neo4j
    Nextdoor
    New_Provider (new line)
    Nones
    NotFound
    Nullable
    ...
    neo4j
    neq
    networkUri
    new_provider (new line)
    nginx
    nobr
    nodash

Add your provider dependencies into **PROVIDER_REQUIREMENTS** variable in ``setup.py``. If your provider doesn't have
any dependency add a empty list.

  .. code-block:: python

      PROVIDERS_REQUIREMENTS: Dict[str, List[str]] = {
          ...
          'microsoft.winrm': winrm,
          'mongo': mongo,
          'mysql': mysql,
          'neo4j': neo4j,
          'new_provider': [],
          'odbc': odbc,
          ...
          }

In the ``CONTRIBUTING.rst`` adds:

  - your provider name in the list in the **Extras** section
  - your provider dependencies in the **Provider Packages** section table, only if your provider has external dependencies.


In the ``INSTALL`` file adds:

  - your provider to the **Extras** section list.

In the ``docs/apache-airflow-providers-new_provider/connections.rst``:

  - add information how to connect to your providers.

In the ``docs/apache-airflow-providers-new_provider/operators/new_provider.rst``:

- add information how to use the Operator. It's important to add examples and additional information if your Operator has extra-parameters.

  .. code-block:: RST

      .. _howto/operator:NewProviderOperator:

      NewProviderOperator
      ===================

      Use the :class:`~airflow.providers.new_provider.operators.NewProviderOperator` to do something
      amazing with Airflow!

      Using the Operator
      ^^^^^^^^^^^^^^^^^^

      The NewProviderOperator requires a ``connection_id`` and this other awesome parameter.
      You can see an example below:

      .. exampleinclude:: /../../airflow/providers/new_provider/example_dags/example_new_provider.py
          :language: python
          :start-after: [START howto_operator_new_provider]
          :end-before: [END howto_operator_new_provider]


In the ``docs/apache-airflow-providers-new_provider/commits.rst``:

  - add the initial information of your providers. This file is updated automatically by Airflow. Below is shown an example.

  .. code-block:: RST

      package apache-airflow-providers-new_provider
      ---------------------------------------------

      `New_Provider <https://example.io/>`__


      This is detailed commit list of changes for versions provider package: ``new_provider``.
      For high-level changelog, see :doc:`package information including changelog <index>`.

In the ``docs/apache-airflow-providers-new_provider/index.rst``:

  - add all information of the purpose of your provider. It is recommended to check with another provider to help you complete this document as best as possible.


In the ``/airflow/providers/new_provider/CHANGELOG`` add the initial information. Providers start with 1.0.0 version.

  .. code-block:: RST

      Changelog
      ---------

      1.0.0
      .....

      Initial version of the provider.

In the ``airflow/providers/new_provider/provider.yaml`` add information of your provider:

  .. code-block:: yaml

      package-name: apache-airflow-providers-new_provider
      name: New_Provider
      description: |
        `New_Provider <https://example.io/>`__
      versions:
        - 1.0.0

      integrations:
        - integration-name: New_Provider
          external-doc-url: https://www.example.io/
          logo: /integration-logos/new_provider/New_Provider.png
          how-to-guide:
            - /docs/apache-airflow-providers-new_provider/operators/new_provider.rst
          tags: [service]

      operators:
        - integration-name: New_Provider
          python-modules:
            - airflow.providers.new_provider.operators.new_provider

      hooks:
        - integration-name: New_Provider
          python-modules:
            - airflow.providers.new_provider.hooks.new_provider

      sensors:
        - integration-name: New_Provider
          python-modules:
            - airflow.providers.new_provider.sensors.new_provider

      hook-class-names:
        - airflow.providers.new_provider.hooks.new_provider.NewProviderHook

After changing and creating these files you can build the documentation locally. The two commands below will
serve to accomplish this. The first will build your provider's documentation. The second will ensure that the
main Airflow documentation that involves some steps with the providers is also working.

  .. code-block:: bash

    ./breeze build-docs -- --package-filter apache-airflow-providers-new_provider
    ./breeze build-docs -- --package-filter apache-airflow

How-to Update a community provider
----------------------------------

[to do]

Airflow follows the `SEMVER <https://semver.org/>`_ standard for versioning.
When making a modification, please check which increment should be performed.
