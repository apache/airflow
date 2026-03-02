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

Contributors' guide
===================

Contributions are welcome and are greatly appreciated! Every little bit helps,
and credit will always be given.

This page aims to explain the basic concept of contributions. It contains links
to detailed documents for the different aspects of contribution. We encourage both
Open Source first timers as well as more experienced contributors to read and
learn about this community's contribution guidelines as it support easy and efficient collaboration.

Getting Started
----------------
New Contributor
...............

If you are a new contributor, please follow the `Contributors Quick Start <03a_contributors_quick_start_beginners.rst>`__
guide for a step-by-step introduction to setting up the development environment and making your first
contribution (15-minute path).

If you need a full development environment, test suite, and advanced tooling, please see the
`Seasoned Developers Guide <03_contributors_quick_start.rst>`__.

We also suggest you to check out `Contribution Workflow <18_contribution_workflow.rst>`__ in order to get an overview of how to
contribute to Airflow.

If you are new to the project, you might need some help in understanding how the dynamics
of the community work and hence can consider getting  mentorship from other members of the
community - mostly Airflow committers (maintainers). Mentoring new members of the community is part of
maintainers job so do not be afraid to ask them to help you. You can do it
via comments in your PR, asking on a devlist or via Slack. We also have a dedicated ``#new-contributors`` Slack channel where you can ask any questions
about making your first Pull Request (PR) contribution to the Airflow codebase - it's a safe space
where it is expected that people asking questions do not know a lot Airflow (yet!).
If you need help with Airflow see the Slack channel ``#user-troubleshooting``.

To check on how mentoring works for the projects under Apache Software Foundation's
`Apache Community Development - Mentoring <https://community.apache.org/mentoring/>`_.

Contribution Basics
....................

To learn about various roles and communication channels in the Airflow project:

* `Roles in Airflow Project <01_roles_in_airflow_project.rst>`__ describes
  the roles in the Airflow project and how they relate to each other.

* `How to communicate <02_how_to_communicate.rst>`__
  describes how to communicate with the community and how to get help.

* `How to contribute <04_how_to_contribute.rst>`__ describes the various ways of how you can contribute to Airflow.

To learn how to setup your environment for development and how to develop and test code:

* `Contributors quick start <03a_contributors_quick_start_beginners.rst>`__ describes
  how to set up your development environment and make your first contribution.

* `Pull requests <05_pull_requests.rst>`__ describes how you can create pull requests. It also includes the pull request guidelines and the coding standards.

* `Development environment <06_development_environments.rst>`__ describes the development environment
  used in Airflow.

  * `Local virtualenv <07_local_virtualenv.rst>`__ describes the setup and details of the local virtualenv
    development environment.

  * `Breeze <../dev/breeze/doc/README.rst>`__ describes the setup and details of the Breeze development environment.

* `Static code checks <08_static_code_checks.rst>`__ describes the static code checks used in Airflow.

* `Testing <09_testing.rst>`__ describes what kind of tests we have and how to run them.

* `Working with Git <10_working_with_git.rst>`__ describes the Git branches used in Airflow,
  how to sync your fork and how to rebase your PR.

* `Building documentation <11_documentation_building.rst>`__ describes how to build the documentation.


Advanced Topics
----------------
Developing Providers
.....................

You can learn how Airflow repository is a monorepo split into Airflow and providers,
and how to contribute to the providers:

* `Provider distributions <12_provider_distributions.rst>`__ describes the providers and how they
  are used in Airflow.


Airflow Deep Dive
..................

You can also dive deeper into more specific areas that are important for contributing to Airflow:

* `Airflow dependencies and extras <13_airflow_dependencies_and_extras.rst>`__ describes
  the dependencies - both required and optional (extras) used in Airflow.

* `Metadata database updates <14_metadata_database_updates.rst>`__ describes
  how to make changes in the metadata database.

* `Node environment setup <15_node_environment_setup.rst>`__ describes how to set up
  the node environment for Airflow UI.

* `Adding API endpoints <16_adding_api_endpoints.rst>`__ describes how to add API endpoints
  to the Airflow REST API.

* `Architecture diagram <17_architecture_diagrams.rst>`__ describes how to create and
  update the architecture diagrams embedded in Airflow documentation.

* `Execution API versioning <19_execution_api_versioning.rst>`__ describes how to
  version the Task Execution API and how to add new versions of the API.

* `Debugging Airflow Components <20_debugging_airflow_components.rst>`__ describes how to debug
  Airflow components using Breeze with debugpy and VSCode integration.
