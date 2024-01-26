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

This index of linked documents aims to explain the subject of contributions if you have not contributed to
any Open Source project, but it will also help people who have contributed to other projects learn about the
rules of that community.

.. contents:: :local:

New Contributor
---------------

If you are a new contributor, please follow the `Contributors Quick Start <./contributing-docs/03_contributors_quick_start.rst>`__
guide to get a gentle step-by-step introduction to setting up the development environment and making your
first contribution.

If you are new to the project, you might need some help in understanding how the dynamics
of the community works and you might need to get some mentorship from other members of the
community - mostly Airflow committers (maintainers). Mentoring new members of the community is part of
maintainers job so do not be afraid of asking them to help you. You can do it
via comments in your PR, asking on a devlist or via Slack. For your convenience,
we have a dedicated ``#development-first-pr-support`` Slack channel where you can ask any questions
about making your first Pull Request (PR) contribution to the Airflow codebase - it's a safe space
where it is expected that people asking questions do not know a lot Airflow (yet!).
If you need help with Airflow see the Slack channel #troubleshooting.

To check on how mentoring works for the projects under Apache Software Foundation's
`Apache Community Development - Mentoring <https://community.apache.org/mentoring/>`_.

Basic contributing tasks
------------------------

You can learn about various roles and communication channels in the Airflow project,

* `Roles in Airflow Project <01_roles_in_airflow_project.rst>`__ describes
  the roles in the Airflow project and how they relate to each other.

* `How to communicate <02_how_to_communicate.rst>`__
  describes how to communicate with the community and how to get help.

You can learn how to setup your environment for development and how to develop and test code:

* `Contributors quick start <03_contributors_quick_start.rst>`__ describes
  how to set up your development environment and make your first contribution. There are also more
  detailed documents describing how to set up your development environment for specific IDE/environment:

* `How to contribute <04_how_to_contribute.rst>`__ describes various ways how you can contribute to Airflow.

* `Pull requests <05_pull_requests.rst>`__ describes how you can create pull requests and you can learn
  there what are the pull request guidelines and coding standards.

* `Development environment <06_development_environments.rst>`__ describes the developments environment
  used in Airflow.

  * `Local virtualenv <07_local_virtualenv.rst>`__ describes the setup and details of the local virtualenv
    development environment.

  * `Breeze <../dev/breeze/doc/README.rst>`__ describes the setup and details of the Breeze development environment.

* `Static code checks <08_static_code_checks.rst>`__ describes the static code checks used in Airflow.

* `Testing <09_testing.rst>`__ describes what kind of tests we have and how to run them.

* `Building documentation <../docs/README.rst>`__ describes how to build the documentation locally.

* `Working with Git <10_working_with_git.rst>`__ describes the Git branches used in Airflow,
  how to sync your fork and how to rebase your PR.

Developing providers
--------------------

You can learn how Airflow repository is a monorepo split into airflow and provider packages,
and how to contribute to the providers:

* `Provider packages <11_provider_packages.rst>`__ describes the provider packages and how they
  are used in Airflow.


Deep dive into specific topics
------------------------------

Once you can also dive deeper into specific areas that are important for contributing to Airflow:

* `Airflow dependencies and extras <12_airflow_dependencies_and_extras.rst>`__ describes
  the dependencies - both required and optional (extras) used in Airflow.

* `Metadata database updates <13_metadata_database_updates.rst>`__ describes
  how to make changes in the metadata database.

* `Node environment setup <14_node_environment_setup.rst>`__ describes how to set up
  the node environment for Airflow UI.

* `Architecture diagram <15_architecture_diagrams.rst>`__ describes how to create and
  update the architecture diagrams embedded in Airflow documentation.

Finally there is an overview of the overall contribution workflow that you should follow

* `Contribution workflow <16_contribution_workflow.rst>`__ describes the workflow of contributing to Airflow.
