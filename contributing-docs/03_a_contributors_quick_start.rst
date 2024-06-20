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

*************************
Contributor's Quick Start
*************************

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Note to Starters
################

Airflow is a complex project, as is setting up an Airflow development environment to test changes.
This guide and its sub-guides are designed to make it as easy as possible to contribute to Airflow

There are three main ways you can run an Airflow development environment:

1. With `Breeze <../dev/breeze/doc/README.rst>`_ (local) Breeze...
  1. Makes it easy (you might say, "a *breeze*") to run integrations, tests, and more
  2. A Python-built tool specifically built to develop Airflow using Docker Compose
2. With a virtual environment management tool such as `hatch <https://hatch.pypa.io/latest/>`_ or `virtualenv <https://virtualenv.pypa.io/en/latest/>`_ (local)
3. With a remote environment management tool like `GitPod <https://www.gitpod.io/>`_ or `GitHub Codespaces <https://github.com/features/codespaces>`_ (remote)

Before deciding which method to choose, there are a few factors to consider:

* Breeze offers a lot of features, but it requires Docker Desktop, *at least* 2.5GB RAM, and 2 cores. 4GB of RAM is recommended. It also requires a substantial amount of disk space. 20GB is recommended.
* Experienced developers may prefer a batteries-not-included environment and might find installing into a virtual environment preferable.
  For a comprehensive venv tutorial, visit `Local virtualenv <07_local_virtualenv.rst>`_.
* Remote environments such as GitPod or GitHub Codespaces might require a paid account.

Forking and cloning Airflow
###########################

In order to submit a `pull request <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests>`_
(PR), you'll first need to `fork <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo>`_ and `clone <https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository>`_
the `Airflow git repository <https://github.com/apache/airflow>`_. This will more or less create a copy of the Airflow repository in
your own GitHub account and copy that repository to your local machine where you'll implement your changes to Airflow before submitting
a PR.

Using your IDE
--------------

If you are familiar with Python development using an integrated development environment (IDE), Airflow can be setup
similarly to other projects. If you need specific instructions to configure Airflow with your IDE, you
can find more information here:

* `Pycharm/IntelliJ <quick-start-ide/contributors_quick_start_pycharm.rst>`_
* `Visual Studio Code <quick-start-ide/contributors_quick_start_vscode.rst>`_


Static checks, pre-commit, and tests
#######################################################

The Airflow project has a robust test suite that includes both `static checks <08_static_code_checks.rst>`_ as well as
`unit tests, integration tests, and various system tests <09_testing.rst>`_. If you submit a pull request (PR) that
doesn't satisfy all static checks and tests, your PR will be automatically fail the github tests, and you'll be asked to update the
PR accordingly.

Static checks & pre-commit
--------------------------

Static checks help ensure that the Airflow project maintains high levels of code quality. Some static checks such as `black <https://black.readthedocs.io/en/stable/>`_
and `ruff <https://docs.astral.sh/ruff/>`_ ensure consistent style and formatting. Others, like `mypy <https://www.mypy-lang.org/>`_
help prevent bugs inherent to duck-typed languages like Python.

`pre-commit <https://pre-commit.com/>`_ is a tool used by Airflow to run static checks. pre-commit can be run ad-hoc
(``pre-commit run --all-files``) or enabled in your repository to run pre-commit hooks when you make a commit (that is, run
``git commit -m "some message"``). All pre-commit errors will be found before pushing to your repository/updating your PR, and some
(like black and ruff), will automatically reformat files on your behalf!

First, `install pre-commit <https://pre-commit.com/#install>`_. Then, if you'd like to configure pre-commit on commit, run
``pre-commit install`` from the root of your Airflow directory.

To disable precommit, simply run ``pre-commit uninstall`` from the root of your Airflow directory.

You can also run all pre-commit hooks against specific files:

.. code-block:: bash

   pre-commit run --files airflow/utils/decorators.py tests/utils/test_task_group.py


Specific pre-commit hooks against all files:

.. code-block:: bash

  pre-commit run black
    black...............................................................Passed

Or even specific pre-commit hooks against specific files:

.. code-block:: bash

   pre-commit run black --files airflow/decorators.py tests/utils/test_task_group.py
   black...............................................................Passed

Links to quick start sub-guides
###############################

Once you've `forked and cloned the Airflow repository <Forking and cloning Airflow>`_, you can follow one of the below sub-guides
to get your local development environment up and running.

* `Breeze quick start <03_b_contributors_quick_start_breeze.rst>`_
* `hatch quick start <03_b_contributors_quick_start_hatch.rst>`_
* `GitPod quick start <quick-start-ide/contributors_quick_start_gitpod.rst>`_
* `GitHub Codespaces quick start <quick-start-ide/contributors_quick_start_codespaces.rst>`_

Non-code Contributions
######################

There are plenty of other ways to contribute to the Airflow project without writing a single line of code.
Here are a couple examples of how you can get started without writing code:

* Engage with the `community <https://airflow.apache.org/community/>`_
   * `Report a bug or request a feature <https://github.com/apache/airflow/issues/new/choose>`_
   * Ask questions or help others in the Airflow Slack
   * Get involved in the Airflow dev list (aka mailing list)
   * Host an Airflow meetup
* Submit a PR to improve Airflow's documentation
   * These are super valuable!
* Propose a fundamental change to Apache Airflow via an `Airflow Improvement Proposal (AIP) <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals>`_
