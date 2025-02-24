
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

Local Virtual Environment (virtualenv)
======================================

The easiest way to run tests for Airflow is to use local virtualenv. While Breeze is the recommended
way to run tests - because it provides a reproducible environment and is easy to set up, it is not
always the best option as you need to run your tests inside a docker container. This might make it
harder to debug the tests and to use your IDE to run them.

That's why we recommend using local virtualenv for development and testing.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Required Software Packages
--------------------------

Use system-level package managers like yum, apt-get for Linux, or
Homebrew for macOS to install required software packages:

* Python (One of: 3.9, 3.10, 3.11, 3.12)
* MySQL 5.7+
* libxml
* helm (only for helm chart tests)

There are also sometimes other system level packages needed to install python packages - especially
those that are coming from providers. For example you might need to install ``pkgconf`` to be able to
install ``mysqlclient`` package for ``mysql`` provider . Or you might need to install ``graphviz`` to be able to install
``devel`` extra bundle.

Please refer to the `Dockerfile.ci <../Dockerfile.ci>`__ for a comprehensive list of required packages.

.. note::

   - MySql 2.2.0 needs pkgconf to be a pre requisite, refer `here <http://pkgconf.org/>`_ to install pkgconf
   - MacOs with ARM architectures require graphviz for venv setup, refer `here <https://graphviz.org/download/>`_ to install graphviz
   - The helm chart tests need helm to be installed as a pre requisite. Refer `here <https://helm.sh/docs/intro/install/>`_ to install and setup helm

.. note::

   As of version 2.8 Airflow follows PEP 517/518 and uses ``pyproject.toml`` file to define build dependencies
   and build process and it requires relatively modern versions of packaging tools to get airflow built from
   local sources or ``sdist`` packages, as PEP 517 compliant build hooks are used to determine dynamic build
   dependencies. In case of ``pip`` it means that at least version 22.1.0 is needed (released at the beginning of
   2022) to build or install Airflow from sources. This does not affect the ability of installing Airflow from
   released wheel packages.


Creating and maintaining local virtualenv with uv
-------------------------------------------------

As of November 2024 we are recommending to use ``uv`` for local virtualenv management for Airflow development.
The ``uv`` utility is a build frontend tool that is designed to manage python, virtualenvs and workspaces for development
and testing of Python projects. It is a modern tool that is designed to work with PEP 517/518 compliant projects
and it is much faster than "reference" ``pip`` tool. It has extensive support to not only create development
environment but also to manage python versions, development environments, workspaces and Python tools used
to develop Airflow (via ``uv tool`` command - such as ``pre-commit`` and others, you can also use ``uv tool``
to install ``breeze`` - containerized development environment for Airflow that we use to reproduce the
CI environment locally and to run release-management and certain development tasks.

You can read more about ``uv`` in `UV Getting started <https://docs.astral.sh/uv/getting-started/>`_ but
below you will find a few typical steps to get you started with ``uv``.

Installing uv
.............

You can follow the `installation instructions <https://docs.astral.sh/uv/getting-started/installation/>`_ to install
``uv`` on your system. Once you have ``uv`` installed, you can do all the environment preparation tasks using
``uv`` commands.

.. note::

  Mac OS has a low ``ulimit`` setting (256) for number of opened file descriptors which does not work well with our
  workspace when installing it and you can hit ``Too many open files`` error. You should run the
  ``ulimit -n 2048`` command to increase the limit of file descriptors to 2048 (for example). It's best to add
  the ``ulimit`` command to your shell profile (``~/.bashrc``, ``~/.zshrc`` or similar) to make sure it's set
  for all your terminal sessions automatically. Other than small increase in resource usage it has no negative
  impact on your system.

Installing Python versions
..........................

You can install Python versions using ``uv python install`` command. For example, to install Python 3.9.7, you can run:

.. code:: bash

    uv python install 3.9.7

This is optional step - ``uv`` will automatically install the Python version you need when you create a virtualenv.

Creating virtualenvs with uv
............................

.. code:: bash

    uv venv

This will create a default venv in your project's ``.venv`` directory. You can also create a venv
with a specific Python version by running:

.. code:: bash

    uv venv --python 3.9.7

You can also create a venv with a different venv directory name by running:

.. code:: bash

    uv venv .my-venv

However ``uv`` creation/re-creation of venvs is so fast that you can easily create and delete venvs as needed.
So usually you do not need to have more than one venv and recreate it as needed - for example when you
need to change the python version.

Syncing project (including providers) with uv
.............................................

In a project like airflow it's important to have a consistent set of dependencies across all developers.
You can use ``uv sync`` to install dependencies from ``pyproject.toml`` file. This will install all dependencies
from the ``pyproject.toml`` file in the current directory.

.. code:: bash

    uv sync

If you also need to install development and provider dependencies you can specify extras for that providers:

.. code:: bash

    uv sync --extra devel --extra devel-tests --extra google

This will synchronize all extras that you need for development and testing of Airflow and google provider
dependencies - including their runtime dependencies.

.. code:: bash

    uv sync --all-extras

This will synchronize all extras of airflow (this might require some system dependencies to be installed).


Creating and installing airflow with other build-frontends
----------------------------------------------------------

While ``uv`` uses ``workspace`` feature to synchronize both Airflow and Providers in a single sync
command, you can still use other frontend tools (such as ``pip``) to install Airflow and Providers
and to develop them without relying on ``sync`` and ``workspace`` features of ``uv``. Below chapters
describe how to do it with ``pip``.

Installing Airflow with pip
...........................

Since Airflow follows the standards define by the packaging community, we are not bound with
``uv`` as the only tool to manage virtualenvs - and you can use any other compliant frontends to install
airflow for development. The standard way of installing environment with dependencies necessary to
run tests is to use ``pip`` to install airflow dependencies:

.. code:: bash

    pip install -e ".[devel,devel-tests,<OTHER EXTRAS>]" # for example: pip install -e ".[devel,devel-tests,google,postgres]"

This will install airflow in ``editable`` mode - where sources of
Airflow are taken directly from ``airflow`` source code.

You need to run this command in the virtualenv you want to install Airflow in and you need to have the virtualenv activated.

   .. code:: bash

       pip install -e ".[devel,devel-tests,<OTHER EXTRAS>]" # for example: pip install -e ".[devel,devel-tests,google,postgres]"
       pip install -e "./providers/airbyte[devel]"

   This will install:

       * airflow in ``editable`` mode - where sources of Airflow are taken directly from ``airflow`` source code.
       * airbyte provider in ``editable`` mode - where sources are read from ``providers/airbyte`` folder

Extras (optional dependencies)
..............................

You can also install extra packages (like ``[ssh]``, etc) via
``pip install -e [devel,EXTRA1,EXTRA2 ...]``. However, some of them may
have additional install and setup requirements for your local system.

For example, if you have a trouble installing the mysql client on macOS and get
an error as follows:

.. code:: text

    ld: library not found for -lssl

you should set LIBRARY\_PATH before running ``pip install``:

.. code:: bash

    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/

You are STRONGLY encouraged to also install and use `pre-commit hooks <08_static_code_checks.rst#pre-commit-hooks>`_
for your local virtualenv development environment. Pre-commit hooks can speed up your
development cycle a lot.

The full list of extras is available in `pyproject.toml <../pyproject.toml>`_ and can be easily retrieved using hatch via

.. note::

   Only ``pip`` installation is currently officially supported.
   Make sure you have the latest pip installed, reference `version <https://pip.pypa.io/en/stable/#>`_

   While there are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
   `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
   ``pip`` - especially when it comes to constraint vs. requirements management.
   Installing via ``Poetry`` or ``pip-tools`` is not currently supported.

   There are known issues with ``bazel`` that might lead to circular dependencies when using it to install
   Airflow. Please switch to ``pip`` if you encounter such problems. ``Bazel`` community works on fixing
   the problem in `this PR <https://github.com/bazelbuild/rules_python/pull/1166>`_ so it might be that
   newer versions of ``bazel`` will handle it.

   If you wish to install airflow using those tools you should use the constraint files and convert
   them to appropriate format and workflow that your tool requires.

Developing community providers in local virtualenv
..................................................

While the above installation is good enough to work on Airflow code, in order to develop
providers, you also need to install them in the virtualenv you work on (after installing
the extras in airflow, that correspond to the provider you want to develop). This is something
you need to do manually if not using ``uv sync`` to synchronize the whole Airflow workspace.

This is a bit repeated information from earlier chapters but it shows quickly how you set it
up for a single provider. See above for more details.

If you want to develop google provider, for example here is what you need to do:

If you use ``uv`` this is very simple:

``uv sync --extra google``

If you use ``pip`` it is quite a bit mre you can run the following command in the
venv that you have installed airflow in (also in editable mode):

.. code:: bash

    pip install -e ".[devel,devel-tests,google]"
    pip install -e "./task_sdk"
    pip install -e "./providers/google"

The first command installs airflow, it's development dependencies, test dependencies and
both runtime and development dependencies of the google provider (Note that in the future, when
dependency groups will be implemented in ``pip`` - April 2025) - it will not be needed to use ``google`` extra
when installing airflow - currently with ``pip`` it is the only way to install development dependencies
of the provider and is a bit convoluted.

The second installs ``task_sdk`` project - where APIs for providers are kept.

The third one installs google provider source code in development mode, so that modifications
to the code are automatically reflected in your installed virtualenv.

You need to separately install each provider you want to develop in the same virtualenv where you
have installed Airflow.

Developing Providers
--------------------

In Airflow 2.0 we introduced split of Apache Airflow into separate distributions - there is one main
apache-airflow package with core of Airflow and 90+ distributions for all providers (external services
and software Airflow can communicate with).

In Airflow 3.0 we moved each provider to a separate sub-folder in "providers" directory - and each of those
providers is a separate distribution with its own ``pyproject.toml`` file. The ``uv workspace`` feature allows
to install all the distributions together and work together on all of them but you also can do it manually
with ``pip``.

When you install airflow from sources using editable install you only install airflow now, but as described
in the previous chapter, you can develop together both - main version of Airflow and providers of your choice,
which is pretty convenient, because you can use the same environment for both.

Running ``pip install -e .`` will install Airflow in editable mode, but all provider code is elsewhere (
in ``providers/PROVIDER`` folder, Also most provider need some additional dependencies.

You can install the dependencies of the provider you want to develop by installing the provider distribution
in editable mode.

The dependencies for providers are configured in ``providers/PROVIDER/pyproject.toml`` files -
separately for each provider. You can find there two types of ``dependencies`` - production runtime
dependencies, and sometimes ``development dependencies`` (in ``dev`` dependency group) which are needed
to run tests and are installed automatically when you install environment with ``uv-sync``.

If you want to add another dependency to a provider, you should add it to corresponding ``pyproject.toml``,
add the files to your commit with ``git add`` and run ``pre-commit run`` to update generated dependencies.
Note that in the future we will remove that step.

For ``uv`` it's simple, you need to run ``uv sync`` in main airflow directory after you modified
``pyproject.toml`` file in the provider.

For ``pip`` you should run ``pip install -e .[devel,PROVIDER_EXTRA]`` will install the new dependencies -
including devel dependencies of the provider..


Installing "golden" version of dependencies
-------------------------------------------

Whatever virtualenv solution you use, when you want to make sure you are using the same
version of dependencies as in main, you can install recommended version of the dependencies by using pip:
constraint-python<PYTHON_MAJOR_MINOR_VERSION>.txt files as ``constraint`` file. This might be useful
to avoid "works-for-me" syndrome, where you use different version of dependencies than the ones
that are used in main, CI tests and by other contributors.

There are different constraint files for different python versions. For example this command will install
all basic devel requirements and requirements of google provider as last successfully tested for Python 3.9:

.. code:: bash

    pip install -e ".[devel,google]" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-source-providers-3.9.txt"

Or with ``uv``:

.. code:: bash

    uv pip install -e ".[devel,google]" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-source-providers-3.9.txt"

In the future we will utilise ``uv.lock`` to manage dependencies and constraints, but for the moment we do not
commit ``uv.lock`` file to airflow repository because we need to figure out automation of updating the ``uv.lock``
very frequently (few times a day sometimes). With Airflow's 700+ dependencies it's all but guaranteed that we
will have 3-4 changes a day and currently automated constraints generation mechanism in ``canary`` build keeps
constraints updated, but for ASF policy reasons we cannot update ``uv.lock`` in the same way - but work is in
progress to fix it.

Make sure to use latest main for such installation, those constraints are "development constraints" and they
are refreshed several times a day to make sure they are up to date with the latest changes in the main branch.

Note that this might not always work as expected, because the constraints are not always updated
immediately after the dependencies are updated, sometimes there is a very recent change (few hours, rarely more
than a day) which still runs in ``canary`` build and constraints will not be updated until the canary build
succeeds. Usually what works in this case is running your install command without constraints.

You can upgrade just airflow, without paying attention to provider's dependencies by using
the 'constraints-no-providers' constraint files. This allows you to keep installed provider dependencies
and install to latest supported ones by pure airflow core.

.. code:: bash

    pip install -e ".[devel]" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-no-providers-3.9.txt"

These are examples of the development options available with the local virtualenv in your IDE:

* local debugging;
* Airflow source view;
* auto-completion;
* documentation support;
* unit tests.

This document describes minimum requirements and instructions for using a standalone version of the local virtualenv.

Running Tests
-------------

Running tests is described in `Testing documentation <09_testing.rst>`_.

While most of the tests are typical unit tests that do not require external components, there are a number
of Integration tests. You can technically use local virtualenv to run those tests, but it requires to
set up all necessary dependencies for all the providers you are going to tests and also setup
databases - and sometimes other external components (for integration test).

So, generally it should be easier to use the `Breeze <../dev/breeze/doc/README.rst>`__ development environment
(especially for Integration tests).


Connecting to database
----------------------

When analyzing the situation, it is helpful to be able to directly query the database. You can do it using
the built-in Airflow command (however you needs a CLI client tool for each database to be installed):

.. code:: bash

    airflow db shell

The command will explain what CLI tool is needed for the database you have configured.


-----------

As the next step, it is important to learn about `Static code checks <08_static_code_checks.rst>`__.that are
used to automate code quality checks. Your code must pass the static code checks to get merged.
