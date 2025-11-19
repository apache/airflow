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

Airflow dependencies
====================

This document describes how we manage Apache Airflow dependencies, as well as how we make sure
that users can use Airflow both as an application and as a library when they are deploying
their own Airflow - using our constraints mechanism.

Airflow ``pyproject.toml`` files and ``uv`` workspace
.....................................................

Managing dependencies is an important part of developing Apache Airflow, we have more than 700
dependencies, and often when you add a new feature that requires a new dependency, you should
also update the dependency list. This also happens when you want to add a new tool that is used
for development, testing, or building the documentation and this document describes how to do it.

When it comes to defining dependencies for any of the Apache Airflow distributions, we are following the
standard ``pyproject.toml`` format that has been defined in `PEP 518 <https://peps.python.org/pep-0518/>`_
and `PEP 621 <https://peps.python.org/pep-0621/>`_, we are also using dependency groups defined  in
`PEP 735 <https://peps.python.org/pep-0735/>`_  - particularly ``dev`` dependency group which we use in
all our ``pyproject.toml`` files to define development dependencies.

We have big number (currently more than 100) python distributions in Apache Airflow repository - including the main
``apache-airflow`` package, ``apache-airflow-providers-*`` packages, ``apache-airflow-core`` package,
``apache-airflow-task-sdk`` package, ``apache-airflow-ctl`` package, and several other packages.

They are all connected together via ``uv`` workspace feature (workspace is defined in the root ``pyproject.toml``
file of the repository in the ``apache-airflow`` distribution definition. The workspace feature allows us to
run ``uv sync`` at the top of the repository to install all packages in editable mode in the development
environment from all the distributions and resolve the dependencies together, so that we know that
the dependencies have no conflicting requirements. Also the distributions are referring to each other via
name - which means that when you run locally ``uv sync``, the local version of the packages are used, not the
ones released on PyPI, which means that you can develop and test changes that span multiple packages at the
same time. This is a very powerful feature that allows us to maintain the complex ecosystem of Apache Airflow
distributions in a single monorepo, and allows us - for example to add new feature to common distributions used
by multiple providers and test them all together before releasing new versions of either of those pacckages

Managing dependencies in ``pyproject.toml`` files
.................................................

Each of the ``pyproject.toml`` files in Apache Airflow repository defines dependencies in one of the
following sections:

* ``[project.dependencies]`` - this section defines the required dependencies of the package. These
  dependencies are installed when you install the package without any extras.
* ``[project.optional-dependencies]`` - this section defines optional dependencies (extras) of the package.
  These dependencies are installed when you install the package with extras - for example
  ``pip install apache-airflow[ssh]`` will install the ``ssh`` extra dependencies defined in this section.
* ``[dependency-group.dev]`` - this section defines development dependencies of the package.
  These dependencies are installed when you run ``uv sync`` by default. when ``uv`` syncs sources with
  local pyproject.toml it adds ``dev`` dependency group and package is installed in editable mode with
  development dependencies.


Adding and modifying dependencies
.................................

Adding and modifying dependencies in Apache Airflow is done by modifying the appropriate
``pyproject.toml`` file in the appropriate distribution.

When you add a new dependency, you should make sure that:

* The dependency is added to the right section (main dependencies, optional dependencies, or
  development dependencies)

* Some parts of those dependencies might be automatically generated (and overwritten) by our ``prek``
  hooks. Those are the necessary dependencies that ``prek`` hoks can figure out automatically by
  analyzing the imports in the sources and structure of the project. We also have special case of
  shared dependencies (described in `shared dependencies document <../shared/README.md>`__) where we
  effectively "static-link" some libraries into multiple distributions, to avoid unnecessary coupling and
  circular dependencies, and those dependencies contribute some dependencies automatically as well.
  Pay attention to comments such us example start and end of such generated block below.
  All the dependencies between those comments are automatically generated and you should not modify
  them manually. You might instead modify the root source of those dependencies - depending on the automation,
  you can usually also add extra dependencies manually outside of those comment blocks

  .. code-block:: python

    # Automatically generated airflow optional dependencies (update_airflow_pyproject_toml.py)
    # ....
    # LOTS OF GENERATED DEPENDENCIES HERE
    # ....
    # End of automatically generated airflow optional dependencies

* The version specifier is as open as possible (upper) while still allowing the package to install
  and pass all tests. We very rarely upper-bind dependencies - only when there is a known
  conflict with a new or upcoming version of a dependency that breaks the installation or tests
  (and we always make a comment why we are upper-bounding a dependency).

* Make sure to lower-bind any dependency you add. Usually we lower-bind dependencies to the
  minimum version that is required for the package to work but in order to simplify the work of
  resolvers such as ``pip``, we often lower-bind to higher (and newer) version than the absolute minimum
  especially when the minimum version is very old. This is for example good practice in ``boto`` and related
  packages, where new version of those packages are released frequently (almost daily) and there are many
  versions that need to be considered by the resolver if the version is not new enough.

* Make sure to run ``uv sync`` after modifying dependencies to make sure that there are no
  conflicts between dependencies of different packages in the workspace. You can run it in multiple
  ways - either from the root of the repository (which will sync all packages) or from the package
  you modified (which will sync only that package and its dependencies). Also good idea might be to
  run ``uv sync --all-packages --all-extras`` at the root of the repository to make sure that
  all packages with all extras can be installed together without conflicts, but this might be sometimes
  difficult and slow as some of the extras require some additional system level dependencies to be installed
  (for example ``mysql`` or ``postgres`` extras require client libraries to be installed on the system).

* Make sure to run all tests after modifying dependencies to make sure that nothing is broken


Referring to other Apache Airflow distributions in dependencies
...............................................................

With having more than distributions in the repository, it is often necessary to refer to
other distributions in the dependencies in order to use some common features or simply to use the
features that the other distribution provides. There are two ways of doing it:

* Regular package linking with ``apache-airflow-*`` dependency
* Airflow "shared dependencies" mechanism - which is a bit of a custom hack for Airflow monorepo
  that allows us to "static-link" some common dependencies into multiple distributions without
  creating circular dependencies.

We are not going to describe the shared dependencies mechanism here, please refer to the
`shared dependencies document <../shared/README.md>`__ for details, but there are certain rules
when it comes to referring to other Airflow distributions in dependencies - here are the important
rules to remember:

* You can refer to other distributions in your dependencies - as usual using distribution name. For example,
  if you are adding a dependency to ``apache-airflow-providers-common-compat`` package from
  ``apache-airflow-providers-google``, you can just add ``apache-airflow-providers-common>=x.y.z`` to the
  dependencies and when you run ``uv sync``, the local version of the package will be used automatically
  (this is thanks to the workspace feature of ``uv`` that does great job of binding our monorepo together).
  Some of those are added automatically by prek hooks - when it can detect such dependencies by analyzing
  imports in the sources - then they are added automatically between the special comments mentioned above,
  but sometimes (especially when such dependencies are not at the top-level imports) you might need to
  add them manually.

* In case you make a feature change in a distribution and would like to update its version, you should
  never update the distribution version on your own. It is **entirely** up to the Release Manager
  to bump the version of distributions that are defined as ``project.version``. This goes almost without an
  exception and any diversions from this rule should be discussed at ``#release-management`` channel in
  Airflow Slack beforehand. The only exception to this rule is when you are adding a new distribution to
  the repository - in that case you should set the initial version of the distribution  - usually ``0.0.1``,
  ``0.1.0`` or ``1.0.0`` depending on the maturity of the package. But still you should discuss
  it in the channel.

* Sometimes, when you add a new feature to a common distribution, you might add a feature to it or
  change the API in the way that other packages can use it. This is especially true for common packages such as
  ``apache-airflow-providers-common-compat``, but can happen for other packages (for example
  ``apache-airflow-providers-apache-beam`` is used by ``apache-airflow-providers-google`` to use ``Apache Beam``
  hooks to communicate with Google Dataflow). In such case, when you are adding a feature to a common package
  remember that the feature you just add will only be released in the **FUTURE** release of such common
  package and you cannot add ``>==x.y.z`` dependency to it where ``x.y.z`` is the version you are
  going to release in the future. Ultimately, this should happen (and happens) when the Release Manager prepares
  both packages together. Let us repeat - such changes in versions between different airflow package should
  **NOT** be added to the dependencies manually by the contributor. They should **exclusively** be added by
  the Release Manager. when preparing the release of **both** packages together.
  We have a custom mechanism to support such additions, where it is contributor's responsibility to mark
  dependency with a special comment - simply communicating with the Release Manager that such dependency
  should be updated to the next version when the release is prepared. If you see such a need to use newly
  added feature and using it at the same time in a different distribution -  make sure to add this comment
  in the line where dependency you want to use the new feature from is defined:

  .. warning::
    You must use the exact comment ``# use next version`` - otherwise the automation will not pick it up.

  .. code-block:: python

      # ...
      "apache-airflow-SOMETHING>1.0.0",  # use next version
      # ...

  We have tooling in place to:

  - check that no regular PRs modify such cross-dependency versions
  - make sure that such dependencies marked with the comment are updated automatically to the next version,
    when preparing the release

  Example of such dependency (for example placed in ``apache-airflow-providers-google/pyproject.toml``) when
  you want to use a new feature you are adding in the same PR to ``apache-airflow-providers-common-compat``
  package:

  .. code-block:: python

     "apache-airflow-providers-google>=1.2.0",
     "apache-airflow-providers-common-compat>=5.5.0",  # use next version
     "requests>=2.25.1",

  Note that you **SHOULD NOT** change version of the common-compat package in this PR. When the Release Manager
  will prepare the release of both packages, the version will be updated automatically to the next version
  that is being released (for example ``5.6.0``) and the Release Manager will make sure that both packages
  are released together.

* Some cross dependencies like that are added automatically by prek hooks as well - when it can
  detect such dependencies by analyzing imports in the sources - then they are added automatically between
  the special comments mentioned above. In such case the dependency line will be added in the section that
  is commented around with ``# Start automated ...``, ``# End automated ...`` comments as described above. In this case,
  just copy the dependency line to outside of those comments and add the
  ``# use next version`` comment to it, next time when ``prek hook`` will be run it will remove the automatically
  added line and keep only the manually added line with the comment.

* Some of our dependencies have forced minimum version - mostly because of the Airflow 3 minimum version
  compatibility. Just in case in the future, we have other distributions referring to them we are forcing a
  minimum version for those distributions by a ``prek`` hook. This causes entries like this:

  .. code-block:: python

     "apache-airflow-providers-google-something>=1.2.0",
     "apache-airflow-providers-fab>=2.2.0",  # Set from MIN_VERSION_OVERRIDE in update_airflow_pyproject_toml.py
     "requests>=2.25.1",

  This will not happen, when the distribution will depend on even **newer** version of the dependency,
  but this is mainly a precaution in cases where we **know** we need a minimum version for Airflow 3
  compatibility (for example for ``git``, ``common.messaging`` and few other providers) or where we
  know that we should not use older version of providers in the future because some functionality in them
  stopped working (like in case of ``amazon``, ``fab``). You are free to modify those versions to higher
  versions if you need to, and ``prek`` will remove those comments automatically.

Our CI system will do all the tests for you anyway - including running some lower-bind checks on dependencies.
For example it will take each provider in a turn and will try to resolve lowest-possible dependencies defined
for that provider and see if the tests are still passing, so we should be relatively protected against putting
too low lower-bounds on dependencies, but running ``uv sync`` locally is still a good idea to find such things
before they hit the CI system.


Airflow as both library and application - constraint files
----------------------------------------------------------

Why constraints?
................

Airflow is not a standard python project. Most of the python projects fall into one of two types -
application or library. As described in
`this StackOverflow question <https://stackoverflow.com/questions/28509481/should-i-pin-my-python-dependencies-versions>`_,
the decision whether to pin (freeze) dependency versions for a python project depends on the type. For
applications, dependencies should be pinned, but for libraries, they should be open.

For applications, pinning the dependencies makes it more stable to install in the future - because new
(even transitive) dependencies might cause installation to fail. For libraries - the dependencies should
be open to allow several different libraries with the same requirements to be installed at the same time.

The problem is that Apache Airflow is a bit of both - application to install and library to be used when
you are developing your own operators and Dags.

This - seemingly unsolvable - puzzle is solved by having pinned constraints files.

Why not using a standard approach?
..................................

Because the standards defined in Python environment have not yet caught up with the needs of complex
projects that are both libraries and applications.

What we do is more of a hack to overcome the limitations of existing tools, when it comes to
reproducible installations of such projects.

Discussion about standards advanced over the last few years with `PEP 751 <https://peps.python.org/pep-0751/>`_
that introduces ``A file format to record Python dependencies for installation reproducibility``,
but support for this format (``pylock.toml``) is not yet widespread. As of November 2025
it is experimental in ``pip`` and supported as export format for ``uv`` from their own lock (development
environment focused) file. But the ability to use the format for reproducible installation as intended
in the ``PEP 751`` is not yet supported, not even ``PEP 751`` is complete enough to allow that.

The discussion is on-going on further advancing this mechanism with follow-up PEP, to support such
reproducible installation process that Airflow introduced years ago with the constraints hack - see
`Pre-PEP <https://discuss.python.org/t/pre-pep-add-ability-to-install-a-package-with-reproducible-dependencies/99497/14>_`_
where discussion about doing something like we do is on-going.

Pinned constraints files
........................

.. note::

   Only ``pip`` and ``uv`` installation is officially supported.

   While it is possible to install Airflow with tools like `poetry <https://python-poetry.org/>`_ or
   `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
   ``pip`` - especially when it comes to constraint vs. requirements management.
   Installing via ``Poetry`` or ``pip-tools`` is not currently supported. Uv follows ``pip`` approach
   with ``uv pip`` so it should work similarly.

   There are known issues with ``bazel`` that might lead to circular dependencies when using it to install
   Airflow. Please switch to ``pip`` if you encounter such problems. The ``Bazel`` community added support
   for cycles in `this PR <https://github.com/bazelbuild/rules_python/pull/1166>`_ so it might be that
   newer versions of ``bazel`` will handle it.

   If you wish to install Airflow using these tools you should use the constraint files and convert
   them to appropriate format and workflow that your tool requires.


By default when you install ``apache-airflow`` package - the dependencies are as open as possible while
still allowing the ``apache-airflow`` package to install. This means that the ``apache-airflow`` package
might fail to install when a direct or transitive dependency is released that breaks the installation.
In that case, when installing ``apache-airflow``, you might need to provide additional constraints (for
example ``pip install apache-airflow==1.10.2 Werkzeug<1.0.0``)

There are several sets of constraints we keep:

* 'constraints' - these are constraints generated by matching the current Airflow version from sources
   and providers that are installed from PyPI. Those are constraints used by the users who want to
   install Airflow with pip, they are named ``constraints-<PYTHON_MAJOR_MINOR_VERSION>.txt``.

* "constraints-source-providers" - these are constraints generated by using providers installed from
  current sources. While adding new providers their dependencies might change, so this set of providers
  is the current set of the constraints for Airflow and providers from the current main sources.
  Those providers are used by CI system to keep "stable" set of constraints. They are named
  ``constraints-source-providers-<PYTHON_MAJOR_MINOR_VERSION>.txt``

* "constraints-no-providers" - these are constraints generated from only Apache Airflow, without any
  providers. If you want to manage Airflow separately and then add providers individually, you can
  use them. Those constraints are named ``constraints-no-providers-<PYTHON_MAJOR_MINOR_VERSION>.txt``.

The first two can be used as constraints file when installing Apache Airflow in a repeatable way.
It can be done from the sources:

from the PyPI package:

.. code-block:: bash

  pip install "apache-airflow[google,amazon,async]==3.0.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.10.txt"

The last one can be used to install Airflow in "minimal" mode - i.e when bare Airflow is installed without
extras.

When you install Airflow from sources (in editable mode) you should use "constraints-source-providers"
instead (this accounts for the case when some providers have not yet been released and have conflicting
requirements).

.. code-block:: bash

  pip install -e ".[devel]" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-source-providers-3.10.txt"


This also works with extras - for example:

.. code-block:: bash

  pip install ".[ssh]" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-source-providers-3.10.txt"


There are different set of fixed constraint files for different python major/minor versions and you should
use the right file for the right python version.

If you want to update just the Airflow dependencies, without paying attention to providers, you can do it
using ``constraints-no-providers`` constraint files as well.

.. code-block:: bash

  pip install . --upgrade \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-no-providers-3.10.txt"


The ``constraints-<PYTHON_MAJOR_MINOR_VERSION>.txt`` and ``constraints-no-providers-<PYTHON_MAJOR_MINOR_VERSION>.txt``
will be automatically regenerated by CI job every time after the ``pyproject.toml`` is updated and pushed
if the tests are successful.


.. note::

   Only ``pip`` and ``uv`` installation is currently officially supported.

   While there are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
   `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
   ``pip`` - especially when it comes to constraint vs. requirements management.
   Installing via ``Poetry`` or ``pip-tools`` is not currently supported. Uv follows ``pip`` approach
   with ``uv pip`` so it should work similarly.

   There are known issues with ``bazel`` that might lead to circular dependencies when using it to install
   Airflow. Please switch to ``pip`` if you encounter such problems. ``Bazel`` community works on fixing
   the problem in `this PR <https://github.com/bazelbuild/rules_python/pull/1166>`_ so it might be that
   newer versions of ``bazel`` will handle it.

   If you wish to install Airflow using these tools you should use the constraint files and convert
   them to appropriate format and workflow that your tool requires.

Optional dependencies (extras) of ``apache-airflow`` package
............................................................

There are a number of extras that can be specified when installing Airflow. Those
extras can be specified after the usual pip install - for example ``pip install -e.[ssh]`` for editable
installation. Note that there are two kinds of extras - ``regular`` extras (used when you install
airflow as a user, but in ``editable`` mode you can also install ``devel`` extras that are necessary if
you want to run Airflow locally for testing and ``doc`` extras that install tools needed to build
the documentation.

Note that some of those extras are only added in the meta-distribution ``apache-airflow``, they are not defined
in the ``airflow-core`` and any other packages. The dependencies to providers and copy of such extras from
the ``apache-airflow`` package is done automatically by our ``prek`` hooks, similarly as creating extras for
providers from the ``apache-airflow-providers-*`` packages in the workspace.

There are also some manually defined extras for optional features that are often used with Airflow.

You can read more about those extras in the
`extras reference <https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html>`_.


**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**


-----

You can now check how to update Airflow's `metadata database <14_metadata_database_updates.rst>`__ if you need
to update structure of the DB.
