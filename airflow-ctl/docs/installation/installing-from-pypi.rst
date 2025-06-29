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

Installation from PyPI
----------------------

This page describes installations using the ``apache-airflow-ctl`` package `published in
PyPI <https://pypi.org/project/apache-airflow-ctl/>`__.

Installation tools
''''''''''''''''''

Only ``pip`` installation is currently officially supported.

.. note::

  While there are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
  `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
  ``pip`` - especially when it comes to constraint vs. requirements management.
  Installing via ``Poetry`` or ``pip-tools`` is not currently supported. If you wish to install airflow
  using those tools you should use the constraints and convert them to appropriate
  format and workflow that your tool requires.

  There are known issues with ``bazel`` that might lead to circular dependencies when using it to install
  Airflow. Please switch to ``pip`` if you encounter such problems. ``Bazel`` community works on fixing
  the problem in `this PR <https://github.com/bazelbuild/rules_python/pull/1166>`_ so it might be that
  newer versions of ``bazel`` will handle it.

Typical command to install airflowctl from scratch in a reproducible way from PyPI looks like below:

.. code-block:: bash

    pip install "apache-airflow-ctl==|version|"

Those are just examples, see further for more explanation why those are the best practices.

.. note::

   Generally speaking, Python community established practice is to perform application installation in a
   virtualenv created with ``virtualenv`` or ``venv`` tools. You can also use ``uv`` or ``pipx`` to install
   Airflow in application dedicated virtual environment created for you. There are also other tools that can be used
   to manage your virtualenv installation and you are free to choose how you are managing the environments.
   Airflow has no limitation regarding to the tool of your choice when it comes to virtual environment.

   The only exception where you might consider not using virtualenv is when you are building a container
   image with only Airflow installed - this is for example how Airflow is installed in the official Container
   image.

.. _installation:constraints:

Constraints files
'''''''''''''''''

Why we need constraints
=======================

Airflow CTL installation can be tricky because Airflow CTL is both a library and an application.

Libraries usually keep their dependencies open and applications usually pin them, but we should do neither
and both at the same time. We decided to keep our dependencies as open as possible
(in ``pyproject.toml``) so users can install different version of libraries if needed. This means that
from time to time plain ``pip install apache-airflow-ctl`` will not work or will produce an unusable
Airflow CTL installation.

Reproducible Airflow CTL installation
=====================================

In order to have a reproducible installation, we also keep a set of constraint files in the
``constraints-main``, ``constraints-2-0``, ``constraints-2-1`` etc. orphan branches and then we create a tag
for each released version e.g. :subst-code:`constraints-|version|`.

This way, we keep a tested set of dependencies at the moment of release. This provides you with the ability
of having the exact same installation of airflowctl + dependencies as was known to be working
at the moment of release - frozen set of dependencies for that version of Airflow CTL. There is a separate
constraints file for each version of Python that Airflow CTL supports.

You can create the URL to the file substituting the variables in the template below.

.. code-block::

  https://raw.githubusercontent.com/apache/airflow/airflow-ctl/constraints-${AIRFLOWCTL_VERSION}/constraints-${PYTHON_VERSION}.txt

where:

- ``AIRFLOW_CTL_VERSION`` - Airflow CTL version (e.g. :subst-code:`|version|`) or ``main``, ``2-0``, for latest development version
- ``PYTHON_VERSION`` Python version e.g. ``3.10``, ``3.11``


Verifying installed dependencies
================================

You can also always run the ``pip check`` command to test if the set of your Python packages is
consistent and not conflicting.


.. code-block:: bash

    > pip check
    No broken requirements found.


When you see such message and the exit code from ``pip check`` is 0, you can be sure, that there are no
conflicting dependencies in your environment.


Using your own constraints
==========================

When you decide to install your own dependencies, or want to upgrade or downgrade providers, you might want
to continue being able to keep reproducible installation of Airflow CTL and those dependencies via a single command.
In order to do that, you can produce your own constraints file and use it to install Airflow CTL instead of the
one provided by the community.

.. code-block:: bash

    pip install "apache-airflow-ctl==|version|"
    pip freeze > my-constraints.txt


Then you can use it to create reproducible installations of your environment in a single operation via
a local constraints file:

.. code-block:: bash

    pip install "apache-airflow-ctl==|version|" --constraint "my-constraints.txt"


Similarly as in case of Airflow CTL original constraints, you can also host your constraints at your own
repository or server and use it remotely from there.

Fixing Constraints at release time
''''''''''''''''''''''''''''''''''

The released "versioned" constraints are mostly ``fixed`` when we release Airflow CTL version and we only
update them in exceptional circumstances. For example when we find out that the released constraints might prevent
Airflow CTL from being installed consistently from the scratch.

In normal circumstances, the constraint files are not going to change if new version of Airflow CTL
dependencies are released - not even when those versions contain critical security fixes.
The process of Airflow CTL releases is designed around upgrading dependencies automatically where
applicable but only when we release a new version of Airflow CTL, not for already released versions.

Between the releases you can upgrade dependencies on your own and you can keep your own constraints
updated as described in the previous section.

The easiest way to keep-up with the latest released dependencies is to upgrade to the latest released
Airflow CTL version. Whenever we release a new version of Airflow CTL, we upgrade all dependencies to the latest
applicable versions and test them together, so if you want to keep up with those tests - staying up-to-date
with latest version of Airflow CTL is the easiest way to update those dependencies.
