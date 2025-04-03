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

Building documentation
======================

Airflow uses Sphinx to build the documentation. As of Airflow 3 when we split
the distributions into multiple packages, and moved the documentation to inside those
distribution building the documentation is way easier than it was on Airflow 2 and you
can usually iterate on documentation way faster, including automatically refreshing the
documentation in your browser when using ``sphinx-autobuild``.

The current distributions we have that have sphinx-buildable documentation are:

Documentation in separate distributions:

* ``airflow-core/docs`` - documentation for Airflow Core
* ``providers/**/docs`` - documentation for Providers
* ``chart/docs`` - documentation for Helm Chart
* ``task-sdk/docs`` - documentation for Task SDK (new format not yet published)
* ``airflow-ctl/docs`` - documentation for Airflow CLI (future)

Documentation for general overview and summaries not connected with amy specific distribution:

* ``docker-stack-docs`` - documentation for Docker Stack'
* ``providers-summary-docs`` - documentation for provider summary page


Building documentation with uv in local venv
--------------------------------------------

Prerequisites
.............

First of all. you need to have ``uv`` installed. You can very easily build the documentation
in your local virtualenv using ``uv`` command.

.. warning::

   There is a prerequisite needed to build the documentation locally. Airflow's Sphinx configuration
   depends on ``enchant`` C-library that needs to be installed and configured to be used by ``pyenchant``
   Python distribution package. You can install it as explained in the
   `pyenchant documentation <https://pyenchant.github.io/pyenchant/install.html>`__. Usually you use your
   package manager on Linux or ``brew`` on ``MacOS``. Sometimes you might also need to configure it
   so that ``pyenchant`` can find the dictionary files. You will get error like:

   .. code-block:: txt

       Could not import extension sphinxcontrib.spelling (exception: The 'enchant' C library was not
       found and maybe needs to be installed

   In this case the easiest way is to find where your enchant library is (usually libenchant-2.dylib)
   and set the ``PYENCHANT_LIBRARY_PATH`` environment variable to that path in your ``.zshrc`` or ``.bashrc``
   - depending which terminal you use and restart your terminal. For example on MacOS,
   ``brew install enchant`` or ``brew reinstall enchant`` will print the path where enchant is
   installed and usually the library is in the ``lib`` subfolder of it. An example of line added
   to one of the ``.rc`` files:

   .. code-block:: bash

      export PYENCHANT_LIBRARY_PATH=/opt/homebrew/Cellar/enchant/2.8.2/lib/libenchant-2.dylib

   Also for some of the providers you might need to have trouble in installing dependencies. The ``uv run``
   command will automatically install the needed dependencies for you but some of them might need extra
   system dependencies. You can always revert to ``breeze`` way of building the documentation in this
   case (see below) in case you have problems with ``uv run`` command not being able to install
   the dependencies.

Once you have enchant installed and configured, you can build the documentation very easily.



First thing to do, is to sync the inter-sphinx inventory. You can do it only once and refresh it periodically
when you find that your documentation cannot be build because of missing inter-sphinx inventory or links
The inventory is basically a list of links to other documentation that is used in the Airflow documentation
and our build issues warnings if the inventory links are missing.

.. code-block:: bash

    cd devel-common
    uv run src/sphinx_exts/docs_build/fetch_inventories.py

When you want to refresh the inventory, you can run the same command with ``clean`` flag.

.. code-block:: bash

    cd devel-common
    uv run src/sphinx_exts/docs_build/fetch_inventories.py clean


To run docs building go to the directory where documentation is placed (usually ``doc`` subfolder of
a distribution you want to build the documentation for) and run, but some distributions do not have ``docs``
subfolder if they are ``doc-only``.

.. code-block:: bash
      cd <YOUR-DISTRIBUTION>/docs
      uv run --group doc sphinx-build -T --color -b html  . _build

To check spelling:

.. code-block:: bash
      cd <YOUR-DISTRIBUTION>/docs
      uv run --group doc sphinx-build -T --color -b spelling  . _build

But most importantly you can run a documentation server that will expose the built documentation and
enable auto-refreshing build (automatically refreshing the browser when you change the documentation files):

.. code-block:: bash
      cd <YOUR-DISTRIBUTION>/docs
      uv run sphinx-autobuild -T --color -b html  . _build

That should allow you to build the documentation and see the changes in your browser.

Note that those build commands are not the same as the "breeze build-docs" command. The ``breeze build-docs``
command is a more complete way of building the whole documentation together for all distributions.

.. warning::

   Sometimes - when you have references between distributions, you might need to build those distributions
   first and then build the documentation for the distribution that depends on them, but in this case you will
   need to specify the right ``_build`` directory - one of the ``generated/_build`` subfolder, depending on
   the distribution. It's easier in this case to use the breeze command to build such dependent package
   locally.

Building documentation in Breeze
--------------------------------

This used to be the default way of building documentation - but this method requires things to start
and run in Docker containers and is not as fast as using ``uv`` - especially when iterating with changes.
and especially comparing it to sphinx-autobuild.

Basic command is ``breeze build-docs``. You can also specify a number of options like selecting which
distribution packages you want to build and which kind of build to run (``--doc-only`` od ``--spelling-only``)
or request to ``--clean`` the build directory before building the documentation.

For example:

.. code-block:: bash

    breeze build-docs --doc-only --clean fab

Will build ``fab`` provider documentation and clean inventories and other build artifacts before.

You can also use ``breeze build-docs --help`` to see available options and head to
`breeze documentation <../dev/breeze/doc/03_developer_tasks.rst>`__ to learn more about the ``breeze``
command and it's options.
