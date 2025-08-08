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

Static code checks
==================

The static code checks in Airflow are used to verify that the code meets certain quality standards.
All the static code checks can be run through prefligit hooks.

The prefligit hooks perform all the necessary installation when you run them
for the first time. See the table below to identify which prefligit checks require the Breeze Docker images.

You can also run the checks via `Breeze <../dev/breeze/doc/README.rst>`_ environment.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

prefligit hooks
---------------

prefligit hooks help speed up your local development cycle and place less burden on the CI infrastructure.
Consider installing the prefligit hooks as a necessary prerequisite.

The prefligit hooks by default only check the files you are currently working on and make
them fast. Yet, these checks use exactly the same environment as the CI tests
use. So, you can be sure your modifications will also work for CI if they pass
prefligit hooks.

We have integrated the `prefligit <https://github.com/j178/prefligit>`__ framework
in our development workflow. To install and use it, you need at least Python 3.10 locally.

Installing prefligit hooks
--------------------------

It is the best to use prefligit hooks when you have your local virtualenv for
Airflow activated since then prefligit hooks and other dependencies are
automatically installed. You can also install the prefligit hooks manually using ``uv`` or ``pipx``.

.. code-block:: bash

    uv tool install prefligit

.. code-block:: bash

    pipx install prefligit

After installation, prefligit hooks are run automatically when you commit the code and they will
only run on the files that you change during your commit, so they are usually pretty fast and do
not slow down your iteration speed on your changes. There are also ways to disable the ``prefligits``
temporarily when you commit your code with ``--no-verify`` switch or skip certain checks that you find
to much disturbing your local workflow. See `Available prefligit checks <#available-prefligit-checks>`_
and `Using prefligit <#using-prefligit>`_

The prefligit hooks use several external linters that need to be installed before prefligit is run.
Each of the checks installs its own environment, so you do not need to install those, but there are some
checks that require locally installed binaries. On Linux, you typically install
them with ``sudo apt install``, on macOS - with ``brew install``.

The current list of prerequisites is limited to ``xmllint``:

- on Linux, install via ``sudo apt install libxml2-utils``
- on macOS, install via ``brew install libxml2``

Some prefligit hooks also require the Docker Engine to be configured as the static
checks are executed in the Docker environment. You should build the images
locally before installing prefligit checks as described in `Breeze docs <../dev/breeze/doc/README.rst>`__.

Sometimes your image is outdated and needs to be rebuilt because some dependencies have been changed.
In such cases, the Docker-based prefligit will inform you that you should rebuild the image.

Enabling prefligit hooks
-------------------------

To turn on prefligit checks for ``commit`` operations in git, enter:

.. code-block:: bash

    prefligit install

To install the checks also for ``pre-push`` operations, enter:

.. code-block:: bash

    prefligit install -t pre-push


For details on advanced usage of the install method, use:

.. code-block:: bash

   prefligit install --help

Using prefligit
----------------

After installation, prefligit hooks are run automatically when you commit the
code. But you can run prefligit hooks manually as needed.

-   Run all checks on your staged files by using:

.. code-block:: bash

    prefligit run

-   Run only mypy check on your staged files (in ``airflow/`` excluding providers) by using:

.. code-block:: bash

    prefligit run mypy-airflow

-   Run only mypy checks on all files by using:

.. code-block:: bash

    prefligit run mypy-airflow --all-files


-   Run all checks on all files by using:

.. code-block:: bash

    prefligit run --all-files


-   Run all checks only on files modified in the last locally available commit in your checked out branch:

.. code-block:: bash

    prefligit run --source=HEAD^ --origin=HEAD


-   Show files modified automatically by prefligit when prefligits automatically fix errors

.. code-block:: bash

    prefligit run --show-diff-on-failure

-   Skip one or more of the checks by specifying a comma-separated list of
    checks to skip in the SKIP variable:

.. code-block:: bash

    SKIP=mypy-airflow-core,ruff prefligit run --all-files


You can always skip running the tests by providing ``--no-verify`` flag to the
``git commit`` command.

To check other usage types of the prefligit framework, see `prefligit website <https://prefligit.com/>`__.

Disabling particular checks
---------------------------

In case you have a problem with running particular ``prefligit`` check you can still continue using the
benefits of having ``prefligit`` installed, with some of the checks disabled. In order to disable
checks you might need to set ``SKIP`` environment variable to coma-separated list of checks to skip. For example,
when you want to skip some checks (ruff/mypy for example), you should be able to do it by setting
``export SKIP=ruff,mypy-airflow-core,``. You can also add this to your ``.bashrc`` or ``.zshrc`` if you
do not want to set it manually every time you enter the terminal.

In case you do not have breeze image configured locally, you can also disable all checks that require breeze
the image by setting ``SKIP_BREEZE_PREFLIGITS`` to "true". This will mark the tests as "green" automatically
when run locally (note that those checks will anyway run in CI).

Disabling goproxy for firewall issues
-------------------------------------

Sometimes your environment might not allow to connect to the ``goproxy`` server, which is used to
proxy/cache Go modules. When your firewall blocks go proxy it usually ends with message similar to:

.. code-block:: text

  lookup proxy.golang.org: i/o timeout

In such case, you can disable the ``goproxy`` by setting the
``GOPROXY`` environment variable to "direct". You can do it by running:

.. code-block:: bash

    export GOPROXY=direct

Alternatively if your company has its own Go proxy, you can set the ``GOPROXY`` to
your company Go proxy URL. For example:

.. code-block:: bash

    export GOPROXY=https://mycompanygoproxy.com

See `Go Proxy lesson <https://www.practical-go-lessons.com/chap-18-go-module-proxies#configuration-of-the-go-module-proxy>`__)
for more details on how to configure Go proxy - including setting multiple proxies.

You can add the variable to your ``.bashrc`` or ``.zshrc`` if you do not want to set it manually every time you
enter the terminal.

Manual prefligits
------------------

Most of the checks we run are configured to run automatically when you commit the code. However,
there are some checks that are not run automatically and you need to run them manually. Those
checks are marked with ``manual`` in the ``Description`` column in the table below. You can run
them manually by running ``prefligit run --hook-stage manual <hook-id>``.

Special pin-versions prefligit
-------------------------------

There is a separate prefligit ``pin-versions`` prefligit which is used to pin versions of
GitHub Actions in the CI workflows.

This action requires ``GITHUB_TOKEN`` to be set, otherwise you might hit the rate limits with GitHub API, it
is also configured in a separate ``.prefligit-config.yaml`` file in the
``.github`` directory as it requires Python 3.11 to run. It is not run automatically
when you commit the code but in runs as a separate job in the CI. However, you can run it
manually by running:

.. code-block:: bash

    export GITHUB_TOKEN=YOUR_GITHUB_TOKEN
    prefligit run -c .github/.prefligit-config.yaml --all-files --hook-stage manual --verbose


Mypy checks
-----------

When we run mypy checks locally when committing a change, one of the ``mypy-*`` checks is run, ``mypy-airflow``,
``mypy-dev``, ``mypy-providers``, ``mypy-airflow-ctl``, depending on the files you are changing. The mypy checks
are run by passing those changed files to mypy. This is way faster than running checks for all files (even
if mypy cache is used - especially when you change a file in Airflow core that is imported and used by many
files). However, in some cases, it produces different results than when running checks for the whole set
of files, because ``mypy`` does not even know that some types are defined in other files and it might not
be able to follow imports properly if they are dynamic. Therefore in CI we run ``mypy`` check for whole
directories (``airflow`` - excluding providers, ``providers``, ``dev`` and ``docs``) to make sure
that we catch all ``mypy`` errors - so you can experience different results when running mypy locally and
in CI. If you want to run mypy checks for all files locally, you can do it by running the following
command (example for ``airflow`` files):

.. code-block:: bash

  prefligit run --hook-stage manual mypy-<FOLDER> --all-files

For example:

.. code-block:: bash

  prefligit run --hook-stage manual mypy-airflow --all-files

To show unused mypy ignores for any providers/airflow etc, eg: run below command:

.. code-block:: bash
  export SHOW_UNUSED_MYPY_WARNINGS=true
  prefligit run --hook-stage manual mypy-airflow --all-files

MyPy uses a separate docker-volume (called ``mypy-cache-volume``) that keeps the cache of last MyPy
execution in order to speed MyPy checks up (sometimes by order of magnitude). While in most cases MyPy
will handle refreshing the cache when and if needed, there are some cases when it won't (cache invalidation
is the hard problem in computer science). This might happen for example when we upgrade MyPY. In such
cases you might need to manually remove the cache volume by running ``breeze down --cleanup-mypy-cache``.

Debugging prefligit check scripts requiring image
--------------------------------------------------

Those commits that use Breeze docker image might sometimes fail, depending on your operating system and
docker setup, so sometimes it might be required to run debugging with the commands. This is done via
two environment variables ``VERBOSE`` and ``DRY_RUN``. Setting them to "true" will respectively show the
commands to run before running them or skip running the commands.

Note that you need to run prefligit with --verbose command to get the output regardless of the status
of the static check (normally it will only show output on failure).

Printing the commands while executing:

.. code-block:: bash

     VERBOSE="true" prefligit run --verbose ruff

Just performing dry run:

.. code-block:: bash

     DRY_RUN="true" prefligit run --verbose ruff

-----------

Once your code passes all the static code checks, you should take a look at `Testing documentation <09_testing.rst>`__
to learn about various ways to test the code.
