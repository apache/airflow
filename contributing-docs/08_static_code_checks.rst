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
All the static code checks can be run through prek hooks.

The prek hooks perform all the necessary installation when you run them
for the first time.

You can also run the checks via `Breeze <../dev/breeze/doc/README.rst>`_ environment.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Prek hooks
----------

Prek hooks help speed up your local development cycle and place less burden on the CI infrastructure.
Consider installing the prek hooks as a necessary prerequisite.

The hooks by default only check the files you are currently working on (and are staged) which makes the
checks rather fast. Yet, these checks use exactly the same environment as the CI tests
use. So, you can be sure your modifications will also work for CI if they pass
prek hooks.

We have integrated the `prek <https://github.com/j178/prek>`__ framework
in our development workflow. It can be installed in various ways and does not even need ``pip`` or

``python`` to be installed. It is a drop-in replacement for the legacy ``pre-commit`` tool, but it is
much faster and more feature-rich. It is written in Rust and it is designed to install environments in parallel,
so it is much faster than the ``pre-commit`` tool.

Installing prek hooks
---------------------

It is the best to use prek hooks when you have your local virtualenv for
Airflow activated since then prek hooks and other dependencies are
automatically installed. You can also install the prek hooks manually using ``uv`` or ``pipx``.

.. code-block:: bash

    uv tool install prek

.. code-block:: bash

    pipx install prek

Since we have a lot of hooks and sometimes you want to run them individually, it's advised to install
auto-completion for the ``prek`` command. You can do it by adding the following line to your
``.bashrc`` or ``.zshrc`` file:

For bash:

.. code-block:: bash

    eval "$(COMPLETE=bash prek)"  # for bash

For zsh:

.. code-block:: zsh

    eval "$(COMPLETE=zsh prek)"

Similarly for other shells like fish, powershell, etc.

After installation, prek hooks are run automatically when you commit the code and they will
only run on the files that you change during your commit, so they are usually pretty fast and do
not slow down your iteration speed on your changes. There are also ways to disable the prek hooks
temporarily when you commit your code with ``--no-verify`` switch or skip certain checks that you find
to much disturbing your local workflow. See `Using prek <#using-prek>`_

The ``prek`` hooks use several external linters that need to be installed before prek is run.
Each of the checks installs its own environment, so you do not need to install those, but there are some
checks that require locally installed binaries. On Linux, you typically install
them with ``sudo apt install``, on macOS - with ``brew install``.

The current list of prerequisites is limited to ``xmllint`` and ``golang`` if you want to modify
the Golang code.:

- on Linux, install via ``sudo apt install libxml2-utils golang``
- on macOS, install via ``brew install libxml2 golang``

Some prek hooks also require the Docker Engine to be configured as the static
checks are executed in the Docker environment. You should build the images
locally before installing prek checks as described in `Breeze docs <../dev/breeze/doc/README.rst>`__.

Sometimes your image is outdated and needs to be rebuilt because some dependencies have been changed.
In such cases, the Docker-based prek will inform you that you should rebuild the image.

Enabling prek hooks
-------------------

To turn on prek checks for ``commit`` operations in git, enter:

.. code-block:: bash

    prek install

To install the checks also for ``pre-push`` operations, enter:

.. code-block:: bash

    prek install --hook-type pre-push

For details on advanced usage of the install method, use:

.. code-block:: bash

   prek install --help

.. note::

    The ``prek`` tool is a drop-in replacement for the legacy ``pre-commit`` tool - much faster and more
    feature-rich, If you have already installed ``pre-commit`` to handle your hooks, you can run
    ``prek install -f`` to replace the existing ``pre-commit`` hooks with the ``prek`` hooks.

Available prek hooks
--------------------

You can see the list of available hooks by running:

.. code-block:: bash

    prek list

You can also see more details about the hooks by running:

.. code-block:: bash

    prek list --verbose

And if you want to see the details of a particular hook, you can run:

.. code-block:: bash

    prek list --verbose <hook-id>

When you install auto-completion, you can also use the tab-completion to see the available hooks.

Using prek
----------

After installation, prek hooks are run automatically when you commit the
code or push it to the repository (depending on stages configured for the hooks). Some of the
hooks are configured to run on "manual" stage only and are not run automatically.

By default when you run ``prek``, the ``pre-commit`` stage hooks are run.

But you can run prek hooks manually as needed.

-   Run all checks on your staged files by using:

.. code-block:: bash

    prek

-   Run only mypy check on your staged airflow and dev files by specifying the
    ``mypy-airflow-core`` and ``mypy-dev`` prek hooks (more hooks can be specified):

.. code-block:: bash

    prek mypy-airflow-core mypy-dev  --hook-stage pre-push

-   Run only mypy airflow checks on all "airflow-core" files by using:

.. code-block:: bash

    prek mypy-airflow-core --all-files --hook-stage pre-push

-   Run all pre-commit stage hooks on all files by using:

.. code-block:: bash

    prek --all-files

-   Run all pre-commit stage hooks only on files modified in the last locally available
    commit in your checked out branch:

.. code-block:: bash

    prek --last-commit

-   Run all pre-commit stage hooks only on files modified in your last branch that is targeted
    to be merged into the main branch:

.. code-block:: bash

    prek --from-ref main

-   Show files modified automatically by prek when prek automatically fixes errors (after running all
    ``pre-commit`` stage hooks on locally modified files):

.. code-block:: bash

    prek --show-diff-on-failure

-   Skip one or more of the checks by specifying a comma-separated list of
    checks to skip in the SKIP variable:

.. code-block:: bash

    SKIP=ruff,rst-backticks prek --all-files


You can always skip running the tests by providing ``--no-verify`` flag to the
``git commit`` command.

To check other usage types of the pre-commit framework, see `Pre-commit website <https://pre-commit.com/>`__.

Disabling particular checks
---------------------------

In case you have a problem with running particular ``prek`` check you can still continue using the
benefits of having ``prek`` installed, with some of the checks disabled. In order to disable
checks you might need to set ``SKIP`` environment variable to coma-separated list of checks to skip. For example,
when you want to skip some checks (ruff/mypy for example), you should be able to do it by setting
``export SKIP=ruff,mypy-airflow-core,``. You can also add this to your ``.bashrc`` or ``.zshrc`` if you
do not want to set it manually every time you enter the terminal.

In case you do not have breeze image configured locally, you can also disable all checks that require breeze
the image by setting ``SKIP_BREEZE_PREK_HOOKS`` to "true". This will mark the tests as "green" automatically
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

Manual prek hooks
-----------------

Most of the checks we run are configured to run automatically when you commit the code or push PR. However,
there are some checks that are not run automatically and you need to run them manually. You can run
them manually by running ``prek --hook-stage manual <hook-id>``.

Special pin-versions prek
-------------------------

There is a separate prek ``pin-versions`` prek hook which is used to pin versions of
GitHub Actions in the CI workflows.

This action requires ``GITHUB_TOKEN`` to be set, otherwise you might hit the rate limits with GitHub API, it
It is not run automatically when you commit the code but in runs as a separate job in the CI.
However, you can run it manually by running:

.. code-block:: bash

    export GITHUB_TOKEN=YOUR_GITHUB_TOKEN
    prek --all-files --hook-stage manual --verbose pin-versions


Mypy checks
-----------

When we run mypy checks locally when pushing a change to PR, the ``mypy-*`` checks is run, ``mypy-airflow``,
``mypy-dev``, ``mypy-providers``, ``mypy-airflow-ctl``, depending on the files you are changing. The mypy checks
are run by passing those changed files to mypy. This is way faster than running checks for all files (even
if mypy cache is used - especially when you change a file in Airflow core that is imported and used by many
files). You also need to have ``breeze ci-image build --python 3.10`` built locally to run the mypy checks.

However, in some cases, it produces different results than when running checks for the whole set
of files, because ``mypy`` does not even know that some types are defined in other files and it might not
be able to follow imports properly if they are dynamic. Therefore in CI we run ``mypy`` check for whole
directories (``airflow`` - excluding providers, ``providers``, ``dev`` and ``docs``) to make sure
that we catch all ``mypy`` errors - so you can experience different results when running mypy locally and
in CI. If you want to run mypy checks for all files locally, you can do it by running the following
command (example for ``airflow`` files):

.. code-block:: bash

  prek --hook-stage manual mypy-<FOLDER> --all-files

For example:

.. code-block:: bash

  prek --hook-stage manual mypy-airflow --all-files

To show unused mypy ignores for any providers/airflow etc, eg: run below command:

.. code-block:: bash
  export SHOW_UNUSED_MYPY_WARNINGS=true
  prek --hook-stage manual mypy-airflow --all-files

MyPy uses a separate docker-volume (called ``mypy-cache-volume``) that keeps the cache of last MyPy
execution in order to speed MyPy checks up (sometimes by order of magnitude). While in most cases MyPy
will handle refreshing the cache when and if needed, there are some cases when it won't (cache invalidation
is the hard problem in computer science). This might happen for example when we upgrade MyPY. In such
cases you might need to manually remove the cache volume by running ``breeze down --cleanup-mypy-cache``.

-----------

Once your code passes all the static code checks, you should take a look at `Testing documentation <09_testing.rst>`__
to learn about various ways to test the code.
