
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

Troubleshooting
===============

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Basic troubleshooting of breeze command
---------------------------------------

If you are having problems with the Breeze environment, try the steps below. After each step you
can check whether your problem is fixed.

1. If you are on macOS, check if you have enough disk space for Docker (Breeze will warn you if not).
2. Stop Breeze with ``breeze down``.
3. Git fetch the origin and git rebase the current branch with main branch.
4. Delete the ``.build`` directory and run ``breeze ci-image build``.
5. Clean up Docker images via ``breeze cleanup`` command.
6. Restart your Docker Engine and try again.
7. Restart your machine and try again.
8. Re-install Docker Desktop and try again.

.. note::
  If the pip is taking a significant amount of time and your internet connection is causing pip to be unable to download the libraries within the default timeout, it is advisable to modify the default timeout as follows and run the breeze again.

  .. code-block::

      export PIP_DEFAULT_TIMEOUT=1000

In case the problems are not solved, you can set the VERBOSE_COMMANDS variable to "true":

.. code-block::

        export VERBOSE_COMMANDS="true"


Then run the failed command, copy-and-paste the output from your terminal to the
`Airflow Slack <https://s.apache.org/airflow-slack>`_  ``#airflow-breeze`` channel and
describe your problem.


.. warning::

    Some operating systems (Fedora, ArchLinux, RHEL, Rocky) have recently introduced Kernel changes that result in
    Airflow in Breeze consuming 100% memory when run inside the community Docker implementation maintained
    by the OS teams.

    This is an issue with backwards-incompatible containerd configuration that some of Airflow dependencies
    have problems with and is tracked in a few issues:

    * `Moby issue <https://github.com/moby/moby/issues/43361>`_
    * `Containerd issue <https://github.com/containerd/containerd/pull/7566>`_

    There is no solution yet from the containerd team, but seems that installing
    `Docker Desktop on Linux <https://docs.docker.com/desktop/install/linux-install/>`_ solves the problem as
    stated in `This comment <https://github.com/moby/moby/issues/43361#issuecomment-1227617516>`_ and allows to
    run Breeze with no problems.

Cannot import name 'cache' or Python >=3.9 required
---------------------------------------------------

When you see this error:

.. code-block::

    ImportError: cannot import name 'cache' from 'functools' (/Users/jarek/Library/Application Support/hatch/pythons/3.8/python/lib/python3.8/functools.py)

or

.. code-block::

    ERROR: Package 'blacken-docs' requires a different Python: 3.8.18 not in '>=3.9'


It means that your pre-commit hook is installed with (already End-Of-Life) Python 3.8 and you should reinstall
it and clean pre-commit cache.

This can be done (if you use ``pipx`` to install ``pre-commit``):

.. code-block:: bash

    pipx uninstall pre-commit
    pipx install pre-commit --python $(which python3.9) --force
    # This one allows pre-commit to use uv for venvs installed by pre-commit
    pipx inject pre-commit pre-commit-uv
    pre-commit clean
    pre-commit install

If you installed ``pre-commit`` differently, you should remove and reinstall
it (and clean cache) in the way you installed it.


Bad Interpreter Error
---------------------

If you are experiencing bad interpreter errors
``zsh: /Users/eladkal/.local/bin/breeze: bad interpreter: /Users/eladkal/.local/pipx/venvs/apache-airflow-breeze/bin/python: no such file or directory``

try to run ``pipx list`` to view which packages has bad interpreter (it can be more than just breeze, for example  pre-commit)
you can fix these errors by running ``pipx reinstall-all``

ETIMEDOUT Error
--------------

When running ``breeze start-airflow``, either normally or in ``dev-mode``, the following output might be observed:

.. code-block:: bash

    Skip fixing ownership of generated files as Host OS is darwin


    Waiting for asset compilation to complete in the background.

    Still waiting .....
    Still waiting .....
    Still waiting .....
    Still waiting .....
    Still waiting .....
    Still waiting .....

    The asset compilation is taking too long.

    If it does not complete soon, you might want to stop it and remove file lock:
      * press Ctrl-C
      * run 'rm /opt/airflow/.build/www/.asset_compile.lock'

    Still waiting .....
    Still waiting .....
    Still waiting .....
    Still waiting .....
    Still waiting .....
    Still waiting .....
    Still waiting .....

    The asset compilation failed. Exiting.

    [INFO] Locking pre-commit directory

    Error 1 returned

This timeout can be increased by setting ``ASSET_COMPILATION_WAIT_MULTIPLIER`` a reasonable number
could be 3-4.

.. code-block:: bash

  export ASSET_COMPILATION_WAIT_MULTIPLIER=3

This error is actually caused by the following error during the asset compilation which resulted in
``ETIMEDOUT`` when ``npm`` command is trying to install required packages:

.. code-block:: bash

    npm ERR! code ETIMEDOUT
    npm ERR! syscall connect
    npm ERR! errno ETIMEDOUT
    npm ERR! network request to https://registry.npmjs.org/yarn failed, reason: connect ETIMEDOUT 2606:4700::6810:1723:443
    npm ERR! network This is a problem related to network connectivity.
    npm ERR! network In most cases you are behind a proxy or have bad network settings.
    npm ERR! network
    npm ERR! network If you are behind a proxy, please make sure that the
    npm ERR! network 'proxy' config is set properly.  See: 'npm help config'

In this situation, notice that the IP address ``2606:4700::6810:1723:443`` is in IPv6 format, which was the
reason why the connection did not go through the router, as the router did not support IPv6 addresses in its DNS lookup.
In this case, disabling IPv6 in the host machine and using IPv4 instead resolved the issue.

The similar issue could happen if you are behind an HTTP/HTTPS proxy and your access to required websites are
blocked by it, or your proxy setting has not been done properly.

It could also be possible that you have a proxy which is not available from your network, leading to the timeout
issue. You may try running the below commands in the same terminal and then try the ``breeze start-airflow`` command:

.. code-block::

    npm config delete http-proxy
    npm config delete https-proxy

    npm config rm proxy
    npm config rm https-proxy

    set HTTP_PROXY=null
    set HTTPS_PROXY=null

----

Next step: Follow the `Test commands <05_test_commands.rst>`_ guide to running tests using Breeze.
