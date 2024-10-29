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

Advanced Breeze topics
======================

This document describes advanced topics related to Breeze. It is intended for people who already
know how to use Breeze and want to learn more about it and understand how it works under the hood.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Debugging/developing Breeze
---------------------------

Breeze can be quite easily debugged with PyCharm/VSCode or any other IDE - but it might be less discoverable
if you never tested modules and if you do not know how to bypass version check of breeze.

For testing, you can create your own virtual environment, or use the one that ``pipx`` created for you if you
already installed breeze following the recommended ``pipx install -e ./dev/breeze`` command.

For local virtualenv, you can use ``pyenv`` or any other virtualenv wrapper. For example with ``pyenv``,
you can use ``pyenv virtualenv 3.8.6 airflow-breeze`` to create virtualenv called ``airflow-breeze``
with Python 3.8.6. Then you can use ``pyenv activate airflow-breeze`` to activate it and install breeze
in editable mode with ``pip install -e ./dev/breeze``.

For ``pipx`` virtualenv, you can use the virtualenv that ``pipx`` created for you. You can find the name
where ``pipx`` keeps their venvs via ``pipx list`` command. Usually it is
``${HOME}/.local/pipx/venvs/apache-airflow-breeze`` where ``$HOME`` is your home directory.

The venv can be used for running breeze tests and for debugging breeze. While running tests should
be usually "out-of-the-box" for most IDEs, once you configure ``./dev/breeze`` project to use the venv,
running/debugging a particular breeze command you want to debug might be a bit more tricky.

When you configure your "Run/Debug configuration" to run breeze command you should
make sure to follow these steps:

* pick one of the above interpreters to use (usually you need to choose ``bin/python`` inside the venv)
* choose ``module`` to run and set it to ``airflow_breeze.breeze`` - this is the entrypoint of breeze
* add parameters you want to run breeze with (for example ``ci-image build`` if you want to debug
  how breeze builds the CI image
* set ``SKIP_BREEZE_SELF_UPGRADE_CHECK`` environment variable to ``true`` to bypass built-in upgrade check of breeze,
  this will bypass the check we run in Breeze to see if there are new requirements to install for it

See example configuration for PyCharm which has run/debug configuration for
``breeze sbom generate-providers-requirements --provider-id sqlite --python 3.8``

.. raw:: html

    <div align="center">
        <img src="images/pycharm_debug_breeze.png" width="640" alt="Airflow Breeze - PyCharm debugger configuration">
    </div>

Then you can setup breakpoints and debug breeze as any other Python script or test.

Similar configuration can be done for VSCode.


Airflow directory structure inside container
--------------------------------------------

When you are in the CI container, the following directories are used:

.. code-block:: text

  /opt/airflow - Contains sources of Airflow mounted from the host (AIRFLOW_SOURCES).
  /root/airflow - Contains all the "dynamic" Airflow files (AIRFLOW_HOME), such as:
      airflow.db - sqlite database in case sqlite is used;
      logs - logs from Airflow executions;
      unittest.cfg - unit test configuration generated when entering the environment;
      webserver_config.py - webserver configuration generated when running Airflow in the container.
  /files - files mounted from "files" folder in your sources. You can edit them in the host as well
      dags - this is the folder where Airflow Dags are read from
      airflow-breeze-config - this is where you can keep your own customization configuration of breeze

Note that when running in your local environment, the ``/root/airflow/logs`` folder is actually mounted
from your ``logs`` directory in the Airflow sources, so all logs created in the container are automatically
visible in the host as well. Every time you enter the container, the ``logs`` directory is
cleaned so that logs do not accumulate.

When you are in the production container, the following directories are used:

.. code-block:: text

  /opt/airflow - Contains sources of Airflow mounted from the host (AIRFLOW_SOURCES).
  /root/airflow - Contains all the "dynamic" Airflow files (AIRFLOW_HOME), such as:
      airflow.db - sqlite database in case sqlite is used;
      logs - logs from Airflow executions;
      unittest.cfg - unit test configuration generated when entering the environment;
      webserver_config.py - webserver configuration generated when running Airflow in the container.
  /files - files mounted from "files" folder in your sources. You can edit them in the host as well
      dags - this is the folder where Airflow Dags are read from

Note that when running in your local environment, the ``/root/airflow/logs`` folder is actually mounted
from your ``logs`` directory in the Airflow sources, so all logs created in the container are automatically
visible in the host as well. Every time you enter the container, the ``logs`` directory is
cleaned so that logs do not accumulate.

Mounting Local Sources to Breeze
--------------------------------

Important sources of Airflow are mounted inside the ``airflow`` container that you enter.
This means that you can continue editing your changes on the host in your favourite IDE and have them
visible in the Docker immediately and ready to test without rebuilding images. You can modify mounting
options by specifying ``--mount-sources`` flag when running Breeze. For example, in the case of ``--mount-sources skip`` you will have sources
embedded in the container and changes to these sources will not be persistent.


After you run Breeze for the first time, you will have empty directory ``files`` in your source code,
which will be mapped to ``/files`` in your Docker container. You can pass there any files you need to
configure and run Docker. They will not be removed between Docker runs.

By default ``/files/dags`` folder is mounted from your local ``<AIRFLOW_SOURCES>/files/dags`` and this is
the directory used by airflow scheduler and webserver to scan dags for. You can use it to test your dags
from local sources in Airflow. If you wish to add local Dags that can be run by Breeze.

The ``/files/airflow-breeze-config`` folder contains configuration files that might be used to
customize your breeze instance. Those files will be kept across checking out a code from different
branches and stopping/starting breeze so you can keep your configuration there and use it continuously while
you switch to different source code versions.

Managing Dependencies
---------------------

There are couple of things you might want to do when adding/changing dependencies when developing with
Breeze. You can add dependencies temporarily (which will last until you exit Breeze shell), or you might
want to add them permanently (which require you to rebuild the image). Also there are different things
you need to do when you are adding system level (debian) level, Python (pip) dependencies or Node (yarn)
dependencies for the webserver.

Python dependencies
...................

For temporary adding and modifying the dependencies, you just (in Breeze shell) run
``pip install <dependency>`` or similar - in the same way as you would do it
in your local environment. You can also use ``pip install -r /files/requirements.txt`` to install several
dependencies - if you place your requirements file in ``files`` directory. Those dependencies will
disappear when you exit Breeze shell.

When you want to add dependencies permanently, then it depends what kind of dependency you add.

If you want to add core dependency that should always be installed - you need to add it to ``pyproject.toml``
to ``dependencies`` section. If you want to add it to one of the optional core extras, you should
add it in the extra definition in ``pyproject.toml`` (you need to find out where it is defined).
If you want to add it to one of the providers, you need to add it to the ``provider.yaml`` file in the provider
directory - but remember that this should be followed by running pre-commit that will automatically update
the ``pyproject.toml`` with the new dependencies as the ``provider.yaml`` files are not used directly, they
are used to update ``pyproject.toml`` file:

.. code-block:: bash

    pre-commit run update-providers-dependencies  --all-files

You can also run the pre-commit by ``breeze static-checks --type update-providers-dependencies --all-files``
command - which provides autocomplete.

After you've updated the dependencies, you need to rebuild the image:

Breeze will automatically detect when you updated dependencies and it will propose you to build image next
time when you enter it. You can answer ``y`` during 10 seconds to get it done for you.

.. code-block:: bash

    breeze ci-image build


Sometimes, when you have conflicting change in dependencies (i.e. dependencies in the old constraints
are conflicting with the new specification, you might want to build the image with
``--upgrade-to-newer-dependencies`` flag:

.. code-block:: bash

    breeze ci-image build --upgrade-to-newer-dependencies


System (debian) dependencies
............................

You can install ``apt-get`` dependencies temporarily by running ``apt-get install <dependency>`` in
Breeze shell. Those dependencies will disappear when you exit Breeze shell.

When you want to add dependencies permanently, you need to add them to ``Dockerfile.ci``. But you need to
do it indirectly via shell scripts that get automatically inlined in the ``Dockerfile.ci``. Those
scripts are present in ``scripts/docker`` directory and are aptly (!) named ``install*.sh``. Most
of the apt dependencies are installed in the ``install_os_dependencies.sh``, but some are installed in
other scripts (for example ``install_postgres.sh`` or ``install_mysql.sh``).

After you modify the dependencies in the scripts, you need to inline them by running pre-commit:

.. code-block:: bash

    pre-commit run update-inlined-dockerfile-scripts --all-files

You can also run the pre-commit by ``breeze static-checks --type update-inlined-dockerfile-scripts --all-files``
command - which provides autocomplete.


After you've updated the dependencies, you need to rebuild the image:

Breeze will automatically detect when you updated dependencies and it will propose you to build image next
time when you enter it. You can answer ``y`` during 10 seconds to get it done for you.

.. code-block:: bash

    breeze ci-image build

Sometimes, when you have conflicting change in dependencies (i.e. dependencies in the old constraints
are conflicting with the new specification, you might want to build the image with
``--upgrade-to-newer-dependencies`` flag:

.. code-block:: bash

    breeze ci-image build --upgrade-to-newer-dependencies


Node (yarn) dependencies
........................

If you need to change "node" dependencies in ``airflow/www``, you need to compile them in the
host with ``breeze compile-www-assets`` command. No need to rebuild the image.


Recording command output
------------------------

Breeze uses built-in capability of ``rich`` to record and print the command help as an ``svg`` file.
It's enabled by setting ``RECORD_BREEZE_OUTPUT_FILE`` to a file name where it will be recorded.
By default it records the screenshots with default characters width and with "Breeze screenshot" title,
but you can override it with ``RECORD_BREEZE_WIDTH`` and ``RECORD_BREEZE_TITLE`` variables respectively.

------

**Thank you for getting that far** - we hope you will enjoy using Breeze!
