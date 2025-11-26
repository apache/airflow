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

Airflow is a complex project, but setting up a working environment is quite simple
if you follow the guide.

There are three ways you can run the Airflow dev env:

1. With a local virtual environment (on your local machine)
2. With Docker Containers and Docker Compose (on your local machine). This environment is managed
   with the `Breeze <../dev/breeze/doc/README.rst>`_ tool written in Python that makes environment
   management, yeah you guessed it - a breeze.
3. With a remote, managed environment (via remote development environment)

Before deciding which method to choose, there are a couple of factors to consider:

* In most cases, installing Airflow in a local environment might be sufficient.
  For a comprehensive local virtualenv tutorial, visit `Local virtualenv <07_local_virtualenv.rst>`_
* Running Airflow in a container is the most reliable and repeatable way: it provides a more consistent
  environment - with almost no dependencies (except docker) on your Host OS / machine
  and allows integration tests with a number of integrations (Cassandra, MongoDB, MySQL, etc.).
  However, it also requires **4GB RAM, 40GB disk space and at least 2 cores**.
* You need to have a (usually paid) account to access managed, remote virtual environments.

Local machine development
#########################

If you do not work in a remote development environment, you will need these prerequisites:

1. UV is recommended for managing Python versions and virtual environments
2. Docker Community Edition (you can also use Colima or others, see instructions below)
3. Docker buildx
4. Docker Compose

The below setup describes `Ubuntu installation <https://docs.docker.com/engine/install/ubuntu/>`_.
It might be slightly different on different machines.

Docker Community Edition
------------------------

1. Installing required packages for Docker and setting up docker repo

.. code-block:: bash

  sudo apt-get update

  sudo apt-get install \
      ca-certificates \
      curl \
      gnupg \
      lsb-release

  sudo mkdir -p /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

2. Install Docker Engine, containerd

.. code-block:: bash

  sudo apt-get update
  sudo apt-get install docker-ce docker-ce-cli containerd.io

3. Manage docker as non-root user

.. code-block:: bash

  sudo groupadd docker
  sudo usermod -aG docker $USER

.. note::
    This is done so a non-root user can access the ``docker`` command.
    After adding user to docker group Logout and Login again for group membership re-evaluation.
    On some Linux distributions, the system automatically creates this group.

4. Test Docker installation

.. code-block:: bash

  docker run hello-world

.. note::
    Read more about `Linux post-installation steps for Docker Engine <https://docs.docker.com/engine/install/linux-postinstall/>`_.

Colima
------
If you use Colima as your container runtimes engine, please follow the next steps:

1. `Install buildx manually <https://github.com/docker/buildx#manual-download>`_ and follow its instructions

2. Link the Colima socket to the default socket path. Note that this may break other Docker servers

.. code-block:: bash

  sudo ln -sf $HOME/.colima/default/docker.sock /var/run/docker.sock

3. Change docker context to use default

.. code-block:: bash

  docker context use default

Docker Compose
--------------

1. Installing latest version of the Docker Compose plugin

on Debian / Ubuntu,
Install using the repository:

.. code-block:: bash

  sudo apt-get update
  sudo apt-get install docker-compose-plugin

Install manually:

.. code-block:: bash

  COMPOSE_VERSION="$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep '"tag_name":'\
  | cut -d '"' -f 4)"

  COMPOSE_URL="https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/\
  docker-compose-$(uname -s)-$(uname -m)"

  sudo curl -L "${COMPOSE_URL}" -o /usr/local/bin/docker-compose

  sudo chmod +x /usr/local/bin/docker-compose
.. note::
    This option requires you to manage updates manually.
    It is recommended that you set up Docker's repository for easier maintenance.

on macOS, you can also install docker-compose via

.. code-block:: bash

  brew install docker-compose


1. Verifying installation

.. code-block:: bash

  docker-compose --version

Setting up virtual-env
----------------------

1. While you can use any virtualenv manager, we recommend using `UV <https://github.com/astral-sh/uv>`__
   as your build and integration frontend. You can read more about UV and its use in
   Airflow in `Local virtualenv <07_local_virtualenv.rst>`_.

2. After creating the environment, you need to install a few more required packages for Airflow. The below command adds
   basic system-level dependencies on Debian/Ubuntu-like system. You will have to adapt it to install similar packages
   if your operating system is MacOS or another flavour of Linux

.. code-block:: bash

  sudo apt install openssl sqlite3 default-libmysqlclient-dev libmysqlclient-dev postgresql

If you want to install all Airflow providers, more system dependencies might be needed. For example on Debian/Ubuntu
like system, this command will install all necessary dependencies that should be installed when you use
``all`` extras while installing airflow.

.. code-block:: bash

  sudo apt install apt-transport-https apt-utils build-essential ca-certificates dirmngr \
  freetds-bin freetds-dev git graphviz graphviz-dev krb5-user ldap-utils libffi-dev \
  libkrb5-dev libldap2-dev libpq-dev libsasl2-2 libsasl2-dev libsasl2-modules \
  libssl-dev locales lsb-release openssh-client sasl2-bin \
  software-properties-common sqlite3 sudo unixodbc unixodbc-dev

Forking and cloning Project
---------------------------

1. Go to |airflow_github| and fork the project

   .. |airflow_github| raw:: html

     <a href="https://github.com/apache/airflow/" target="_blank">https://github.com/apache/airflow/</a>

   .. raw:: html

     <div align="center" style="padding-bottom:10px">
       <img src="images/quick_start/airflow_fork.png"
            alt="Forking Apache Airflow project">
     </div>

2. Go to your github account's fork of Airflow click on ``Code`` you will find the link to your repo

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/airflow_clone.png"
             alt="Cloning github fork of Apache airflow">
      </div>

3. Follow `Cloning a repository <https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository>`_
   to clone the repo locally (you can also do it in your IDE - see the `Using your IDE`_
   chapter below

.. note::
    For windows based machines, on cloning, the Git line endings may be different from unix based systems
    and might lead to unexpected behaviour on running breeze tooling. Manually setting a property will mitigate this issue.
    Set it to true for windows.

.. code-block:: bash

  git config core.autocrlf true

Configuring prek
----------------

Before committing changes to github or raising a pull request, the code needs to be checked for certain quality standards
such as spell check, code syntax, code formatting, compatibility with Apache License requirements etc. This set of
tests are applied when you commit your code.

.. raw:: html

  <div align="center" style="padding-bottom:20px">
    <img src="images/quick_start/ci_tests.png"
         alt="CI tests GitHub">
  </div>


To avoid burden on our CI infrastructure and to save time, prek hooks can be run locally before committing changes.

.. note::
    We have recently started to recommend ``uv`` for our local development.

.. note::
    Remember to have global python set to Python >= 3.10 - Python 3.10 is end-of-life already and we've
    started to use Python 3.10+ features in Airflow and accompanying scripts.

Installing prek is best done with ``uv`` (recommended) or ``pipx``.

1.  Installing required packages

on Debian / Ubuntu, install via

.. code-block:: bash

  sudo apt install libxml2-utils

on macOS, install via

.. code-block:: bash

  brew install libxml2

2. Installing prek:

.. note::
  You might need to pass ``--python <python>`` to force the python version if not it uses the latest system python version.
  python value can be fetched from ``uv python list``

.. code-block:: bash

  uv tool install prek

or with pipx:

.. code-block:: bash

  pipx install prek


3. Go to your project directory

.. code-block:: bash

  cd ~/Projects/airflow


4. Running prek hooks

.. code-block:: bash

  prek --all-files
    No-tabs checker......................................................Passed
    Add license for all SQL files........................................Passed
    Add license for all other files......................................Passed
    Add license for all rst files........................................Passed
    Add license for all JS/CSS/PUML files................................Passed
    Add license for all JINJA template files.............................Passed
    Add license for all shell files......................................Passed
    Add license for all python files.....................................Passed
    Add license for all XML files........................................Passed
    Add license for all yaml files.......................................Passed
    Add license for all md files.........................................Passed
    Add license for all mermaid files....................................Passed
    Add TOC for md files.................................................Passed
    Add TOC for upgrade documentation....................................Passed
    Check hooks apply to the repository..................................Passed
    black................................................................Passed
    Check for merge conflicts............................................Passed
    Debug Statements (Python)............................................Passed
    Check builtin type constructor use...................................Passed
    Detect Private Key...................................................Passed
    Fix End of Files.....................................................Passed
    ...........................................................................

5. Running prek for selected files

.. code-block:: bash

  prek  --files airflow-core/src/airflow/utils/decorators.py  airflow-core/tests/unit/utils/test_task_group.py


6. Running specific hook for selected files

.. code-block:: bash

  prek black --files airflow-core/src/airflow/utils/decorators.py airflow-core/tests/unit/utils/test_task_group.py
    black...............................................................Passed
  prek ruff --files airflow-core/src/airflow/utils/decorators.py airflow-core/tests/unit/utils/test_task_group.py
    Run ruff............................................................Passed


7. Enabling prek hook check before push

It will run prek hooks automatically before committing and stops the commit on failure

.. code-block:: bash

  cd ~/Projects/airflow
  prek install
  git commit -m "Added xyz"

8. To disable prek hooks

.. code-block:: bash

  cd ~/Projects/airflow
  prek uninstall

- For more information on this visit |08_static_code_checks.rst|

.. |08_static_code_checks.rst| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/contributing-docs/08_static_code_checks.rst" target="_blank">
   08_static_code_checks.rst</a>

- Following are some of the important links of 08_static_code_checks.rst

  - |Prek Hooks|

  .. |Prek Hooks| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/contributing-docs/08_static_code_checks.rst#prek-hooks" target="_blank">
   Prek Hooks</a>

  - |Running Static Code Checks via Breeze|

  .. |Running Static Code Checks via Breeze| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/contributing-docs/08_static_code_checks.rst#running-static-code-checks-via-breeze"
   target="_blank">Running Static Code Checks via Breeze</a>


Setting up Breeze
#################

For many of the development tasks you will need ``Breeze`` to be configured. ``Breeze`` is a development
environment which uses docker and docker-compose and its main purpose is to provide a consistent
and repeatable environment for all the contributors and CI. When using ``Breeze`` you avoid the "works for me"
syndrome - because not only others can reproduce easily what you do, but also the CI of Airflow uses
the same environment to run all tests - so you should be able to easily reproduce the same failures you
see in CI in your local environment.

1. Install ``uv`` or ``pipx``. We recommend to install ``uv`` as the general purpose python development
   environment - you can install it via https://docs.astral.sh/uv/getting-started/installation/ or you can
   install ``pipx`` (>=1.2.1) - follow the instructions in `Install pipx <https://pipx.pypa.io/stable/>`_
   It is important to install version of pipx >= 1.2.1 to workaround ``packaging`` breaking change introduced
   in September 2023

2. Run ``uv tool install -e ./dev/breeze`` (or ``pipx install -e ./dev/breeze`` in your checked-out
   repository. Make sure to follow any instructions printed during the installation - this is needed
   to make sure that the ``breeze`` command is available in your PATH

.. warning::

  If you see below warning while running pipx - it means that you have hit the
  `known issue <https://github.com/pypa/pipx/issues/1092>`_ with ``packaging`` version 23.2:

  .. code-block:: bash

    ⚠️ Ignoring --editable install option. pipx disallows it for anything but a local path,
    to avoid having to create a new src/ directory.

  The workaround is to downgrade packaging to 23.1 and re-running the ``pipx install`` command, for example
  by running ``pip install "packaging<23.2"``.

  .. code-block:: bash

     pip install "packaging==23.1"
     pipx install -e ./dev/breeze --force

3. Initialize breeze autocomplete

.. code-block:: bash

  breeze setup autocomplete

4. Initialize breeze environment with required python version and backend. This may take a while for first time.

.. code-block:: bash

  breeze --python 3.10 --backend postgres

.. note::
   If you encounter an error like "docker.credentials.errors.InitializationError:
   docker-credential-secretservice not installed or not available in PATH", you may execute the following command to fix it:

   .. code-block:: bash

      sudo apt install golang-docker-credential-helpers

   Once the package is installed, execute the breeze command again to resume image building.

   If you encounter an error such as

   .. code-block:: text

      jinja2.exceptions.TemplateNotFound: '/index.html' not found in search path: '/opt/airflow/airflow-core/src/airflow/ui/dist'

   you may need to compile the UI assets before starting the Breeze environment. To do so, run the following command **before** executing step 4:

   .. code-block:: bash

      breeze compile-ui-assets

   After running this, verify that the compiled UI assets have been added to ``/airflow/.build/ui``.

   Then, proceed with:

   .. code-block:: bash

      breeze --python 3.10 --backend postgres


5. When you enter the Breeze environment you should see a prompt similar to ``root@e4756f6ac886:/opt/airflow#``. This
   means that you are inside the Breeze container and ready to run most of the development tasks. You can leave
   the environment with ``exit`` and re-enter it with just ``breeze`` command

6. Once you enter the Breeze environment, create Airflow tables and users from the breeze CLI. ``airflow db reset``
   is required to execute at least once for Airflow Breeze to get the database/tables created. If you run
   tests, however - the test database will be initialized automatically for you

.. code-block:: bash

  root@b76fcb399bb6:/opt/airflow# airflow db reset

.. code-block:: bash

        root@b76fcb399bb6:/opt/airflow# airflow users create \
                --username admin \
                --firstname FIRST_NAME \
                --lastname LAST_NAME \
                --role Admin \
                --email admin@example.org

.. note::
    ``airflow users`` command is only available when `FAB auth manager <https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/index.html>`_ is enabled.

7. Exiting the Breeze environment. After successfully finishing above command will leave you in container,
   type ``exit`` to exit the container. The database created before will remain and servers will be
   running though, until you stop the Breeze environment completely

.. code-block:: bash

  root@b76fcb399bb6:/opt/airflow# exit

8. You can stop the environment (which means deleting the databases and database servers running in the
   background) via ``breeze down`` command

.. code-block:: bash

  breeze down


Using Breeze
------------

1. Starting the Breeze environment using ``breeze start-airflow`` starts the Breeze environment with last configuration run(
   In this case Python version and backend are picked up from last execution ``breeze --python 3.10 --backend postgres``)
   It also automatically starts the API server (FastAPI api and UI), triggerer, dag processor and scheduler. It drops you in tmux with triggerer to the right, and
   Scheduler, API server (FastAPI api and UI), Dag processor from left to right at the bottom. Use ``[Ctrl + B] and Arrow keys`` to navigate.

.. code-block:: bash

  breeze start-airflow

      Use CI image.

   Branch name:            main
   Docker image:           ghcr.io/apache/airflow/main/ci/python3.10:latest
   Airflow source version: 2.4.0.dev0
   Python version:         3.10
   Backend:                mysql 5.7

   * Port forwarding:

        Ports are forwarded to the running docker containers for components and database
          * 12322 -> forwarded to Airflow ssh server -> airflow:22
          * 28080 -> forwarded to Airflow api server API -> airflow:8080
          * 25555 -> forwarded to Flower dashboard -> airflow:5555
          * 25433 -> forwarded to Postgres database -> postgres:5432
          * 23306 -> forwarded to MySQL database  -> mysql:3306
          * 26379 -> forwarded to Redis broker -> redis:6379

        Direct links to those services that you can use from the host:

          * ssh connection for remote debugging: ssh -p 12322 airflow@localhost (password: airflow)
          * API server:    http://localhost:28080
          * Flower:    http://localhost:25555
          * Postgres:  jdbc:postgresql://localhost:25433/airflow?user=postgres&password=airflow
          * Mysql:     jdbc:mysql://localhost:23306/airflow?user=root
          * Redis:     redis://localhost:26379/0


.. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/start_airflow_tmux.png"
             alt="Accessing local airflow">
      </div>


- Alternatively you can start the same using the following commands

  1. Start Breeze

  .. code-block:: bash

    breeze --python 3.10 --backend postgres

  2. Open tmux

  .. code-block:: bash

     tmux

  3. Press Ctrl + B and "

  .. code-block:: bash

    airflow scheduler


  4. Press Ctrl + B and %

  .. code-block:: bash

    airflow api-server

  5. Press Ctrl + B and %

  .. code-block:: bash

    airflow dag-processor

  6. Press Ctrl + B and up arrow followed by Ctrl + B and %

  .. code-block:: bash

    airflow triggerer

  7. Press Ctrl + B followed by (Optional step for better tile arrangement)

  .. code-block:: bash

    :select-layout tiled


2. Now you can access Airflow web interface on your local machine at |http://localhost:28080| with user name ``admin``
   and password ``admin``

   .. |http://localhost:28080| raw:: html

      <a href="http://localhost:28080" target="_blank">http://localhost:28080</a>

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/local_airflow.png"
             alt="Accessing local airflow">
      </div>

3. Setup a PostgreSQL database in your database management tool of choice
   (e.g. DBeaver, DataGrip) with host ``localhost``, port ``25433``,
   user ``postgres``,  password ``airflow``, and default schema ``airflow``

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/postgresql_connection.png"
             alt="Connecting to postgresql">
      </div>

4. Stopping breeze

If ``breeze`` was started with ``breeze start-airflow``, this command will stop breeze and Airflow:

.. code-block:: bash

  root@f3619b74c59a:/opt/airflow# stop_airflow
  breeze down

If ``breeze`` was started with ``breeze --python 3.10 --backend postgres`` (or similar):

.. code-block:: bash

  root@f3619b74c59a:/opt/airflow# exit
  breeze down

.. note::
    ``stop_airflow`` is available only when ``breeze`` is started with ``breeze start-airflow``.

Using mprocs Instead of tmux
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, ``breeze start-airflow`` uses tmux to manage Airflow components. You can use mprocs as an
alternative with the ``--use-mprocs`` flag:

.. code-block:: bash

  breeze start-airflow --use-mprocs

**Benefits of using mprocs:**

* Modern terminal UI with better visual feedback
* Easier navigation with mouse and keyboard
* Individual process controls (start, stop, restart)
* Process status indicators
* Better cross-platform support

For more information on mprocs, look at `mprocs documentation <mprocs/MPROCS_QUICK_REFERENCE.md>`__.

1. Knowing more about Breeze

.. code-block:: bash

   breeze --help


Following are some of important topics of `Breeze documentation <../dev/breeze/doc/README.rst>`__:

* `Breeze Installation <../dev/breeze/doc/01_installation.rst>`__
* `Installing Additional tools to the Docker Image <../dev/breeze/doc/02-customizing.rst#additional-tools-in-breeze-container>`__
* `Regular developer tasks <../dev/breeze/doc/03_developer_tasks.rst>`__
* `Cleaning the environment <../dev/breeze/doc/03_developer_tasks.rst#breeze-cleanup>`__
* `Troubleshooting Breeze environment <../dev/breeze/doc/04_troubleshooting.rst>`__


Installing Airflow in the local venv
------------------------------------

1. It may require some packages to be installed; watch the output of the command to see which ones are missing

.. code-block:: bash

  sudo apt-get install sqlite3 libsqlite3-dev default-libmysqlclient-dev postgresql
  ./scripts/tools/initialize_virtualenv.py


2. Add following line to ~/.bashrc in order to call breeze command from anywhere

.. code-block:: bash

  export PATH=${PATH}:"/home/${USER}/Projects/airflow"
  source ~/.bashrc

Running tests with Breeze
-------------------------

You can usually conveniently run tests in your IDE (see IDE below) using virtualenv but with Breeze you
can be sure that all the tests are run in the same environment as tests in CI.

All Tests are inside ./tests directory.

- Running Unit tests inside Breeze environment.

  Just run ``pytest filepath+filename`` to run the tests.

.. code-block:: bash

   root@63528318c8b1:/opt/airflow# pytest tests/utils/test_dates.py
   ============================================================= test session starts ==============================================================
   platform linux -- Python 3.10.20, pytest-8.3.3, pluggy-1.5.0 -- /usr/python/bin/python
   cachedir: .pytest_cache
   rootdir: /opt/airflow
   configfile: pyproject.toml
   plugins: anyio-4.6.0, time-machine-2.15.0, icdiff-0.9, rerunfailures-14.0, instafail-0.5.0, custom-exit-code-0.3.0, xdist-3.6.1, mock-3.14.0, cov-5.0.0, asyncio-0.24.0, requests-mock-1.12.1, timeouts-1.2.1
   asyncio: mode=strict, default_loop_scope=None
   setup timeout: 0.0s, execution timeout: 0.0s, teardown timeout: 0.0s
   collected 4 items

   tests/utils/test_dates.py::TestDates::test_parse_execution_date PASSED                                                                           [ 25%]
   tests/utils/test_dates.py::TestDates::test_round_time PASSED                                                                                     [ 50%]
   tests/utils/test_dates.py::TestDates::test_infer_time_unit PASSED                                                                                [ 75%]
   tests/utils/test_dates.py::TestDates::test_scale_time_units PASSED                                                                               [100%]

   ================================================================== 4 passed in 3.30s ===================================================================

- Running All the tests with Breeze by specifying the required python version, backend, backend version

.. code-block:: bash

   breeze --backend postgres --postgres-version 15 --python 3.10 --db-reset testing tests --test-type All

- Running specific type of test

  .. code-block:: bash

    breeze --backend postgres --postgres-version 15 --python 3.10 --db-reset testing tests --test-type Core


- Running Integration test for specific test type

  .. code-block:: bash

   breeze --backend postgres --postgres-version 15 --python 3.10 --db-reset testing tests --test-type All --integration mongo

- For more information on Testing visit |09_testing.rst|

  .. |09_testing.rst| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/contributing-docs/09_testing.rst" target="_blank">09_testing.rst</a>

- Similarly to regular development, you can also debug while testing using your IDE, for more information, you may refer to

  |Local and Remote Debugging in IDE|

  .. |Local and Remote Debugging in IDE| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/contributing-docs/07_local_virtualenv.rst#local-and-remote-debugging-in-ide"
   target="_blank">Local and Remote Debugging in IDE</a>

Contribution guide
##################

- To know how to contribute to the project visit |README.rst|

.. |README.rst| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/contributing-docs/README.rst" target="_blank">README.rst</a>

- Following are some of the important links of Contribution documentation

  - |Types of contributions|

  .. |Types of contributions| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/contributing-docs/04_how_to_contribute.rst" target="_blank">
   Types of contributions</a>

  - |Roles of contributor|

  .. |Roles of contributor| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/contributing-docs/01_roles_in_airflow_project.rst" target="_blank">Roles of
   contributor</a>


  - |Workflow for a contribution|

  .. |Workflow for a contribution| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/contributing-docs/18_contribution_workflow.rst" target="_blank">
   Workflow for a contribution</a>



Raising Pull Request
--------------------

1. Go to your GitHub account and open your fork project and click on Branches

   .. raw:: html

    <div align="center" style="padding-bottom:10px">
      <img src="images/quick_start/pr1.png"
           alt="Go to fork and select branches">
    </div>

2. Click on ``New pull request`` button on branch from which you want to raise a pull request

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/pr2.png"
             alt="Accessing local airflow">
      </div>

3. Add title and description as per Contributing guidelines and click on ``Create pull request``

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/pr3.png"
             alt="Accessing local airflow">
      </div>


Syncing Fork and rebasing Pull request
--------------------------------------

Often it takes several days or weeks to discuss and iterate with the PR until it is ready to merge.
In the meantime new commits are merged, and you might run into conflicts, therefore you should periodically
synchronize main in your fork with the ``apache/airflow`` main and rebase your PR on top of it. Following
describes how to do it.

* `Update new changes made to apache:airflow project to your fork <10_working_with_git.rst#how-to-sync-your-fork>`__
* `Rebasing pull request <10_working_with_git.rst#how-to-rebase-pr>`__


Using your IDE
##############

If you are familiar with Python development and use your favourite editors, Airflow can be setup
similarly to other projects of yours. However, if you need specific instructions for your IDE you
will find more detailed instructions here:

* `Pycharm/IntelliJ <quick-start-ide/contributors_quick_start_pycharm.rst>`_
* `Visual Studio Code <quick-start-ide/contributors_quick_start_vscode.rst>`_


Using Remote development environments
#####################################

In order to use remote development environment, you usually need a paid account, but you do not have to
setup local machine for development.

* `GitPod <quick-start-ide/contributors_quick_start_gitpod.rst>`_
* `GitHub Codespaces <quick-start-ide/contributors_quick_start_codespaces.rst>`_


----------------

Once you have your environment set up, you can start contributing to Airflow. You can find more
about ways you can contribute in the `How to contribute <04_how_to_contribute.rst>`_ document.
