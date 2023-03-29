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

.. contents:: :local:

Note to Starters
################

Airflow is quite a complex project, and setting up a working environment, but we made it rather simple if
you follow the guide.

There are three ways you can run the Airflow dev env:

1. With a Docker Containers and Docker Compose (on your local machine). This environment is managed
   with `Breeze <BREEZE.rst>`_ tool written in Python that makes the environment management, yeah you
   guessed it - a breeze.
2. With a local virtual environment (on your local machine).
3. With a remote, managed environment (via remote development environment)

Before deciding which method to choose, there are a couple factors to consider:

* Running Airflow in a container is the most reliable way: it provides a more consistent environment
  and allows integration tests with a number of integrations (cassandra, mongo, mysql, etc.).
  However it also requires **4GB RAM, 40GB disk space and at least 2 cores**.
* If you are working on a basic feature, installing Airflow on a local environment might be sufficient.
  For a comprehensive venv tutorial - visit
  `Virtual Env guide <https://github.com/apache/airflow/blob/main/LOCAL_VIRTUALENV.rst>`_
* You need to have usually a paid account to access managed, remote virtual environment.

Local machine development
#########################

If you do not work with remote development environment, you need those prerequisites.

1. Docker Community Edition (you can also use Colima, see instructions below)
2. Docker Compose
3. pyenv (you can also use pyenv-virtualenv or virtualenvwrapper)

The below setup describe `Ubuntu installation <https://docs.docker.com/engine/install/ubuntu/>`_. It might be slightly different on different machines.

Docker Community Edition
------------------------

1. Installing required packages for Docker and setting up docker repo

.. code-block:: bash

  $ sudo apt-get update

  $ sudo apt-get install \
      ca-certificates \
      curl \
      gnupg \
      lsb-release

  $ sudo mkdir -p /etc/apt/keyrings
  $ curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

  $ echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

2. Install Docker Engine, containerd, and Docker Compose Plugin.

.. code-block:: bash

  $ sudo apt-get update
  $ sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin

3. Creating group for docker and adding current user to it.

.. code-block:: bash

  $ sudo groupadd docker
  $ sudo usermod -aG docker $USER

Note : After adding user to docker group Logout and Login again for group membership re-evaluation.

4. Test Docker installation

.. code-block:: bash

  $ docker run hello-world

Colima
------
If you use Colima as your container runtimes engine, please follow the next steps:

1. `Install buildx manually <https://github.com/docker/buildx#manual-download>`_ and follow it's instructions

2. Link the Colima socket to the default socket path. Note that this may break other Docker servers.

.. code-block:: bash

  $ sudo ln -sf $HOME/.colima/default/docker.sock /var/run/docker.sock

3. Change docker context to use default:

.. code-block:: bash

  $ docker context use default

Docker Compose
--------------

1. Installing latest version of Docker Compose

.. code-block:: bash

  $ COMPOSE_VERSION="$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep '"tag_name":'\
  | cut -d '"' -f 4)"

  $ COMPOSE_URL="https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/\
  docker-compose-$(uname -s)-$(uname -m)"

  $ sudo curl -L "${COMPOSE_URL}" -o /usr/local/bin/docker-compose

  $ sudo chmod +x /usr/local/bin/docker-compose

2. Verifying installation

.. code-block:: bash

  $ docker-compose --version

Pyenv and setting up virtual-env
--------------------------------
    Note: You might have issues with pyenv if you have a Mac with an M1 chip. Consider using virtualenv as an alternative.

1. Install pyenv and configure your shell's environment for Pyenv as suggested in Pyenv `README <https://github.com/pyenv/pyenv/blob/master/README.md#installation>`_

2. After installing pyenv, you need to install a few more required packages for Airflow. The below command adds
   basic system-level dependencies on Debian/Ubuntu-like system. You will have to adapt it to install similar packages
   if your operating system is MacOS or another flavour of Linux


.. code-block:: bash

  $ sudo apt install openssl sqlite default-libmysqlclient-dev libmysqlclient-dev postgresql

If you want to install all airflow providers, more system dependencies might be needed. For example on Debian/Ubuntu
like system, this command will install all necessary dependencies that should be installed when you use ``devel_all``
extra while installing airflow.

.. code-block:: bash

  $ sudo apt install apt-transport-https apt-utils build-essential ca-certificates dirmngr \
  freetds-bin freetds-dev git gosu graphviz graphviz-dev krb5-user ldap-utils libffi-dev \
  libkrb5-dev libldap2-dev libpq-dev libsasl2-2 libsasl2-dev libsasl2-modules \
  libssl-dev locales lsb-release openssh-client sasl2-bin \
  software-properties-common sqlite3 sudo unixodbc unixodbc-dev

3. Restart your shell so the path changes take effect and verifying installation

.. code-block:: bash

  $ exec $SHELL
  $ pyenv --version

4. Checking available version, installing required Python version to pyenv and verifying it

.. code-block:: bash

  $ pyenv install --list
  $ pyenv install 3.8.5
  $ pyenv versions

5. Creating new virtual environment named ``airflow-env`` for installed version python. In next chapter virtual
   environment ``airflow-env`` will be used for installing airflow.

.. code-block:: bash

  $ pyenv virtualenv 3.8.5 airflow-env

6. Entering virtual environment ``airflow-env``

.. code-block:: bash

  $ pyenv activate airflow-env


Forking and cloning Project
---------------------------

1. Goto |airflow_github| and fork the project.

   .. |airflow_github| raw:: html

     <a href="https://github.com/apache/airflow/" target="_blank">https://github.com/apache/airflow/</a>

   .. raw:: html

     <div align="center" style="padding-bottom:10px">
       <img src="images/quick_start/airflow_fork.png"
            alt="Forking Apache Airflow project">
     </div>

2. Goto your github account's fork of airflow click on ``Code`` you will find the link to your repo.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/airflow_clone.png"
             alt="Cloning github fork of Apache airflow">
      </div>

3. Follow `Cloning a repository <https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository>`_
   to clone the repo locally (you can also do it in your IDE - see the `Using your IDE <using-your-ide>`_
   chapter below.

Typical development tasks
#########################

For many of the development tasks you will need ``Breeze`` to be configured. ``Breeze`` is a development
environment which uses docker and docker-compose and it's main purpose is to provide a consistent
and repeatable environment for all the contributors and CI. When using ``Breeze`` you avoid the "works for me"
syndrome - because not only others can reproduce easily what you do, but also the CI of Airflow uses
the same environment to run all tests - so you should be able to easily reproduce the same failures you
see in CI in your local environment.

Setting up Breeze
-----------------

1. Install ``pipx`` - follow the instructions in   `Install pipx <https://pypa.github.io/pipx/>`_


2. Run ``pipx install -e ./dev/breeze`` in your checked-out repository. Make sure to follow any instructions
   printed by ``pipx`` during the installation - this is needed to make sure that ``breeze`` command is
   available in your PATH.

3. Initialize breeze autocomplete

.. code-block:: bash

  $ breeze setup autocomplete

4. Initialize breeze environment with required python version and backend. This may take a while for first time.

.. code-block:: bash

  $ breeze --python 3.8 --backend postgres

.. note::
   If you encounter an error like "docker.credentials.errors.InitializationError:
   docker-credential-secretservice not installed or not available in PATH", you may execute the following command to fix it:

   .. code-block:: bash

      $ sudo apt install golang-docker-credential-helper

   Once the package is installed, execute the breeze command again to resume image building.


5. When you enter Breeze environment you should see prompt similar to ``root@e4756f6ac886:/opt/airflow#``. This
   means that you are inside the Breeze container and ready to run most of the development tasks. You can leave
   the environment with ``exit`` and re-enter it with just ``breeze`` command.
   Once you enter breeze environment, create airflow tables and users from the breeze CLI. ``airflow db reset``
   is required to execute at least once for Airflow Breeze to get the database/tables created. If you run
   tests, however - the test database will be initialized automatically for you.

.. code-block:: bash

  root@b76fcb399bb6:/opt/airflow# airflow db reset
  root@b76fcb399bb6:/opt/airflow# airflow users create --role Admin --username admin --password admin \
    --email admin@example.com --firstname foo --lastname bar


6. Exiting Breeze environment. After successfully finishing above command will leave you in container,
   type ``exit`` to exit the container. The database created before will remain and servers will be
   running though, until you stop breeze environment completely.

.. code-block:: bash

  root@b76fcb399bb6:/opt/airflow#
  root@b76fcb399bb6:/opt/airflow# exit

6. You can stop the environment (which means deleting the databases and database servers running in the
   background) via ``breeze stop`` command.

.. code-block:: bash

  $ breeze stop


Using Breeze
------------

1. Starting breeze environment using ``breeze start-airflow`` starts Breeze environment with last configuration run(
   In this case python and backend will be picked up from last execution ``breeze --python 3.8 --backend postgres``)
   It also automatically starts webserver, backend and scheduler. It drops you in tmux with scheduler in bottom left
   and webserver in bottom right. Use ``[Ctrl + B] and Arrow keys`` to navigate.

.. code-block:: bash

  $ breeze start-airflow

      Use CI image.

   Branch name:            main
   Docker image:           ghcr.io/apache/airflow/main/ci/python3.8:latest
   Airflow source version: 2.4.0.dev0
   Python version:         3.8
   Backend:                mysql 5.7


   Port forwarding:

   Ports are forwarded to the running docker containers for webserver and database
     * 12322 -> forwarded to Airflow ssh server -> airflow:22
     * 28080 -> forwarded to Airflow webserver -> airflow:8080
     * 25555 -> forwarded to Flower dashboard -> airflow:5555
     * 25433 -> forwarded to Postgres database -> postgres:5432
     * 23306 -> forwarded to MySQL database  -> mysql:3306
     * 21433 -> forwarded to MSSQL database  -> mssql:1443
     * 26379 -> forwarded to Redis broker -> redis:6379

   Here are links to those services that you can use on host:
     * ssh connection for remote debugging: ssh -p 12322 airflow@127.0.0.1 (password: airflow)
     * Webserver: http://127.0.0.1:28080
     * Flower:    http://127.0.0.1:25555
     * Postgres:  jdbc:postgresql://127.0.0.1:25433/airflow?user=postgres&password=airflow
     * Mysql:     jdbc:mysql://127.0.0.1:23306/airflow?user=root
     * MSSQL:     jdbc:sqlserver://127.0.0.1:21433;databaseName=airflow;user=sa;password=Airflow123
     * Redis:     redis://127.0.0.1:26379/0


.. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/start_airflow_tmux.png"
             alt="Accessing local airflow">
      </div>


- Alternatively you can start the same using following commands

  1. Start Breeze

  .. code-block:: bash

    $ breeze --python 3.8 --backend postgres

  2. Open tmux

  .. code-block:: bash

    $ root@0c6e4ff0ab3d:/opt/airflow# tmux

  3. Press Ctrl + B and "

  .. code-block:: bash

    $ root@0c6e4ff0ab3d:/opt/airflow# airflow scheduler


  4. Press Ctrl + B and %

  .. code-block:: bash

    $ root@0c6e4ff0ab3d:/opt/airflow# airflow webserver


2. Now you can access airflow web interface on your local machine at |http://127.0.0.1:28080| with user name ``admin``
   and password ``admin``.

   .. |http://127.0.0.1:28080| raw:: html

      <a href="http://127.0.0.1:28080" target="_blank">http://127.0.0.1:28080</a>

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/local_airflow.png"
             alt="Accessing local airflow">
      </div>

3. Setup mysql database in
   MySQL Workbench with Host ``127.0.0.1``, port ``23306``, user ``root`` and password
   blank(leave empty), default schema ``airflow``.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/mysql_connection.png"
             alt="Connecting to mysql">
      </div>

4. Stopping breeze

.. code-block:: bash

  root@f3619b74c59a:/opt/airflow# stop_airflow
  root@f3619b74c59a:/opt/airflow# exit
  $ breeze stop

5. Knowing more about Breeze

.. code-block:: bash

  $ breeze --help


For more information visit : |Breeze documentation|

.. |Breeze documentation| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/BREEZE.rst" target="_blank">Breeze documentation</a>

Following are some of important topics of Breeze documentation:


- |Choosing different Breeze environment configuration|

.. |Choosing different Breeze environment configuration| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/BREEZE.rst#choosing-different-breeze-environment-configuration
   " target="_blank">Choosing different Breeze environment configuration</a>


- |Troubleshooting Breeze environment|

.. |Troubleshooting Breeze environment| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/BREEZE.rst#troubleshooting" target="_blank">Troubleshooting
   Breeze environment</a>


- |Installing Additional tools to the Docker Image|

.. |Installing Additional tools to the Docker Image| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/BREEZE.rst#additional-tools" target="_blank">Installing
   Additional tools to the Docker Image</a>


- |Breeze Command-Line Interface Reference|

.. |Breeze Command-Line Interface Reference| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/BREEZE.rst#installation"
   target="_blank">Breeze Command-Line Interface Reference</a>


- |Cleaning the environment|

.. |Cleaning the environment| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/BREEZE.rst#cleaning-the-environment" target="_blank">
   Cleaning the environment</a>



Configuring Pre-commit
----------------------

Before committing changes to github or raising a pull request, code needs to be checked for certain quality standards
such as spell check, code syntax, code formatting, compatibility with Apache License requirements etc. This set of
tests are applied when you commit your code.

.. raw:: html

  <div align="center" style="padding-bottom:20px">
    <img src="images/quick_start/ci_tests.png"
         alt="CI tests GitHub">
  </div>


To avoid burden on CI infrastructure and to save time, Pre-commit hooks can be run locally before committing changes.

1.  Installing required packages

.. code-block:: bash

  $ sudo apt install libxml2-utils

2. Installing required Python packages

.. code-block:: bash

  $ pyenv activate airflow-env
  $ pip install pre-commit

3. Go to your project directory

.. code-block:: bash

  $ cd ~/Projects/airflow


1. Running pre-commit hooks

.. code-block:: bash

  $ pre-commit run --all-files
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

5. Running pre-commit for selected files

.. code-block:: bash

  $ pre-commit run  --files airflow/utils/decorators.py tests/utils/test_task_group.py



6. Running specific hook for selected files

.. code-block:: bash

  $ pre-commit run black --files airflow/decorators.py tests/utils/test_task_group.py
    black...............................................................Passed
  $ pre-commit run ruff --files airflow/decorators.py tests/utils/test_task_group.py
    Run ruff............................................................Passed



7. Enabling Pre-commit check before push. It will run pre-commit automatically before committing and stops the commit

.. code-block:: bash

  $ cd ~/Projects/airflow
  $ pre-commit install
  $ git commit -m "Added xyz"

8. To disable Pre-commit

.. code-block:: bash

  $ cd ~/Projects/airflow
  $ pre-commit uninstall


- For more information on visit |STATIC_CODE_CHECKS.rst|

.. |STATIC_CODE_CHECKS.rst| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/STATIC_CODE_CHECKS.rst" target="_blank">
   STATIC_CODE_CHECKS.rst</a>

- Following are some of the important links of STATIC_CODE_CHECKS.rst

  - |Pre-commit Hooks|

  .. |Pre-commit Hooks| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/STATIC_CODE_CHECKS.rst#pre-commit-hooks" target="_blank">
   Pre-commit Hooks</a>

  - |Running Static Code Checks via Breeze|

  .. |Running Static Code Checks via Breeze| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/STATIC_CODE_CHECKS.rst#running-static-code-checks-via-breeze"
   target="_blank">Running Static Code Checks via Breeze</a>


Installing airflow in the local venv
------------------------------------

1. It may require some packages to be installed; watch the output of the command to see which ones are missing.

.. code-block:: bash

  $ sudo apt-get install sqlite libsqlite3-dev default-libmysqlclient-dev postgresql
  $ ./scripts/tools/initialize_virtualenv.py


2. Add following line to ~/.bashrc in order to call breeze command from anywhere.

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
   platform linux -- Python 3.8.16, pytest-7.2.1, pluggy-1.0.0 -- /usr/local/bin/python
   cachedir: .pytest_cache
   rootdir: /opt/airflow, configfile: pytest.ini
   plugins: timeouts-1.2.1, capture-warnings-0.0.4, cov-4.0.0, requests-mock-1.10.0, rerunfailures-11.1.1, anyio-3.6.2, instafail-0.4.2, time-machine-2.9.0, asyncio-0.20.3, httpx-0.21.3, xdist-3.2.0
   asyncio: mode=strict
   setup timeout: 0.0s, execution timeout: 0.0s, teardown timeout: 0.0s
   collected 12 items

   tests/utils/test_dates.py::TestDates::test_days_ago PASSED                                                                               [  8%]
   tests/utils/test_dates.py::TestDates::test_parse_execution_date PASSED                                                                   [ 16%]
   tests/utils/test_dates.py::TestDates::test_round_time PASSED                                                                             [ 25%]
   tests/utils/test_dates.py::TestDates::test_infer_time_unit PASSED                                                                        [ 33%]
   tests/utils/test_dates.py::TestDates::test_scale_time_units PASSED                                                                       [ 41%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_no_delta PASSED                                                                 [ 50%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_end_date_before_start_date PASSED                                               [ 58%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_both_end_date_and_num_given PASSED                                              [ 66%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_invalid_delta PASSED                                                            [ 75%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_positive_num_given PASSED                                                       [ 83%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_negative_num_given PASSED                                                       [ 91%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_delta_cron_presets PASSED                                                       [100%]

   ============================================================== 12 passed in 0.24s ==============================================================

- Running All the test with Breeze by specifying required python version, backend, backend version

.. code-block:: bash

   $ breeze --backend postgres --postgres-version 10 --python 3.8 --db-reset testing tests --test-type All

- Running specific type of test

  - Types of tests

  - Running specific type of test

  .. code-block:: bash

    $ breeze --backend postgres --postgres-version 10 --python 3.8 --db-reset testing tests --test-type Core


- Running Integration test for specific test type

  - Running an Integration Test

  .. code-block:: bash

   $ breeze --backend postgres --postgres-version 10 --python 3.8 --db-reset testing tests --test-type All --integration mongo


- For more information on Testing visit : |TESTING.rst|

.. |TESTING.rst| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/TESTING.rst" target="_blank">TESTING.rst</a>

- Following are the some of important topics of TESTING.rst

  - |Airflow Test Infrastructure|

  .. |Airflow Test Infrastructure| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/TESTING.rst#airflow-test-infrastructure" target="_blank">
   Airflow Test Infrastructure</a>


  - |Airflow Unit Tests|

  .. |Airflow Unit Tests| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/TESTING.rst#airflow-unit-tests" target="_blank">Airflow Unit
   Tests</a>


  - |Helm Unit Tests|

  .. |Helm Unit Tests| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/TESTING.rst#helm-unit-tests" target="_blank">Helm Unit Tests
   </a>


  - |Airflow Integration Tests|

  .. |Airflow Integration Tests| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/TESTING.rst#airflow-integration-tests" target="_blank">
   Airflow Integration Tests</a>


  - |Running Tests with Kubernetes|

  .. |Running Tests with Kubernetes| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/TESTING.rst#running-tests-with-kubernetes" target="_blank">
   Running Tests with Kubernetes</a>


  - |Airflow System Tests|

  .. |Airflow System Tests| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/TESTING.rst#airflow-system-tests" target="_blank">Airflow
   System Tests</a>


  - |Local and Remote Debugging in IDE|

  .. |Local and Remote Debugging in IDE| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/TESTING.rst#local-and-remote-debugging-in-ide"
   target="_blank">Local and Remote Debugging in IDE</a>

Contribution guide
##################

- To know how to contribute to the project visit |CONTRIBUTING.rst|

.. |CONTRIBUTING.rst| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst" target="_blank">CONTRIBUTING.rst</a>

- Following are some of important links of CONTRIBUTING.rst

  - |Types of contributions|

  .. |Types of contributions| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#contributions" target="_blank">
   Types of contributions</a>


  - |Roles of contributor|

  .. |Roles of contributor| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#roles" target="_blank">Roles of
   contributor</a>


  - |Workflow for a contribution|

  .. |Workflow for a contribution| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#contribution-workflow" target="_blank">
   Workflow for a contribution</a>



Raising Pull Request
--------------------

1. Go to your GitHub account and open your fork project and click on Branches

   .. raw:: html

    <div align="center" style="padding-bottom:10px">
      <img src="images/quick_start/pr1.png"
           alt="Goto fork and select branches">
    </div>

2. Click on ``New pull request`` button on branch from which you want to raise a pull request.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/pr2.png"
             alt="Accessing local airflow">
      </div>

3. Add title and description as per Contributing guidelines and click on ``Create pull request``.

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


- |Syncing fork|

.. |Syncing fork| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#how-to-sync-your-fork" target="_blank">
   Update new changes made to apache:airflow project to your fork</a>


- |Rebasing pull request|

.. |Rebasing pull request| raw:: html

   <a href="https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#how-to-rebase-pr" target="_blank">
   Rebasing pull request</a>

Using your IDE
##############

If you are familiar with Python development and use your favourite editors, Airflow can be setup
similarly to other projects of yours. However, if you need specific instructions for your IDE you
will find more detailed instructions here:

* `Pycharm/IntelliJ <CONTRIBUTORS_QUICK_START_PYCHARM.rst>`_
* `Visual Studio Code <CONTRIBUTORS_QUICK_START_VSCODE.rst>`_


Using Remote development environments
#####################################

In order to use remote development environment, you usually need a paid account, but you do not have to
setup local machine for development.

* `GitPod <CONTRIBUTORS_QUICK_START_GITPOD.rst>`_
* `GitHub Codespaces <CONTRIBUTORS_QUICK_START_CODESPACES.rst>`_
