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

.. contents:: Table of Contents
   :depth: 2
   :local:

Setup your project
##################

1. Open your IDE or source code editor and select the option to clone the repository

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/pycharm_clone.png"
             alt="Cloning github fork to Pycharm">
      </div>

2. Paste the repository link in the URL field and submit.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/pycharm_click_on_clone.png"
             alt="Cloning github fork to Pycharm">
      </div>

3. Run the ``dev/ide_setup/setup_idea.py`` script to configure the project automatically.
   The script runs ``uv sync`` to create the ``.venv`` virtualenv, detects the Python SDK, and
   generates the ``.idea/airflow.iml``, ``.idea/modules.xml``, and ``.idea/misc.xml`` files.

   The script supports two modes — **single-module** and **multi-module** — and
   **auto-detects** which one to use based on the installed IDE:

   * If **IntelliJ IDEA** is detected → defaults to **multi-module**.
   * If only **PyCharm** is detected (or no IDE is found) → defaults to **single-module**.

   You can override auto-detection with ``--multi-module`` or ``--single-module``.

   **Single-module mode** — all source roots are registered under one IntelliJ module.
   This works in both PyCharm and IntelliJ IDEA:

    .. code-block:: bash

      $ uv run dev/ide_setup/setup_idea.py --single-module

   **Multi-module mode** — each distribution/package gets its own IntelliJ module with a
   separate ``.iml`` file (e.g. ``airflow-core/airflow-core.iml``,
   ``providers/amazon/providers-amazon.iml``).  This gives better per-module SDK control and
   cleaner project structure in the IDE's Project view.

    .. code-block:: bash

      $ uv run dev/ide_setup/setup_idea.py --multi-module

   .. note::

      **Multi-module mode requires IntelliJ IDEA Ultimate** — it does **not** work in PyCharm
      (Community or Professional).  PyCharm does not support multiple content roots pointing to
      sub-directories of the project root that each carry their own ``.iml`` module file; it
      silently ignores or mishandles sub-modules.  IntelliJ IDEA Ultimate with the Python plugin
      handles this correctly because it has full support for the IntelliJ multi-module project
      model.  If you use PyCharm, stick with single-module mode.

   Then restart PyCharm/IntelliJ IDEA.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/pycharm-airflow.iml.png"
             alt="airflow.iml">
      </div>

   .. raw:: html

        <div align="center" style="padding-bottom:10px">
          <img src="images/pycharm-modules.xml.png"
              alt="modules.xml">
        </div>

Script options
==============

``--python VERSION``
  Choose the Python minor version for the virtualenv (e.g. ``3.12``).  The version is passed
  to ``uv sync --python`` and must be compatible with the project's ``requires-python``
  constraint.  When omitted, ``uv`` picks the default version.

  .. code-block:: bash

     $ uv run dev/ide_setup/setup_idea.py --python 3.12

``--multi-module`` / ``--single-module``
  Control whether the project is configured as a single IntelliJ module (all source roots in
  one ``.iml`` file) or as multiple modules (one ``.iml`` per distribution/package, e.g.
  ``airflow-core/airflow-core.iml``, ``providers/amazon/providers-amazon.iml``).

  **By default the script auto-detects which IDE is installed** and picks the appropriate
  mode: multi-module when IntelliJ IDEA is found, single-module when only PyCharm is found
  (or when no IDE can be detected).  Use ``--multi-module`` or ``--single-module`` to
  override the auto-detected default.

  In multi-module mode the script also creates a dedicated ``dev/breeze`` virtualenv
  (via a second ``uv sync``) with its own Python SDK named *Python X.Y (breeze)*.
  All other sub-modules inherit the project-level SDK.

  .. code-block:: bash

     # Force multi-module (requires IntelliJ IDEA)
     $ uv run dev/ide_setup/setup_idea.py --multi-module

     # Force single-module (works in both PyCharm and IntelliJ IDEA)
     $ uv run dev/ide_setup/setup_idea.py --single-module

``--confirm``
  Automatically answer yes to all interactive confirmation prompts (IDE close, process kill,
  file overwrite).  Useful for non-interactive, scripted, or agent-driven runs.

  .. code-block:: bash

     $ uv run dev/ide_setup/setup_idea.py --confirm

``--open-ide``
  Open IntelliJ IDEA or PyCharm in the project directory after setup completes.  On macOS
  uses ``open -a``, on Linux looks for JetBrains Toolbox launcher scripts and falls back to
  commands on ``PATH``.  Prefers IntelliJ IDEA when both IDEs are installed.

  .. code-block:: bash

     $ uv run dev/ide_setup/setup_idea.py --open-ide

     # Combine with --confirm for fully non-interactive setup + open
     $ uv run dev/ide_setup/setup_idea.py --confirm --open-ide

``--no-kill``
  Do not attempt to detect and kill running PyCharm/IntelliJ IDEA processes.  By default,
  the script looks for running IDE processes, asks for confirmation, sends ``SIGTERM``, and
  falls back to ``SIGKILL`` if they don't exit within 5 seconds.  Use ``--no-kill`` to
  disable this behaviour and fall back to the manual confirmation prompt instead.

  .. code-block:: bash

     $ uv run dev/ide_setup/setup_idea.py --no-kill

``--idea-path PATH``
  Path to the JetBrains configuration directory to update instead of auto-detecting all
  installed IDEs.  Can point to the base JetBrains directory
  (e.g. ``~/Library/Application Support/JetBrains``) or a specific product directory
  (e.g. ``.../JetBrains/IntelliJIdea2025.1``).  Useful when auto-detection does not find
  your IDE or when you want to target a specific installation.

  .. code-block:: bash

     $ uv run dev/ide_setup/setup_idea.py --idea-path ~/Library/Application\ Support/JetBrains/IntelliJIdea2025.1

``--exclude MODULE_OR_GROUP``
  Exclude modules from the generated project configuration.  Can be specified multiple times.
  Useful when you only work on a subset of the codebase and want faster IDE indexing.

  A value can be either a module path relative to the project root (e.g. ``providers/amazon``,
  ``dev/breeze``) or one of the recognised group names:

  * ``providers`` — all provider modules under ``providers/``
  * ``shared`` — all shared libraries under ``shared/``
  * ``dev`` — the ``dev`` module
  * ``tests`` — test-only modules (``docker-tests``, ``kubernetes-tests``, etc.)

  Examples:

  .. code-block:: bash

     # Exclude all providers and shared libraries
     $ uv run dev/ide_setup/setup_idea.py --exclude providers --exclude shared

     # Exclude a single provider
     $ uv run dev/ide_setup/setup_idea.py --exclude providers/amazon

     # Multi-module with only core modules
     $ uv run dev/ide_setup/setup_idea.py --multi-module --exclude providers --exclude shared

Options can be combined freely.  For instance, to create a multi-module project with
Python 3.12 excluding all providers:

.. code-block:: bash

   $ uv run dev/ide_setup/setup_idea.py --multi-module --python 3.12 --exclude providers

What the script generates
=========================

* ``.idea/airflow.iml`` — root module definition with source roots (single-module mode) or
  exclude-only root module (multi-module mode).
* ``.idea/modules.xml`` — module registry listing all IntelliJ modules.
* ``.idea/misc.xml`` — project-level Python SDK reference (derived from ``.venv``).
* ``.idea/.name`` — sets the PyCharm project name to ``airflow-<dirname>`` so the
  auto-detected SDK name matches the configuration.
* ``<module>/<module>.iml`` — per-module files (multi-module mode only).

The script also registers the Python SDKs (root project and Breeze) in the global
JetBrains ``jdk.table.xml`` configuration using the ``uv (<name>)`` naming convention
that matches PyCharm's auto-detected uv interpreters.  This means the SDKs are
immediately available when you open the project — no manual interpreter setup needed.

The script also configures project-wide exclusion patterns (``__pycache__``,
``node_modules``, ``*.egg-info``, cache directories, etc.) so that IntelliJ does not
index or search generated/build artifacts.

4. Alternatively, you can configure your project manually. Configure the source root directories
   for ``airflow-core``, ``task-sdk``, ``airflow-ctl`` and ``devel-common``. You also have to set
   "source" and "tests" root directories for each provider you want to develop (!).

   In Airflow 3.0 we split ``airflow-core``, ``task-sdk``, ``airflow-ctl``, ``devel-common``,
   and each provider to be separate distribution — each with separate ``pyproject.toml`` file,
   so you need to separately add ``src`` and ``tests`` directories for each provider you develop
   to be respectively "source roots" and "test roots".

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/pycharm_add_provider_sources_and_tests.png"
             alt="Adding Source Root directories to Pycharm">
      </div>

   You also need to add ``task-sdk`` sources (and ``devel-common`` in similar way).

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/pycharm_add_task_sdk_sources.png"
             alt="Adding Source Root directories to Pycharm">
      </div>

5. If you configured the project manually (step 4), configure the Python interpreter to use
   the virtualenv created by ``uv sync``:  go to ``File → Settings → Project → Python Interpreter``,
   click the gear icon, choose *Add Interpreter → Existing*, and point to ``.venv/bin/python``.
   If you used the setup script (step 3), the SDK is already registered globally — just
   restart the IDE and the interpreter will be available automatically.

    .. raw:: html

        <div align="center" style="padding-bottom:10px">
          <img src="images/pycharm_add_interpreter.png"
              alt="Configuring Python Interpreter">
        </div>

6. It is recommended to invalidate caches and restart PyCharm after setting up the project.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/pycharm_invalidate_caches.png"
             alt="Invalidate caches and restart Pycharm">
      </div>



Setting up debugging
####################

It requires "airflow-env" virtual environment configured locally.

1. Configuring Airflow database connection

- Airflow is by default configured to use SQLite database. Configuration can be seen on local machine
  ``~/airflow/airflow.cfg`` under ``sql_alchemy_conn``.

- Installing required dependency for MySQL connection in ``airflow-env`` on local machine.

  .. code-block:: bash

    $ pyenv activate airflow-env
    $ pip install PyMySQL

- Now set ``sql_alchemy_conn = mysql+pymysql://root:@127.0.0.1:23306/airflow?charset=utf8mb4`` in file
  ``~/airflow/airflow.cfg`` on local machine.

2. Debugging an example Dag

- Add Interpreter to PyCharm pointing interpreter path to ``~/.pyenv/versions/airflow-env/bin/python``, which is virtual
  environment ``airflow-env`` created with pyenv earlier. For adding an Interpreter go to ``File -> Setting -> Project:
  airflow -> Python Interpreter``.

  .. raw:: html

    <div align="center" style="padding-bottom:10px">
      <img src="images/pycharm_add_interpreter.png"
           alt="Adding existing interpreter">
    </div>

- In PyCharm IDE open Airflow project, directory ``/files/dags`` of local machine is by default mounted to docker
  machine when breeze Airflow is started. So any Dag file present in this directory will be picked automatically by
  scheduler running in docker machine and same can be seen on ``http://127.0.0.1:28080``.

- Copy any example Dag present in the ``/airflow/example_dags`` directory to ``/files/dags/``.

- Add a ``__main__`` block at the end of your Dag file to make it runnable:

  .. code-block:: python

    if __name__ == "__main__":
        dag.test()

- Run the file.

Creating a branch
#################

1. Click on the branch symbol in the status bar

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/pycharm_creating_branch_1.png"
             alt="Creating a new branch">
      </div>

2. Give a name to a branch and checkout

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/pycharm_creating_branch_2.png"
             alt="Giving a name to a branch">
      </div>

Follow the `Quick start <../03b_contributors_quick_start_seasoned_developers.rst>`_ for typical development tasks.
