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

Your First Airflow Pull Request — 15-Minute Guide
===============================================

.. contents:: On this page
   :local:
   :depth: 1


Purpose
-------
This page walks **new contributors** through opening their first
Apache Airflow pull request (PR) in about five minutes.  We present *one*
local option (Breeze) and *one* fully-hosted option (GitHub Codespaces).
Everything else lives in the advanced guides.

Prerequisites
-------------
* GitHub account
* `Fork <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo?tool=webui>`_ `apache/airflow <https://github.com/apache/airflow>`__
* `Basic Git <https://docs.github.com/en/get-started/git-basics/set-up-git>`__ (**only** required for the Breeze path below)

For Breeze (local development):

* `Docker Desktop <https://www.docker.com/products/docker-desktop/>`__
* `Podman <https://podman.io/>`__, a drop-in, license-friendly replacement for Docker Desktop
* `Docker Compose <https://docs.docker.com/compose/install/>`__
* `uv <https://github.com/astral-sh/uv>`__, which is a fast, reliable package manager that you'll use to install other developer tools to make contributing to Airflow easier.

.. code-block:: bash

    curl -LsSf https://astral.sh/uv/install.sh | sh
* `Prek <https://github.com/j178/prek>`__, which runs Airflow's required code-quality checks (formatting, linting, and bug-spotting) before you commit, helping save contributors and committers time during the pull request process.

.. code-block:: bash

    uv tool install prek
    prek install -f
    prek install -f --hook-type pre-push

* 4GB RAM, 40GB disk space, and at least 2 CPU cores

.. note::
   Docker **or Podman** installation varies by OS. See the `full guide <03_contributors_quick_start.rst#local-machine-development>`_ for Ubuntu, macOS, and Windows instructions.

Option A – Breeze on Your Laptop
--------------------------------
1.  **Clone your fork and install Breeze**

.. code-block:: bash

    git clone https://github.com/<you>/airflow.git
    cd airflow
    uv tool install -e ./dev/breeze

2. Setup your idea workspace to detect project src/ and tests/ folders as source roots.

.. code-block:: text

    # For IntelliJ IDEA and PyCharm
    uv run setup_idea.py

    # For VS Code
    uv run setup_vscode.py

3.  **Start the development container** (first run builds the image)

.. code-block:: bash

    breeze start-airflow

The command starts a shell and launches multiple terminals using tmux
and launches all Airflow necessary components in those terminals. To know more about tmux commands,
check out this cheat sheet: https://tmuxcheatsheet.com/. To exit breeze, type ``stop_airflow`` in any
of the tmux panes and hit Enter.

**Working with DAGs in Breeze:**

- **Adding your own DAGs**: Place your DAG files in the ``/files/dags/`` directory in your local Airflow repository. This directory is automatically mounted into the Breeze container and your DAGs will be visible in the Airflow UI.

- **Loading example DAGs**: Use the ``--load-example-dags`` flag to load all example DAGs from the repository:

.. code-block:: bash

    breeze start-airflow --load-example-dags

This flag enables configuration to load example DAGs when starting Airflow, which is useful for exploring Airflow's capabilities and testing.

4.  **Make a tiny change** – e.g. fix a typo in docs

5.  **Run local checks**

.. code-block:: bash

    prek --all-files

6.  **Commit & push**

.. code-block:: bash

    git checkout -b docs-typo
    git commit -am "fix typo in README"
    git push -u origin docs-typo

7.  **Open the PR** – GitHub shows a "Compare & pull request" button.

*Syncing your branch*

.. code-block:: bash

    git fetch upstream && git rebase upstream/main && git push --force-with-lease

Option B – One-Click GitHub Codespaces
---------------------------------------

1. On **your fork**, click *Code → Codespaces → New codespace*.
2. Wait for the VS Code web IDE to appear.  A terminal opens automatically.
3. Install Docker Buildx and Docker Compose (required for Breeze)

.. code-block:: bash

    mkdir -p ~/.docker/cli-plugins

    # Install Docker Buildx
    BUILDX_VERSION=v0.16.2
    curl -SL "https://github.com/docker/buildx/releases/download/${BUILDX_VERSION}/buildx-${BUILDX_VERSION}.linux-amd64" -o ~/.docker/cli-plugins/docker-buildx
    chmod +x ~/.docker/cli-plugins/docker-buildx
    docker buildx version

    # Install Docker Compose v2
    curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m)" -o ~/.docker/cli-plugins/docker-compose
    chmod +x ~/.docker/cli-plugins/docker-compose
    docker compose version

4. Install Breeze and start the development container

.. code-block:: bash

      curl -LsSf https://astral.sh/uv/install.sh | sh
      uv tool install prek
      prek install -f
      prek install -f --hook-type pre-push # for running mypy checks when pushing to repo
      uv tool install -e ./dev/breeze
      uv run setup_vscode.py
      breeze start-airflow

5. Edit a file in the editor, save, and commit via the Source Control sidebar.
   Push when prompted.

6. Press **Create pull request** when GitHub offers.



Review & Merge
--------------
Respond to reviewer comments, push updates (same commands as above).  Once
CI is green and reviews are ✅, a committer will merge.  🎉

Next Steps
----------
* Need a full development environment? See the `Development Environments Guide <https://github.com/apache/airflow/blob/main/contributing-docs/06_development_environments.rst>`_.
* Learn about our contribution workflow? Checkout the `Contribution Workflow Guide <https://github.com/apache/airflow/blob/main/contributing-docs/18_contribution_workflow.rst>`_.
