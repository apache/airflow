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

Your First Airflow Pull Request — 5-Minute Guide
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
* Fork `apache/airflow <https://github.com/apache/airflow>`__
* Basic Git installed **only** for the Breeze path below

For Breeze (local development):
* Docker Community Edition
* Docker Compose
* UV for Python tool management::

    curl -LsSf https://astral.sh/uv/install.sh | sh
* 4GB RAM, 40GB disk space, and at least 2 CPU cores

.. note::
   Docker installation varies by OS. See the `full guide <03b_contributors_quick_start_seasoned_developers.html#local-machine-development>`_ for Ubuntu, macOS, and Windows instructions.

Option A – Breeze on Your Laptop
--------------------------------
1.  **Clone your fork and install Breeze**::

        git clone https://github.com/<you>/airflow.git
        cd airflow
        uv tool install -e ./dev/breeze

2.  **Start the development container** (first run builds the image)::

        breeze start-airflow

3.  **Make a tiny change** – e.g. fix a typo in docs::

        sed -i '' 's/Airflow/Airflow®/' contributing-docs/README.rst

4.  **Run local checks**::

        prek --all-files

5.  **Commit & push**::

        git checkout -b docs-typo
        git commit -am "fix typo in README"
        git push -u origin docs-typo

6.  **Open the PR** – GitHub shows a "Compare & pull request" button.

*Syncing your branch*::

    git fetch upstream && git rebase upstream/main && git push --force-with-lease

Option B – One-Click GitHub Codespaces
-------------------------------------
1. On **your fork**, click *Code → Codespaces → New codespace*.
2. Wait for the VS Code web IDE to appear.  A terminal opens automatically.
3. If you want to test an Airflow functionality, best way is to install Breeze within Codespaces and start the development container::

        breeze start-airflow

4. Edit a file in the editor, save, and commit via the Source Control
   sidebar.  Push when prompted.
5. Press **Create pull request** when GitHub offers.

Review & Merge
--------------
Respond to reviewer comments, push updates (same commands as above).  Once
CI is green and reviews are ✅, a committer will merge.  🎉

Next Steps
----------
* Need a full development environment?  See
  :doc:`03b_contributors_quick_start_seasoned_developers`.
* Learn about our contribution workflow:
  :doc:`04_how_to_contribute`.
