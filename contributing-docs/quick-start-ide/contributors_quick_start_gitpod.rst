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

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Connect your project to Gitpod
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Goto |airflow_github| and fork the project.

   .. |airflow_github| raw:: html

     <a href="https://github.com/apache/airflow/" target="_blank">https://github.com/apache/airflow/</a>

   .. raw:: html

     <div align="center" style="padding-bottom:10px">
       <img src="images/airflow_fork.png"
            alt="Forking Apache Airflow project">
     </div>

2. Goto your github account's fork of airflow click on ``Code`` and copy the clone link.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/airflow_clone.png"
             alt="Cloning github fork of Apache airflow">
      </div>

3. Add goto https://gitpod.io/#<copied-url> as shown.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/airflow_gitpod_url.png"
             alt="Open personal airflow clone with Gitpod">
      </div>

Set up Breeze in Gitpod
~~~~~~~~~~~~~~~~~~~~~~~

Gitpod default image have all the required packages installed.

1. Run ``pipx install -e ./dev/breeze`` to install Breeze

.. warning::

  If you see below warning - it means that you hit `known issue <https://github.com/pypa/pipx/issues/1092>`_
  with ``packaging`` version 23.2:
  ⚠️ Ignoring ``--editable install`` option. ``pipx`` disallows it for anything but a local path,
  to avoid having to create a new ``src/`` directory.

  The workaround is to downgrade packaging to 23.1 and re-running the ``pipx install`` command. for example
  by running ``pip install "packaging<23.2"``.

  .. code-block:: bash

     pip install "packaging==23.1"
     pipx install -e ./dev/breeze --force

2. Run ``breeze`` to enter breeze in Gitpod.

Setting up database in Breeze
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once you enter breeze environment is initialized, create airflow tables and users from the breeze CLI.
The ``airflow db reset`` command is required to execute at least once for Airflow Breeze to
get the database/tables created. When you run the tests, your database will be initialized automatically
the first time you run tests.

.. note::

   This step is needed when you would like to run/use webserver.

.. code-block:: bash

  root@b76fcb399bb6:/opt/airflow# airflow db reset
  root@b76fcb399bb6:/opt/airflow# airflow users create --role Admin --username admin --password admin \
    --email admin@example.com --firstname foo --lastname bar

Follow the `Quick start <../03_contributors_quick_start.rst>`_ for typical development tasks.
