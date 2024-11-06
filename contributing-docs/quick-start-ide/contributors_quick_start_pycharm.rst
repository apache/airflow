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

3. Configure the source root directories for providers as well as for task_sdk.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/pycharm_adding_source_root_directories.png"
             alt="Adding Source Root directories to Pycharm">
      </div>

4. Once step 3 is done it is recommended to invalidate caches and restart Pycharm.

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

2. Debugging an example DAG

- Add Interpreter to PyCharm pointing interpreter path to ``~/.pyenv/versions/airflow-env/bin/python``, which is virtual
  environment ``airflow-env`` created with pyenv earlier. For adding an Interpreter go to ``File -> Setting -> Project:
  airflow -> Python Interpreter``.

  .. raw:: html

    <div align="center" style="padding-bottom:10px">
      <img src="images/pycharm_add_interpreter.png"
           alt="Adding existing interpreter">
    </div>

- In PyCharm IDE open airflow project, directory ``/files/dags`` of local machine is by default mounted to docker
  machine when breeze airflow is started. So any DAG file present in this directory will be picked automatically by
  scheduler running in docker machine and same can be seen on ``http://127.0.0.1:28080``.

- Copy any example DAG present in the ``/airflow/example_dags`` directory to ``/files/dags/``.

- Add a ``__main__`` block at the end of your DAG file to make it runnable:

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

Follow the `Quick start <../03_contributors_quick_start.rst>`_ for typical development tasks.
