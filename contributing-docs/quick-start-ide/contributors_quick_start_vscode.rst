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

.. contents:: :local:

Setup your project
##################

1. Open your IDE or source code editor and select the option to clone the repository

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/vscode_clone.png"
             alt="Cloning github fork to Visual Studio Code">
      </div>


2. Paste the copied clone link in the URL field and submit.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/vscode_click_on_clone.png"
             alt="Cloning github fork to Visual Studio Code">
      </div>


Setting up debugging
####################

1. Configuring Airflow database connection

- Airflow is by default configured to use SQLite database. Configuration can be seen on local machine
  ``~/airflow/airflow.cfg`` under ``sql_alchemy_conn``.

- Installing required dependency for MySQL connection in ``airflow-env`` on local machine.

  .. code-block:: bash

    $ pyenv activate airflow-env
    $ pip install PyMySQL

- Now set ``sql_alchemy_conn = mysql+pymysql://root:@127.0.0.1:23306/airflow?charset=utf8mb4`` in file
  ``~/airflow/airflow.cfg`` on local machine.

1. Debugging an example DAG

- In Visual Studio Code open airflow project, directory ``/files/dags`` of local machine is by default mounted to docker
  machine when breeze airflow is started. So any DAG file present in this directory will be picked automatically by
  scheduler running in docker machine and same can be seen on ``http://127.0.0.1:28080``.

- Copy any example DAG present in the ``/airflow/example_dags`` directory to ``/files/dags/``.

- Add a ``__main__`` block at the end of your DAG file to make it runnable. It will run a ``back_fill`` job:

  .. code-block:: python


    if __name__ == "__main__":
        dag.clear()
        dag.run()

- Add ``"AIRFLOW__CORE__EXECUTOR": "DebugExecutor"`` to the ``"env"`` field of Debug configuration.

  - Using the ``Run`` view click on ``Create a launch.json file``

    .. raw:: html

        <div align="center" style="padding-bottom:10px">
          <img src="images/quick_start/vscode_add_configuration_1.png"
               alt="Add Debug Configuration to Visual Studio Code">
          <img src="images/quick_start/vscode_add_configuration_2.png"
               alt="Add Debug Configuration to Visual Studio Code">
          <img src="images/quick_start/vscode_add_configuration_3.png"
               alt="Add Debug Configuration to Visual Studio Code">
        </div>

  - Change ``"program"`` to point to an example dag and add ``"env"`` and ``"python"`` fields to the new Python configuration

    .. code-block:: json

     {
         "configurations": [
             "program": "${workspaceFolder}/files/dags/example_bash_operator.py",
             "env": {
                 "PYTHONUNBUFFERED": "1",
                 "AIRFLOW__CORE__EXECUTOR": "DebugExecutor"
              },
              "python": "${env:HOME}/.pyenv/versions/airflow/bin/python"
         ]
     }

    .. raw:: html

        <div align="center" style="padding-bottom:10px">
          <img src="images/quick_start/vscode_add_env_variable.png"
               alt="Add environment variable to Visual Studio Code Debug configuration">
        </div>

- Now Debug an example dag and view the entries in tables such as ``dag_run, xcom`` etc in mysql workbench.

Creating a branch
#################

1. Click on the branch symbol in the status bar

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/vscode_creating_branch_1.png"
             alt="Creating a new branch">
      </div>

2. Give a name to a branch and checkout

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/vscode_creating_branch_2.png"
             alt="Giving a name to a branch">
      </div>

Follow the `Quick start <03_contributors_quick_start.rst>`_ for typical development tasks.
