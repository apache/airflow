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


Setup and develop using GitHub Codespaces
#########################################

1. Go to |airflow_github| and fork the project.

   .. |airflow_github| raw:: html

     <a href="https://github.com/apache/airflow/" target="_blank">https://github.com/apache/airflow/</a>

   .. raw:: html

     <div align="center" style="padding-bottom:10px">
       <img src="images/airflow_fork.png"
            alt="Forking Apache Airflow project">
     </div>

2. From your fork create a codespace by clicking this
   👉 |codespace|

   .. |codespace| image:: https://github.com/codespaces/badge.svg
       :target: https://github.com/codespaces/new?hide_repo_select=true&ref=main&repo=33884891
       :alt: Open in GitHub Codespaces

3. Once the codespace starts your terminal should already be in the Airflow
   devcontainer/Breeze environment and you should be able to edit and run the
   tests in VS Code interface.

   If your prompt starts with ``[Breeze:...]``, you are already inside the
   containerized development environment. Do not run ``breeze start-airflow`` from
   that prompt. ``breeze start-airflow`` starts Docker containers and is intended
   to be run from the outer host shell with access to the Docker socket.

4. You can use `Quick start guide for Visual Studio Code <contributors_quick_start_vscode.rst>`_ for details
   as Codespaces use Visual Studio Code as interface.


Troubleshooting Docker in Codespaces
-------------------------------------

If you see a "Docker is not running" error when running Breeze commands, first
check where the command is being run. In Codespaces the terminal is already
inside the Airflow devcontainer/Breeze environment. Commands that start another
set of containers, such as ``breeze start-airflow``, should be run outside that
container context.

1. Verify that Docker is accessible by running:

   .. code-block:: bash

      docker info

   If the output reports that the Docker client API version is too new for the
   server, set the API version reported by the server and retry the command, for
   example:

   .. code-block:: bash

      export DOCKER_API_VERSION=1.43
      docker info

2. If the command fails because Docker cannot connect to the daemon, check that
   the Docker socket exists:

   .. code-block:: bash

      ls -la /var/run/docker.sock

3. Check the active Docker context and socket permissions:

   .. code-block:: bash

      docker context ls
      groups $USER
      ls -l /var/run/docker.sock

   You should see ``docker`` in the group list. If not, add yourself to the group:

   .. code-block:: bash

      sudo usermod -aG docker $USER

4. If the above steps do not help, rebuild the devcontainer
   (Command Palette → *Codespaces: Rebuild Container*) or restart the Codespace
   from the GitHub Codespaces dashboard.


Follow the `Quick start <../03b_contributors_quick_start_seasoned_developers.rst>`_ for typical development tasks.
