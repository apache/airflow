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

***********************************
Contributor's Quick Start - Pull Request flow
***********************************

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Syncing remote fork and rebasing a pull request
-----------------------------------------------

It often takes several days or weeks to discuss and iterate on a pull request (PR) until it is ready to merge.
Dozens of people actively contribute Airflow, so the ``main`` branch is constantly changing. As a result,
you might run into `merge conflicts <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/addressing-merge-conflicts/about-merge-conflicts>`_.
To keep merge conflicts to a minimum, you should frequently sync the branch that you're working on in your
fork with the ``apache/airflow`` main and rebase your PR on top of it. Following
describes how to do it. The `guide on working with git <10_working_with_git.rst#how-to-rebase-pr>`__ has more
detailed instructions, but here's a quick tl;dr:

For the first time only, you'll need to add the apache remote. If you connect to GitHub via https, run:

.. code-block:: bash

    git remote add apache git@github.com:apache/airflow.git

If you connect to GitHub via SSH, run:

.. code-block:: bash

    git remote add apache https://github.com/apache/airflow.git

If you're not on your branch, check out your branch (replace "your-branch" with the name of your branch):

.. code-block:: bash

    git checkout your-branch

.. code-block:: bash

    git fetch apache
    HASH=$(git merge-base your-branch apache/main)
    git rebase $HASH --onto apache/main

If there are no merge conflicts, you're good to go!

.. code-block:: bash

    git push origin your-branch --force-with-lease

Otherwise, work through the merge conflicts before going through this process again.

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
