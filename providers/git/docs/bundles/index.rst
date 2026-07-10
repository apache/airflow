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

Bundles
#######

Use the :class:`~airflow.providers.git.bundles.git.GitDagBundle` to configure a Git bundle in your Airflow's
``[dag_processor] dag_bundle_config_list``.

Example of using the GitDagBundle:

**JSON format example**:

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
     {
         "name": "my-git-repo",
         "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
         "kwargs": {
             "subdir": "dags",
             "tracking_ref": "main",
             "refresh_interval": 3600,
             "submodules": false,
             "prune_dotgit_folder": true,
             "sparse_dirs": ["dags", "includes"]
         }
     }
    ]'

``tracking_ref`` accepts a branch, tag, or full commit SHA. Setting it to a commit SHA pins the
bundle to that exact commit:

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
     {
         "name": "my-git-repo",
         "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
         "kwargs": {
             "repo_url": "https://github.com/org/repo.git",
             "tracking_ref": "a3d1850dd1aa1919a61620aa39f202185c9321c0",
             "subdir": "dags"
         }
     }
    ]'

Branches move as new commits are pushed, so combined with ``refresh_interval`` they pick up new code
without a restart. Tags and commit SHAs are static (assuming tags aren't moved), pinning the bundle
to known-good code — but promoting or rolling back a SHA means changing ``tracking_ref`` in
``dag_bundle_config_list`` itself, which requires restarting the Dag processor and workers to take
effect.
