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

Branches move as new commits are pushed, which is useful when combined with ``refresh_interval`` to
pick up new changes automatically without any restart. Tags and commit SHAs point to a fixed commit
(assuming tags are not moved), so they're useful for pinning a bundle to known-good code.

When ``tracking_ref`` is a commit SHA, new commits pushed to the repository are not picked up by
``refresh_interval``, since a SHA always refers to the same commit. Promoting or rolling back to a
different SHA means changing the ``tracking_ref`` value itself, which is an ``[dag_processor]
dag_bundle_config_list`` config change like any other: it takes effect only after the Dag processor
and workers are restarted to reload the configuration. This is unlike moving a branch/tag ref, where
the config is unchanged and the existing ``refresh_interval`` polling picks up the new commit with no
restart required.

This is also distinct from Dag bundle versioning. Each Dag run already records the commit its bundle
resolved to, so it can be rerun with that exact code regardless of the bundle's current
``tracking_ref`` (see :ref:`rerun_with_latest_version <config:core__rerun_with_latest_version>`).
That only affects individual reruns; it does not change which commit new Dag parses and runs use.
Promoting or rolling back the active environment still requires changing ``tracking_ref`` itself.
