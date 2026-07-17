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

Using an externally synchronized checkout (git-sync)
====================================================

``GitDagBundle`` fetches the repository itself: on every refresh, the dag processor runs
``git fetch`` and resolves the bundle's Airflow Connection. For large repositories that are
parsed frequently, or when you would rather keep git credentials out of Airflow entirely, use
the :class:`~airflow.providers.git.bundles.gitsync.GitSyncDagBundle` together with an external
synchronization process -- typically the Kubernetes `git-sync
<https://github.com/kubernetes/git-sync>`_ sidecar. The sidecar is solely responsible for
pulling the repository; Airflow only reads the files.

git-sync publishes each synced commit as a separate worktree directory and atomically swaps a
symlink to point at the current one. ``GitSyncDagBundle`` resolves that symlink on each
refresh and pins the resolved worktree until the next refresh, so a sync happening mid-parse
cannot mix files from two different commits into one parsing round.

**JSON format example**:

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
     {
         "name": "my-git-repo",
         "classpath": "airflow.providers.git.bundles.gitsync.GitSyncDagBundle",
         "kwargs": {
             "sync_path": "/git/repo",
             "subdir": "dags"
         }
     }
    ]'

where ``/git/repo`` is the git-sync ``<--root>/<--link>`` path.

.. note::

    * Run git-sync with ``--stale-worktree-timeout`` set to a value comfortably larger than
      your longest parse loop, so the previous worktree remains readable while a parse that
      pinned it is still in flight.
    * This bundle does not support versioning: the checkout only ever contains the current
      state, so task runs always use the latest bundle state.
    * Every component that reads the bundle (dag processor, workers, triggerer) must have
      the synced path available locally, e.g. by running the git-sync sidecar in each pod
      or mounting a shared volume.
