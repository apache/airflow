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
to known-good code — but changing a SHA-pinned ``tracking_ref`` is a ``dag_bundle_config_list``
config change, not a ref move, so it only takes effect once the Dag processor is restarted and
reloads the configuration. If ``[dag_processor] disable_bundle_versioning`` (or the
``disable_bundle_versioning`` Dag parameter) is set, workers also resolve code from their own
``tracking_ref`` rather than a recorded bundle version, so they need the updated configuration too.

.. note::

    Rolling back a SHA-pinned ``tracking_ref`` after a restart is reliable, since the commit's
    objects are already present in the bundle's local storage. Promoting to a *new* SHA can fail
    to check out that commit unless the bundle's local storage is cleared first (for example, a
    fresh pod, or manually deleting the bundle's directory) — see
    `GH-71388 <https://github.com/apache/airflow/issues/71388>`_ for the underlying limitation.

Skipping the fetch on start-up
==============================

``initialize()`` fetches from the remote every time the bundle is set up in a process. Where the
bundle storage is a volume shared with a component that already keeps it fresh — ephemeral worker
pods mounting the same volume the Dag processor refreshes — that fetch is pure start-up latency.
``refresh_on_initialize: false`` makes such a component reuse whatever is already on disk:

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
     {
         "name": "my-git-repo",
         "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
         "kwargs": {
             "repo_url": "https://github.com/org/repo.git",
             "tracking_ref": "main",
             "subdir": "dags",
             "refresh_on_initialize": false
         }
     }
    ]'

The repository is still cloned when it is not present on disk, and an explicit ``refresh()`` still
fetches, so a Dag processor configured this way keeps picking up new commits on its
``refresh_interval``.

.. warning::

    Set this only on components that do **not** own the freshness of the bundle, and only where
    they read storage some other component refreshes. A deployment whose workers each get their own
    empty volume — the default for ``KubernetesExecutor`` without a shared ``ReadWriteMany``
    volume — gains nothing, since there is never an existing repo to reuse. A deployment where
    nothing else refreshes the storage will serve stale code indefinitely.

    It is also only worth setting where workers resolve code from ``tracking_ref`` rather than from
    a recorded bundle version, i.e. when ``[dag_processor] disable_bundle_versioning`` (or the
    Dag-level ``disable_bundle_versioning``) is set. Version-pinned bundles already reuse the
    on-disk repo without fetching.
