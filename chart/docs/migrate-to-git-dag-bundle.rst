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

.. _git_sync_to_git_dag_bundle:

Migrating from Git-Sync to GitDagBundle
=======================================

This guide is for users of the Git-Sync sidecar (``dags.gitSync``) in Helm Chart 1.x.
The Git-Sync sidecar is planned for removal in Helm Chart 2.0 in favor of the
Airflow-native :class:`~airflow.providers.git.bundles.git.GitDagBundle`.

What is changing
----------------

Airflow components pods keep a shared DAG folder up to date from a git repository.

With ``GitDagBundle``, syncing moves into Airflow itself. The Dag Processor clones
the repository once into a local bare repository and refreshes it every
``refresh_interval`` seconds. Task execution resolves DAG code from versioned
bundles, so workers always run the code version the Dag Processor recorded. No
sidecar containers and no shared ``ReadWriteMany`` volume are needed for syncing.

Differences to be aware of before migrating:

* Git-Sync synced a single repository per deployment (multiple repositories required
  an "umbrella" repository with submodules). ``dagProcessor.dagBundleConfigList``
  accepts multiple bundles, so each repository simply becomes its own bundle.
* ``GitDagBundle`` supports versioning: changing a SHA-pinned ``tracking_ref`` is a configuration change and
  only takes effect once the Dag Processor restarts and reloads its configuration.
  Promoting to a *new* SHA can also require the bundle's local storage to be
  cleared first (for example by letting the pod be recreated) — see
  `GH-71388 <https://github.com/apache/airflow/issues/71388>`_.
* ``dags.persistence`` was often enabled only to give Git-Sync a shared volume. It
  is not needed for ``GitDagBundle`` and can be disabled if nothing else uses it.

Prerequisites
-------------

* Latest Helm Chart 1.x release with Airflow 3 (DAG bundles are an Airflow 3 feature).
* The ``apache-airflow-providers-git`` provider package must be installed in the
  image you deploy, along with the ``git`` binary (the ``git`` binary is
  pre-installed in the official ``apache/airflow`` images starting from Airflow
  3.0.2). If you build a custom slim image, add the provider to it.

Value mapping
-------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Git-Sync (``dags.gitSync.*``)
     - GitDagBundle (``dagProcessor.dagBundleConfigList[].kwargs``)
   * - ``repo`` — the clone URL
     - ``repo_url``. Alternatively, store the repository URL in the ``host`` field
       of a ``git`` connection and reference it with ``git_conn_id``.
   * - ``branch`` (git-sync v3) / ``ref`` (git-sync v4) — branch, tag or hash to
       check out
     - ``tracking_ref`` — accepts a branch, tag or full commit SHA.
   * - ``rev`` (git-sync v3)
     - ``tracking_ref``. To follow the tip of a branch, set ``tracking_ref`` to
       the branch name.
   * - ``subPath`` — subdirectory with DAGs
     - ``subdir``
   * - ``period`` — sync interval as a Go duration string (for example ``"5s"``)
     - ``refresh_interval`` — sync interval in seconds (integer, for example
       ``300``)
   * - ``credentialsSecret`` (``GITSYNC_USERNAME``, ``GIT_SYNC_USERNAME``, ``GITSYNC_PASSWORD``, ``GIT_SYNC_PASSWORD``)
     - ``git_conn_id`` — an Airflow ``git`` connection whose login/password hold
       the username and access token. See :ref:`git_sync_to_git_dag_bundle:auth`
       below.
   * - ``sshKeySecret`` / ``sshKey`` (``gitSshKey``)
     - ``git_conn_id`` — a ``git`` connection with the SSH private key in the
       ``private_key`` extra (inline key) or ``key_file`` extra (path to a key
       file).
   * - ``knownHosts``
     - ``git_conn_id`` — a ``git`` connection with the ``known_hosts_file`` and
       ``strict_host_key_checking`` extras.
   * - ``depth``, ``maxFailures``, probes, ``resources``, ``env``
     - No equivalent. There are no sidecar containers to tune: ``GitDagBundle``
       keeps one full bare clone and fetches from it, and refresh failures are
       retried internally.

.. _git_sync_to_git_dag_bundle:auth:

Authentication
--------------

``GitDagBundle`` authenticates through a standard Airflow connection of type
``git``, selected via the bundle's ``git_conn_id`` kwarg (i.e. the
``dagProcessor.dagBundleConfigList[].kwargs.git_conn_id`` value), or needs no
connection at all for public repositories when ``repo_url`` is given directly.

For a private repository over HTTPS with a personal access token, create the
connection from a Kubernetes secret managed by the chart:

.. code-block:: yaml
   :caption: override-values.yaml

   extraSecrets:
     '{{ .Release.Name }}-git-connections':
       stringData:
         # The connection id is "git_default" (env var name is upper-cased).
         # login/password become the token credentials; repo_url in the bundle
         # kwargs selects the repository.
         AIRFLOW_CONN_GIT_DEFAULT: 'git://oauth2:<your-token>@github.com/your-org/dags-repo.git'

Then mount it into the pods that need it (the Dag Processor at minimum):

.. code-block:: yaml
   :caption: override-values.yaml

   dagProcessor:
     extraEnv:
       - name: AIRFLOW_CONN_GIT_DEFAULT
         valueFrom:
           secretKeyRef:
             name: '{{ .Release.Name }}-git-connections'
             key: AIRFLOW_CONN_GIT_DEFAULT

.. note::

   The connection ``host`` field is ignored when ``repo_url`` is set in the
   bundle kwargs — only the credentials from the connection are used. For the
   full list of connection extras (SSH keys, known hosts, proxy, GitHub App
   auth), see :doc:`apache-airflow-providers-git:bundles/index`.

   Instead of injecting the connection through ``extraSecrets``/``extraEnv``,
   you can create the same ``git`` connection directly in the Airflow UI
   (**Admin** → **Connections**) or with ``airflow connections add`` — the Dag
   Processor resolves any connection with a matching conn id from the metadata
   database.

Step-by-step migration
----------------------

1. Translate your ``dags.gitSync`` settings using the mapping table above and add
   a bundle to ``dagProcessor.dagBundleConfigList``:

   .. code-block:: yaml
      :caption: override-values.yaml

      dagProcessor:
        dagBundleConfigList:
          - name: dags-repo
            classpath: "airflow.providers.git.bundles.git.GitDagBundle"
            kwargs:
              repo_url: "https://github.com/your-org/dags-repo.git"
              tracking_ref: "main"     # was dags.gitSync.branch / ref
              subdir: "dags"           # was dags.gitSync.subPath
              refresh_interval: 300    # was dags.gitSync.period: "5s"
              git_conn_id: "git_default"  # only needed for private repos

   The chart renders this list into
   ``config.dag_processor.dag_bundle_config_list`` automatically.

2. If nothing else uses the shared DAG volume, disable the pieces Git-Sync
   needed:

   .. code-block:: yaml
      :caption: override-values.yaml

      dags:
        persistence:
          enabled: false
        gitSync:
          enabled: false

   Keep the default ``dags-folder`` ``LocalDagBundle`` in the bundle list only if
   you still bake DAGs into the image; otherwise remove it.

3. Upgrade the release:

   .. code-block:: bash

      helm upgrade --install airflow apache-airflow/airflow \
        --namespace airflow -f override-values.yaml

4. Verify the bundle is picked up. The Dag Processor log should show the bundle
   being configured and cloned; in the UI, DAGs appear with the bundle name
   (``dags-repo``) as their source. Trigger a test DAG run to confirm tasks
   resolve code from the bundle.

Multiple repositories
---------------------

Unlike Git-Sync, which supported a single repository, you can declare one bundle
per repository:

.. code-block:: yaml
   :caption: override-values.yaml

   dagProcessor:
     dagBundleConfigList:
       - name: team-a-dags
         classpath: "airflow.providers.git.bundles.git.GitDagBundle"
         kwargs:
           repo_url: "https://github.com/your-org/team-a-dags.git"
           tracking_ref: "main"
           subdir: "dags"
           refresh_interval: 300
       - name: team-b-dags
         classpath: "airflow.providers.git.bundles.git.GitDagBundle"
         kwargs:
           repo_url: "https://github.com/your-org/team-b-dags.git"
           tracking_ref: "main"
           subdir: "dags"
           refresh_interval: 300

For the complete ``GitDagBundle`` reference, see
:doc:`apache-airflow-providers-git:bundles/index`.
