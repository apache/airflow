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

Dag Bundles
===========

A Dag bundle is a collection of one or more Dag files along with their associated files, such as other
Python scripts, configuration files, or other resources. Dag bundles can source the Dags from various
locations, such as local directories, Git repositories, or other external systems. Deployment administrators
can also write their own Dag bundle classes to support custom sources. You can also define more than one Dag
bundle in an Airflow deployments, allowing for better organization of your Dags. By keeping the bundle at a
higher level, it allows for versioning everything the Dag needs to run.

This is similar, but more powerful than the *Dags folder* in Airflow 2 or earlier, where Dags were required to
be in one place on the local disk, and getting the Dags there was solely the responsibility of the deployment
manager.

Since Dag bundles support versioning, they also allow Airflow to run a task using a specific version of the
Dag bundle, allowing for a Dag run to use the same code for the whole run, even if the Dag is updated mid-way
through the run.

Why are Dag bundles important?
------------------------------

- **Version Control**: By supporting versioning, Dag bundles allow Dag runs to use the same code for the whole run, even if the Dag is updated mid way through the run.
- **Scalability**: With Dag bundles, Airflow can efficiently manage large numbers of Dags by organizing them into logical units.
- **Flexibility**: Dag bundles enable seamless integration with external systems, such as Git repositories, to source Dags.

Types of Dag bundles
--------------------
Airflow supports multiple types of Dag Bundles, each catering to specific use cases:

**airflow.dag_processing.bundles.local.LocalDagBundle**
    These bundles reference a local directory containing Dag files. They are ideal for development and testing environments, but do not support versioning of the bundle, meaning tasks always run using the latest code.

**airflow.dag_processing.bundles.manifest_local.ManifestLocalDagBundle**
    These bundles serve Dag files published from a local directory as immutable, content-addressed releases on a
    shared filesystem. They support versioning without requiring Git. See :ref:`manifest-local-dag-bundles`.

**airflow.providers.git.bundles.git.GitDagBundle**
    These bundles integrate with Git repositories, allowing Airflow to fetch Dags directly from a repository. The `GitDagBundle` does support versioning.

**airflow.providers.amazon.aws.bundles.s3.S3DagBundle**
    These bundles reference an S3 bucket containing Dag files. They do not support versioning of the bundle, meaning tasks always run using the latest code.

**airflow.providers.google.cloud.bundles.gcs.GCSDagBundle**
    These bundles reference a GCS bucket containing Dag files. They do not support versioning of the bundle, meaning tasks always run using the latest code.

**airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle**
    These bundles reference an Azure Blob Storage container containing Dag files. They do not support versioning of the bundle, meaning tasks always run using the latest code.

Configuring Dag bundles
-----------------------

Dag bundles are configured in :ref:`config:dag_processor__dag_bundle_config_list`. You can add one or more Dag bundles here.

.. warning:: Reference credentials through a Connection — do not inline them

    Bundle ``kwargs`` are stored in the ``[dag_processor] dag_bundle_config_list``
    configuration, which Airflow exposes through the Config API when
    :ref:`config:api__expose_config` is enabled. Any user authorized to read the
    configuration can read these values verbatim, so they must not contain secrets.

    Do **not** embed credentials directly in bundle ``kwargs`` — for example a
    token placed directly in a ``repo_url`` like
    ``https://x-access-token:<token>@github.com/org/repo.git``. Instead reference
    an Airflow :doc:`Connection <../authoring-and-scheduling/connections>`
    (``git_conn_id`` for Git, ``aws_conn_id`` for S3, ``gcp_conn_id`` for GCS) and
    keep the credential in your
    :doc:`secrets backend <../security/secrets/secrets-backend/index>`. Connection
    fields are resolved at runtime and are not written into
    ``dag_bundle_config_list``.


By default, Airflow adds a ``LocalDagBundle`` pointing at the configured Dags folder, maintaining the same behaviour as Airflow 2's Dags folder. The only kwarg is ``path``, which defaults to the value of :ref:`config:core__dags_folder` when omitted:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
          "name": "dags-folder",
          "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
          "kwargs": {
            "path": "/opt/airflow/dags"
          }
        }
      ]

.. note::

    ``LocalDagBundle`` does not support versioning. Tasks always run against the latest code on disk.

For a Git Dag bundle, the only required kwarg is ``tracking_ref`` (a branch, tag, or commit SHA). Use ``git_conn_id`` to reference an Airflow connection that holds the repository credentials, or supply ``repo_url`` directly. You can also narrow the checkout to a subdirectory with ``subdir``, or use ``sparse_dirs`` to enable a sparse checkout of specific directories:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
          "name": "my-git-repo",
          "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
          "kwargs": {
            "git_conn_id": "my_git_conn",
            "subdir": "dags",
            "tracking_ref": "main",
          }
        }
      ]

.. note::

    ``GitDagBundle`` supports versioning. Each Dag run records the Git commit it was created with, allowing reruns to use the exact same code even if the repository has since been updated.

See :doc:`apache-airflow-providers-git:bundles/index` for the full list of kwargs and more examples.

For an S3 Dag bundle, the required kwarg is ``bucket_name``. You can optionally set ``aws_conn_id`` (defaults to ``aws_default``) and ``prefix`` to scope the bundle to a subdirectory within the bucket:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
          "name": "my-s3-dags",
          "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
          "kwargs": {
            "aws_conn_id": "aws_default",
            "bucket_name": "my-airflow-bucket",
            "prefix": "dags/"
          }
        }
      ]

.. note::

    ``S3DagBundle`` does not support versioning. Tasks always run against the latest code in the bucket.

See :doc:`apache-airflow-providers-amazon:bundles/index` for the full list of kwargs and more examples.

For a GCS Dag bundle, the required kwarg is ``bucket_name``. You can optionally set ``gcp_conn_id`` (defaults to ``google_cloud_default``) and ``prefix`` to scope the bundle to a subdirectory within the bucket:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
          "name": "my-gcs-dags",
          "classpath": "airflow.providers.google.cloud.bundles.gcs.GCSDagBundle",
          "kwargs": {
            "gcp_conn_id": "google_cloud_default",
            "bucket_name": "my-airflow-bucket",
            "prefix": "dags/"
          }
        }
      ]

.. note::

    ``GCSDagBundle`` does not support versioning. Tasks always run against the latest code in the bucket.

See :doc:`apache-airflow-providers-google:bundles/index` for the full list of kwargs and more examples.

You can combine multiple bundle types in a single deployment. The default ``LocalDagBundle`` can be removed if you no longer need it, or kept alongside other bundles:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
          "name": "my_git_bundle",
          "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
          "kwargs": {"tracking_ref": "main", "git_conn_id": "my_git_conn"}
        },
        {
          "name": "dags-folder",
          "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
          "kwargs": {}
        }
      ]

.. note::

    The whitespace, particularly on the last line, is important so a multi-line value works properly. More details can be found in the
    the `configparser docs <https://docs.python.org/3/library/configparser.html#supported-ini-file-structure>`_.

If you want a view url different from the default provided by the Dag bundle, you can change the url in the kwargs of the Dag bundle configuration.
For example, if you want to use a custom URL for the git Dag bundle:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
          "name": "my_git_repo",
          "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
          "kwargs": {
            "tracking_ref": "main",
            "git_conn_id": "my_git_conn",
            "view_url_template": "https://my.custom.git.repo/view/{subdir}",
          }
        }
      ]

Above, the ``view_url_template`` is set to a custom URL that will be used to view the Dags in the ``my_git_repo`` bundle. The ``{subdir}`` placeholder will be replaced
with the ``subdir`` attribute of the bundle. The placeholders are attributes of the bundle. You cannot use any placeholder outside of the bundle's attributes.
When you specify a custom URL, it overrides the default URL provided by the Dag bundle.

The url is verified for safety, and if it is not safe, the view url for the bundle will be set to ``None``. This is to prevent any potential security issues with unsafe URLs.

You can also override the :ref:`config:dag_processor__refresh_interval` per Dag bundle by passing it in kwargs.
This controls how often the Dag processor refreshes, or looks for new files, in the Dag bundles.

Starting with Airflow 3.0.2, git is pre-installed in the base image. However, if you are using versions prior to 3.0.2, you would need to install git in your docker image.

.. code-block:: Dockerfile

  RUN apt-get update && apt-get install -y git
  ENV GIT_PYTHON_GIT_EXECUTABLE=/usr/bin/git
  ENV GIT_PYTHON_REFRESH=quiet

.. _manifest-local-dag-bundles:

Manifest-backed local Dag bundles
---------------------------------

A ``LocalDagBundle`` cannot tell which files a retry or rerun used, because the files can change after the Dag
run is created. ``ManifestLocalDagBundle`` gives a local or shared-filesystem deployment reproducible, pinned
execution without Git, much as ``GitDagBundle`` does with commits:

- A deployment pipeline **publishes** a release with ``airflow dag-bundles publish-local``. The command copies
  the source directory into an immutable snapshot and updates the bundle's release reference as its last step.
- The **version** is ``sha256-<hex>``, a hash of every included file's relative path, SHA-256 digest, size and
  executable flag. Publishing the same content again gives the same version.
- Airflow **materializes** a verified, read-only copy of the snapshot into its bundle cache and parses and runs
  only that copy, never the source directory.
- Retries and reruns **pin** the version recorded on the Dag run, unless bundle versioning is disabled (see
  `Relationship with disable_bundle_versioning`_). If the cache copy was removed, Airflow rebuilds it from the
  published snapshot.

Configure the bundle with the shared ``published_root`` that publishers write to and that Dag processors and
workers read from:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
          "name": "my-dags",
          "classpath": "airflow.dag_processing.bundles.manifest_local.ManifestLocalDagBundle",
          "kwargs": {
            "published_root": "/shared/dag-releases",
            "refresh_interval": 30
          }
        }
      ]

``published_root`` is required and must be a filesystem path. ``allow_empty_source`` (default ``false``) lets
a publication contain no files at all; it is off so that a sync that delivered nothing cannot empty the
release. It does not catch a partial sync that delivered only some files.

The Dag processor cannot initialize the bundle until its first release is published. Until then it logs the
missing release reference and retries on later loops; other bundles are not affected.

Publishing a release
~~~~~~~~~~~~~~~~~~~~

After the deployment pipeline has delivered every source file, publish them:

.. code-block:: bash

    airflow dag-bundles publish-local my-dags /path/to/dag-source

The command reads the named bundle from ``dag_bundle_config_list``, so the publishing host needs that
bundle's configuration. It imports only that bundle's class, so the packages of other configured bundles need
not be installed there. It does not use the metadata database, so it can run on a CI/CD host that has no
database access, as long as that host can read the source directory and write to ``published_root``.

The source directory, ``published_root`` and :ref:`config:dag_processor__dag_bundle_storage_path` must not
overlap. ``.git`` and ``__pycache__`` directories, files named ``.git``, ``.pyc`` files and any
``.airflow-bundle-manifest.json`` file are left out of the release. A symlink or other non-regular file
anywhere else in the source makes the publication fail.

The command prints the bundle name, the version, the snapshot and release reference paths, the manifest hash,
the file count, the total size, and whether this publication created the snapshot (``False`` when the same
content was published before). Use ``--output json`` to read the result in a script:

.. code-block:: bash

    version=$(airflow dag-bundles publish-local my-dags /path/to/dag-source --output json | jq -r '.[0].version')

As in other ``airflow`` commands that print JSON, every value is a string; ``created_snapshot`` is ``"True"``
or ``"False"``, so compare it as a string. Do not add ``--verbose`` when parsing the output, because it lets
log lines through to standard output.

A deployment pipeline whose runs can finish out of order can stop an older run from replacing a newer release.
Pass the version that the run expects to replace:

.. code-block:: bash

    airflow dag-bundles publish-local my-dags /path/to/dag-source \
        --expected-current-version sha256-<current-hex>

The command then publishes only if that version is still the current release. Omit the option for the first
release, when there is no current version to compare with.

A malformed ``--expected-current-version`` value, such as an empty string from an unset pipeline variable, is
rejected before anything is written.

When publication fails, for example because of a stale ``--expected-current-version``, a missing or empty
source, overlapping paths, an invalid bundle configuration, or a filesystem error, the command prints the
reason on standard error and exits with status 1. A failure before replacing the release reference leaves
that reference unchanged. Add ``--verbose`` to see the full traceback instead.

The final directory sync can fail **after** the release reference has been replaced. In this case, the
command reports ``Cannot confirm publication`` and exits with status 1. The new release is already visible,
but the filesystem has not confirmed that the reference change will survive a crash. Airflow does not roll
the reference back, because other components may already have observed the new release.

Before retrying, investigate the storage error and inspect the ``version`` in the release reference file
named in the error (``<published_root>/refs/<bundle>/latest.json``). Another publisher may have advanced it
again. Reading this file identifies the currently visible release; it does not confirm crash durability.
Decide which release to publish, and pass the version currently recorded in the reference as
``--expected-current-version`` to guard against another publisher changing it in the meantime. Reusing the
old expected version fails if the reference has already advanced.

Deployment behavior
~~~~~~~~~~~~~~~~~~~

Publication is atomic: Airflow keeps serving the previous release until the release reference moves, which
happens only after the snapshot is complete and verified. Concurrent publishers of the same bundle take turns
through a lock under ``published_root``. Publishing content that is already published reuses its snapshot and
only moves the release reference.

Each Dag processor picks up a new release on its next refresh of the bundle, every ``refresh_interval``
seconds: the bundle's ``refresh_interval`` kwarg if set, otherwise
:ref:`config:dag_processor__refresh_interval`. Pinned work -- retries, reruns that keep their original
version, deferred tasks and callbacks -- uses the snapshot for the recorded version, never the current release,
and never publishes.

Airflow never deletes snapshots from ``published_root``. Pruning old ``versions/<bundle>/sha256-*`` directories
is up to you:

- Keep the current release (the ``version`` in ``refs/<bundle>/latest.json``) and every version still recorded
  on a Dag run that can be retried or rerun, or that has deferred tasks or callbacks pending. Pinned work for a
  removed version fails until the snapshot is restored.
- Prune while holding the bundle's publication lock, for example with ``flock _locks/<bundle>.lock``, so that a
  publisher cannot reuse a snapshot while it is being removed.
- Snapshots are read-only: make one writable (``chmod -R u+w``) before removing it. A removal that stops
  part-way leaves a damaged snapshot, and publishing the same content again then fails until the rest of it is
  removed.

The bundle cache on each host is disposable. Airflow rebuilds it from ``published_root`` when it is missing, so
you can delete it while no Airflow component on that host is using it.

Storage layout and permissions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The publisher creates this layout below ``published_root``:

.. code-block:: text

    versions/my-dags/sha256-<hex>/           immutable snapshot
        <Dag files>
        .airflow-bundle-manifest.json        snapshot manifest
    refs/my-dags/latest.json                 current release reference
    _locks/my-dags.lock                      publication lock

Airflow materializes the copy it parses and runs below
``<dag_bundle_storage_path>/my-dags/versions/sha256-<hex>/``. A ``.sha256-<hex>.validated`` marker next to
each copy lets later checks skip re-hashing its files. Deleting the marker makes the next process on that host
that loads the copy re-hash it.

Published snapshot directories are mode ``0555`` and their files ``0444``, or ``0555`` when executable, so
publishers and Airflow components can run as different OS users. In the cache, files stay read-only while
directories are ``0755`` so that Airflow's stale-version cleanup can remove them. Airflow components need only
read access to ``published_root``.

Publishers need write access to the bundle's ``versions/<bundle>``, ``refs/<bundle>`` and ``_locks``
directories and read/write access to its lock file. A publisher creates any of these directories that are
missing with mode ``0755``, which only its own OS user can write to. When publishers run as different OS users,
create the directories in advance, owned by a group the publishers share, with group write access and the
setgid bit (``chmod 2775``); the lock file is created group-writable.

``published_root`` must be one filesystem shared by every publisher and Airflow component that serves the
bundle. It must support POSIX advisory file locking (``flock``), atomic rename, and durable ``fsync`` of files
and directories. A disk local to one host works only when every publisher and Airflow component that serves the
bundle runs on that host. Use a network filesystem only if it keeps those guarantees across hosts.

Verifying a deployment
~~~~~~~~~~~~~~~~~~~~~~

After a Dag processor refreshes the bundle, the version shown for it on the **Dag Bundles** page and by
``GET /api/v2/dagBundles`` (see `Monitoring Dag bundles`_) matches the ``version`` printed by the publish
command, and Dag versions recorded from that release carry it as their bundle version.

To check that a release parses without waiting for a Dag processor, list its Dags from any host that has the
bundle configuration, a migrated metadata database, and write access to its own
:ref:`config:dag_processor__dag_bundle_storage_path`:

.. code-block:: bash

    airflow dags list --local --bundle-name my-dags

``--local`` parses the materialized cache copy of the current release, not the source directory.


Monitoring Dag bundles
----------------------

The **Dag Bundles** page under *Browse* shows, for each bundle, the version Airflow currently holds,
when a Dag processor last refreshed it, its import-error count, and whether it is still configured
-- so you can tell whether a commit has been picked up without reading the Dag source. The page
refreshes itself while open, and the same data is available at ``GET /api/v2/dagBundles`` for a
deployment pipeline to poll.

.. note::

   Under the FAB auth manager the *Browse* entry needs a ``apache-airflow-providers-fab`` release
   that maps the new menu item onto the ``DAGs`` resource; upgrade to the latest one if the entry is
   missing. On an older release the lookup falls back to a FAB resource named after the menu item,
   no such resource exists, and the entry is hidden for every role including ``Admin``. The page is
   still reachable at ``/dag_bundles`` and the API is unaffected, so a missing *Browse* entry means
   the provider needs upgrading, not that the feature is off.

``version`` is whatever the bundle reports; for a Git Dag bundle it is the commit SHA of the tracking
ref. Bundles that do not support versioning, such as ``LocalDagBundle``, report ``null``.

``last_refreshed`` is the last *successful* refresh and advances whether or not the version changed.
The cadence is the bundle's :ref:`config:dag_processor__refresh_interval`, checked every
:ref:`config:dag_processor__bundle_refresh_check_interval`.

Some caveats:

* A failing refresh looks like one that is not due, since a refresh that raises is logged and leaves
  ``last_refreshed`` at its last success. Check the Dag processor logs, and
  ``/api/v2/jobs?job_type=DagProcessorJob`` for a live processor.
* A new version does not mean the new Dags are running, because bundles are refreshed before their
  Dags are parsed.
* The version and the import-error count together cannot confirm that a particular commit parsed,
  so do not treat the pair as a deployment-health check. Import errors are not recorded against a
  version, a file whose parse exceeds
  :ref:`config:dag_processor__dag_file_processor_timeout` is killed without writing an import-error
  row at all, and the count only covers errors you are allowed to read. A count of zero therefore
  means "nothing I can show you", not "this commit is healthy". To tie a running Dag to a commit,
  read the Dag's version in ``/api/v2/dags/{dag_id}/dagVersions``.
* With several Dag processors the row reflects whichever refreshed last, as they share one row per
  bundle.
* A bundle is listed for users who can read at least one Dag recorded against it. A bundle holding
  no registered Dag at all -- a first deploy whose only file fails to import, for instance -- has no
  Dag to authorize against, so it is listed only for users who may read import errors for files with
  no registered Dag (``Admin`` by default under the FAB auth manager).

``active`` is false when the bundle was missing from the configuration of whatever last ran a full
sync, which also means processors that disagree about ``dag_bundle_config_list`` will deactivate
each other's bundles. An inactive bundle keeps its last version and timestamp, frozen rather than
current.

Using DAG Bundles with User Impersonation
-----------------------------------------

When using ``run_as_user`` (user impersonation) with DAG bundles, ensure proper file permissions
are configured so that impersonated users can access bundle files created by the main Airflow process.

1. All impersonated users and the Airflow user should be in the same group
2. Configure appropriate umask settings (e.g., ``umask 0002``)


.. note::

    This permission-based approach is a temporary solution. Future versions of Airflow
    will handle multi-user access through supervisor-based bundle operations, eliminating
    the need for shared group permissions.


Configuring Default Rerun Version Behavior
------------------------------------------

When a user clears a DAG run or task instance, the UI shows a checkbox asking whether to rerun
with the latest bundle version or with the version the original run used. The
``rerun_with_latest_version`` setting controls the default state of that checkbox, so teams don't
have to make that decision manually every time. The same setting also governs the default
``run_on_latest_version`` behavior when creating backfills via the API or CLI.

.. note::

    This only applies to versioned bundle types (like ``GitDagBundle`` and ``ManifestLocalDagBundle``).
    Local bundles (``LocalDagBundle``) do not support versioning and will always use the latest code.

How It Works
~~~~~~~~~~~~

Each DAG has a **parsed version** (``DagModel.bundle_version``), updated every time the dag
processor re-parses the DAG file. Each DAG run records the bundle version it was created with.

When ``rerun_with_latest_version`` is **False**, clearing a DAG run preserves its
original bundle version, so the rerun uses the same code. This provides reproducibility when
debugging failures. When **True**, clearing updates the DAG run to the current parsed version,
ensuring the most recent code is used on rerun.

The setting is resolved using the following precedence (highest to lowest):

1. **Explicit request**: The ``run_on_latest_version`` parameter in the API request body (if provided)
2. **DAG-level**: The DAG's ``rerun_with_latest_version`` parameter (if ``True`` or ``False``)
3. **Global config**: The ``[core] rerun_with_latest_version`` option (if set)
4. **Per-call-site fallback**: ``False`` for clear/rerun, ``True`` for backfills (preserving
   the historical default for each path)

One exception: a Dag run with no version of its own — carried over from Airflow 2, or its version
since removed by ``airflow db clean`` — has nothing to preserve, so clearing it always uses the
latest version and bundle version regardless of the resolved setting.

Global Configuration
~~~~~~~~~~~~~~~~~~~~

Set organization-wide defaults using the ``[core] rerun_with_latest_version`` option:

.. code-block:: ini

    [core]
    rerun_with_latest_version = False  # Rerun with the original bundle version
    # rerun_with_latest_version = True  # Rerun with the latest bundle version

When unset, the call site applies the historical fallback (``False`` for clear/rerun,
``True`` for backfills).

DAG-Level Configuration
~~~~~~~~~~~~~~~~~~~~~~~

Override the global default for specific DAGs:

.. code-block:: python

    from datetime import datetime

    from airflow import DAG
    from airflow.operators.empty import EmptyOperator

    # Always rerun with the latest version
    with DAG(
        dag_id="always_latest_dag",
        rerun_with_latest_version=True,
        start_date=datetime(2024, 1, 1),
    ) as dag:
        EmptyOperator(task_id="task")

Use Cases
~~~~~~~~~

**Debugging failed runs**:
    With ``False`` (the default), clearing a failed run reruns it with the same code, making it
    easier to reproduce and isolate issues.

**Always run latest code**:
    Set ``[core] rerun_with_latest_version = True`` if your team prefers reruns to always pick up the
    latest code, for example when bug fixes have been deployed since the original run.

**Mixed policy**:
    Set the global default to ``True`` but override specific critical DAGs with
    ``rerun_with_latest_version=False`` for version stability where it matters most.

Relationship with ``disable_bundle_versioning``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Airflow provides two separate settings that affect bundle versioning behavior.
They serve different purposes:

``disable_bundle_versioning``
    Turns off version tracking entirely. When set to ``True``, no ``bundle_version`` is
    recorded on DAG runs. Available as a DAG parameter and as a global config option
    (``[dag_processor] disable_bundle_versioning``).

``rerun_with_latest_version``
    Controls the default *rerun behavior* while keeping version tracking active. When a user
    clears or reruns a task, this determines whether the new run uses the latest bundle
    version or the original version. Versioning remains enabled so the version history is
    still recorded. This only changes the default choice presented to users.

In short: ``disable_bundle_versioning`` answers "should we track versions at all?", while
``rerun_with_latest_version`` answers "when rerunning, which version should be the default?".
The two settings are independent. ``rerun_with_latest_version`` has no effect when versioning
is disabled.


Writing custom Dag bundles
--------------------------

When implementing your own Dag bundle by extending the ``BaseDagBundle`` class, there are several methods you must implement. Below is a guide to help you implement a custom Dag bundle.

Abstract Methods
~~~~~~~~~~~~~~~~
The following methods are abstract and must be implemented in your custom bundle class:

**path**
    This property should return a ``Path`` to the directory where the Dag files for this bundle are stored.
    Airflow uses this property to locate the Dag files for processing.

**get_current_version**
    This method should return the current version of the bundle as a string.
    Airflow will use pass this version to ``__init__`` later to get this version of the bundle again when it runs tasks.
    If versioning is not supported, it should return ``None``.

**refresh**
    This method should handle refreshing the bundle's contents from its source (e.g., pulling the latest changes from a remote repository).
    This is used by the Dag processor periodically to ensure that the bundle is up-to-date.

Optional Methods
~~~~~~~~~~~~~~~~
In addition to the abstract methods, you may choose to override the following methods to customize the behavior of your bundle:

**__init__**
    This method can be extended to initialize the bundle with extra parameters, such as ``tracking_ref`` for the ``GitDagBundle``.
    It should also call the parent class's ``__init__`` method to ensure proper initialization.
    Expensive operations, such as network calls, should be avoided in this method to prevent delays during the bundle's instantiation, and done
    in the ``initialize`` method instead.

**initialize**
    This method is called before the bundle is first used in the Dag processor or worker. It allows you to perform expensive operations only when the bundle's content is accessed.

    When a worker starts a task, Airflow sets the Dag parsing context before calling ``initialize()``. See **Parsing context** under Other Considerations.

**view_url**
    This method should return a URL as a string to view the bundle on an external system (e.g., a Git repository's web interface).

Other Considerations
~~~~~~~~~~~~~~~~~~~~

- **Versioning**: If your bundle supports versioning, ensure that ``initialize``, ``get_current_version`` and ``refresh`` are implemented to handle version-specific logic.

- **Parsing context**: If expensive bundle setup can be scoped to a single Dag (for example, a sparse checkout of only the paths that Dag needs), worker task startup can do less work in ``initialize()``.
  Call :py:func:`~airflow.sdk.get_parsing_context` inside ``initialize()`` and branch on ``dag_id``.

  Airflow sets ``dag_id`` and ``task_id`` only on the worker task startup path (``startup()`` wraps ``parse()``, which calls ``initialize()``).
  The Dag processor, CLI (for example ``airflow dags reserialize`` or ``dag report``), callbacks, and other call sites invoke ``initialize()`` without setting this parsing context, so custom bundles should prepare the full bundle on those paths.
  ``refresh()`` is not invoked with Dag-specific parsing context either; use it to update the entire bundle, not for per-Dag preparation.

  .. code-block:: python

      from airflow.sdk import get_parsing_context


      class MyBundle(BaseDagBundle):
          def initialize(self):
              dag_id = get_parsing_context().dag_id
              if dag_id:
                  # Worker task startup only: prepare just this Dag (e.g. sparse checkout)
                  ...
              else:
                  # Dag processor, CLI, callbacks, and other paths: prepare the full bundle
                  ...

- **Concurrency**: Workers may create many bundles simultaneously, and Airflow does nothing to serialize calls to the bundle objects. Thus, the bundle class must handle locking if
  that is problematic for the underlying technology. For example, if you are cloning a git repo, the bundle class is responsible for locking to ensure only 1 bundle
  object is cloning at a time. There is a ``lock`` method in the base class that can be used for this purpose, if necessary.

- **Triggerer Limitation**: DAG bundles are not initialized in the triggerer component. In practice, this means that triggers cannot come from a DAG bundle.
  This is because the triggerer does not deal with changes in trigger code over time, as everything happens in the main process.
  Triggers can come from anywhere else on ``sys.path`` instead. If you need to use custom triggers, ensure they are available in the Python environment's
  ``sys.path`` rather than being sourced from DAG bundles.
