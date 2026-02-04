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

A Dag bundle is a collection of one or more Dags, files along with their associated files, such as other
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

**airflow.providers.git.bundles.git.GitDagBundle**
    These bundles integrate with Git repositories, allowing Airflow to fetch Dags directly from a repository.

**airflow.providers.amazon.aws.bundles.s3.S3DagBundle**
    These bundles reference an S3 bucket containing Dag files. They do not support versioning of the bundle, meaning tasks always run using the latest code.

**airflow.providers.google.cloud.bundles.gcs.GCSDagBundle**
    These bundles reference a GCS bucket containing Dag files. They do not support versioning of the bundle, meaning tasks always run using the latest code.

Configuring Dag bundles
-----------------------

Dag bundles are configured in :ref:`config:dag_processor__dag_bundle_config_list`. You can add one or more Dag bundles here.

By default, Airflow adds a local Dag bundle, which is the same as the old Dags folder. This is done for backwards compatibility, and you can remove it if you do not want to use it. You can also keep it and add other Dag bundles, such as a git Dag bundle.

For example, adding multiple Dag bundles to your ``airflow.cfg`` file:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
          "name": "my_git_repo",
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

Starting Airflow 3.0.2 git is pre installed in the base image. However, if you are using versions prior 3.0.2, you would need to install git in your docker image.

.. code-block:: Dockerfile

  RUN apt-get update && apt-get install -y git
  ENV GIT_PYTHON_GIT_EXECUTABLE=/usr/bin/git
  ENV GIT_PYTHON_REFRESH=quiet


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


Configuring Default Bundle Version Behavior
--------------------------------------------

When a user clears a DAG run or task instance, the UI shows a checkbox asking whether to rerun
with the latest bundle version or with the version the original run used. The
``run_on_latest_version`` setting controls the default state of that checkbox, so teams don't
have to make that decision manually every time.

.. note::

    This only applies to versioned bundle types (like ``GitDagBundle``). Local bundles
    (``LocalDagBundle``) do not support versioning and will always use the latest code.

How It Works
~~~~~~~~~~~~

Each DAG has a **parsed version** (``DagModel.bundle_version``), updated every time the scheduler
re-parses the DAG file. Separately, each bundle tracks a **latest version**
(``DagBundleModel.version``), updated when the bundle detects a new version (e.g. a new Git commit).

When ``run_on_latest_version`` is **False** (the default), reruns use the same bundle version as the
original run and new scheduled runs use the parsed version. This provides reproducibility when
debugging failures. When **True**, runs use the latest bundle version from the bundle, ensuring the
most recent code is always used.

The setting is resolved using the following precedence (highest to lowest):

1. **DAG-level**: The DAG's ``run_on_latest_version`` parameter (if ``True`` or ``False``)
2. **Global config**: The ``[core] run_on_latest_version`` option (if set)
3. **Default**: ``False``

Global Configuration
~~~~~~~~~~~~~~~~~~~~

Set organization-wide defaults using the ``[core] run_on_latest_version`` option. This applies to
all DAGs that don't explicitly override it at the DAG level.

.. code-block:: ini

    [core]
    run_on_latest_version = False  # Default - rerun with the original bundle version
    # run_on_latest_version = True  # Rerun with the latest bundle version

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
        run_on_latest_version=True,
        start_date=datetime(2024, 1, 1),
    ) as dag1:
        EmptyOperator(task_id="task")

    # Always rerun with the same version as the original run
    with DAG(
        dag_id="pinned_version_dag",
        run_on_latest_version=False,
        start_date=datetime(2024, 1, 1),
    ) as dag2:
        EmptyOperator(task_id="task")

    # Inherit from global configuration (default if omitted)
    with DAG(
        dag_id="default_behavior_dag",
        start_date=datetime(2024, 1, 1),
    ) as dag3:
        EmptyOperator(task_id="task")

Use Cases
~~~~~~~~~

**Debugging failed runs**:
    With ``False`` (the default), clearing a failed run reruns it with the same code, making it
    easier to reproduce and isolate issues.

**Always run latest code**:
    Set ``[core] run_on_latest_version = True`` if your team prefers reruns to always pick up the
    latest code, for example when bug fixes have been deployed since the original run.

**Mixed policy**:
    Set the global default to ``True`` but override specific critical DAGs with
    ``run_on_latest_version=False`` for version stability where it matters most.

Relationship with ``disable_bundle_versioning``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Airflow provides two separate settings that affect bundle versioning behavior.
They serve different purposes:

``disable_bundle_versioning``
    Turns off version tracking entirely. When set to ``True``, no ``bundle_version`` is
    recorded on DAG runs. Use this when versioning adds no value, for example local
    development with ``LocalDagBundle`` or pipelines where reproducibility is not a concern.
    Available as a DAG parameter and as a global config option
    (``[dag_processor] disable_bundle_versioning``).

``run_on_latest_version``
    Controls the default *rerun behavior* while keeping version tracking active. When a user
    clears or reruns a task, this determines whether the new run uses the latest bundle
    version or the original version. Versioning remains enabled so the version history is
    still recorded. This only changes the default choice presented to users.

In short: ``disable_bundle_versioning`` answers "should we track versions at all?", while
``run_on_latest_version`` answers "when rerunning, which version should be the default?".
The two settings are independent. ``run_on_latest_version`` has no effect when versioning
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

**view_url**
    This method should return a URL as a string to view the bundle on an external system (e.g., a Git repository's web interface).

Other Considerations
~~~~~~~~~~~~~~~~~~~~

- **Versioning**: If your bundle supports versioning, ensure that ``initialize``, ``get_current_version`` and ``refresh`` are implemented to handle version-specific logic.

- **Concurrency**: Workers may create many bundles simultaneously, and does nothing to serialize calls to the bundle objects. Thus, the bundle class must handle locking if
  that is problematic for the underlying technology. For example, if you are cloning a git repo, the bundle class is responsible for locking to ensure only 1 bundle
  object is cloning at a time. There is a ``lock`` method in the base class that can be used for this purpose, if necessary.

- **Triggerer Limitation**: DAG bundles are not initialized in the triggerer component. In practice, this means that triggers cannot come from a DAG bundle.
  This is because the triggerer does not deal with changes in trigger code over time, as everything happens in the main process.
  Triggers can come from anywhere else on ``sys.path`` instead. If you need to use custom triggers, ensure they are available in the Python environment's
  ``sys.path`` rather than being sourced from DAG bundles.
