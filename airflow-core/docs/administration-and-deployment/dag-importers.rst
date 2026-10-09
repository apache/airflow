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

.. _dag-importers:

Dag Importers
=============

|experimental|

.. versionadded:: 3.4.0

.. warning::

    The Dag importer interface may still change in a minor release while it is being stabilized. If you
    maintain a custom importer, check the release notes when you upgrade.

A Dag importer turns the contents of a :doc:`Dag bundle <dag-bundles>` into Dags. Importers let Airflow
load Dags from formats other than Python files, such as YAML or JSON pipeline definitions, without changes
to the Dag processor, the scheduler, or the workers.

Airflow ships with two importers:

**airflow.sdk.importers.PythonDagImporter** (``.py`` and ``.pyc``)
    Loads a Python file as a module and collects the Dags it defines, as described in
    :ref:`concepts-dag-loading`.

**airflow.sdk.importers.ZipImporter** (``.zip``)
    Imports each Python file in a zip archive, as described in :ref:`concepts-packaging-dags`.

Each :doc:`language SDK <../authoring-and-scheduling/language-sdks/index>` with a configured coordinator
also registers an importer for its bundle format.

How Airflow uses importers
--------------------------

An importer handles a Dag definition: a unit of Dag source, such as a Python file or a member of a zip
archive.

* The Dag processor asks each importer to list the Dag definitions it can handle in a bundle, and queues the
  files that hold them. A file holding several definitions, such as a zip archive, is parsed as one.
* A parsing process imports each definition with the importer that listed it. The resulting Dags go through
  the same validation and :doc:`cluster policies <cluster-policies>` as Dags defined in Python, and the
  importer's errors are shown as import errors.
* The source code the importer returns for a definition is shown in the **Code** tab of the UI.
* A worker uses the same importer to load the Dag before it runs a task.

Language SDK importers work differently: the Dag processor runs the SDK runtime to parse their files instead of
calling the importer, and a worker runs their tasks only on a queue routed to a coordinator. See
:doc:`../authoring-and-scheduling/language-sdks/index`.

A Dag definition must be a file in the bundle, or nested in one, as a zip member is. The Dag processor
ignores any other definition with a warning.

Configuring importers
---------------------

Register importers in :ref:`config:dag_processor__dag_importer_configs`. Each entry supplies the
``classpath`` of the importer and, optionally, the ``extensions`` it handles and the ``kwargs`` to create it
with:

.. code-block:: ini

    [dag_processor]
    dag_importer_configs = [
        {
          "classpath": "my_company.importers.YamlDagImporter",
          "extensions": [".yaml", ".yml"]
        }
      ]

Without ``extensions``, the importer handles the extensions its class declares. An entry with only a
classpath can be given as a string, for example ``["my_company.importers.YamlDagImporter"]``.

To register an importer for one bundle only, add an ``importers`` key, in the same format, to that bundle's
entry in :ref:`config:dag_processor__dag_bundle_config_list`:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
          "name": "pipelines",
          "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
          "kwargs": {"path": "/opt/airflow/pipelines"},
          "importers": ["my_company.importers.YamlDagImporter"]
        }
      ]

Each extension is handled by one importer. Importers are registered in this order, and a later importer
takes over an extension from an earlier one, with a warning in the logs:

#. the built-in importers;
#. the importers of the language SDK coordinators;
#. ``dag_importer_configs``;
#. the bundle's ``importers``.

This lets you replace a built-in importer, for example to handle ``.py`` files differently. However, replacing the
``.py`` importer does not change how ``.py`` members of zip archives are imported: ``ZipImporter`` imports them
with its own importers, which its ``internal_importers`` kwarg sets.

.. important::

    Workers import Dags with the same importers as the Dag processor. Install the package that provides your
    importer, and set the same importer configuration, on the Dag processor, on the workers, and anywhere
    else Dags are parsed, such as where you run ``airflow dags`` commands.

Writing a custom importer
-------------------------

An importer subclasses :class:`~airflow.sdk.importers.AbstractDagImporter`, typed with the class of Dag
definition it handles, and implements:

``can_handle(definition)``
    Whether the importer can import a definition, a path, or a file name.
``list_dag_definitions(bundle, *, safe_mode)``
    Yield the Dag definitions in the bundle that the importer handles.
``import_definition(definition, bundle)``
    Import the Dags of one definition and return them in a :class:`~airflow.sdk.importers.DagImportResult`.
``get_source_code(definition, dag_id=None)``
    Return the source of a definition and its language, as a :class:`~airflow.sdk.importers.DagSourceCode`.
    ``dag_id`` names the Dag whose source is wanted when a definition holds several; an importer that cannot
    tell them apart ignores it.

For file-based formats, :func:`~airflow.sdk.importers.find_file_dag_definitions` walks the bundle the same
way the built-in importers do: it applies :ref:`.airflowignore <concepts:airflowignore>` and yields a
:class:`~airflow.sdk.importers.FilesystemDagDefinition` for each file with one of the given extensions.

Keep the following in mind when you write an importer:

* Report a definition that fails to import as a :class:`~airflow.sdk.importers.DagImportError` in the
  result instead of raising, so it is shown as an import error. In ``list_dag_definitions``, yield a
  ``DagImportError`` for a file that cannot be read, so the rest of the bundle is still listed.
* Rely only on ``bundle.name`` and ``bundle.path``. Only the Dag processor's bundle listing passes the
  configured bundle; parsing processes, workers and the CLI pass a ``LocalDagBundle`` built from its name and
  path. When they list definitions, ``bundle.path`` is the single file being parsed;
  ``find_file_dag_definitions`` handles both a directory and a file.
* ``list_dag_definitions`` runs in the Dag processor's main loop, with no timeout, each time a bundle is
  refreshed, so keep it cheap: a slow listing delays the parsing of every bundle. Only the import of a
  definition, in a parsing process, is bounded by :ref:`config:dag_processor__dag_file_processor_timeout`.
  ``dagbag_import_timeout`` applies to the built-in Python importer and to language SDK parsing, not to
  custom importers.
* Declare ``supported_extensions`` as an attribute: Airflow replaces it with the ``extensions`` of the
  importer's configuration entry.
* ``safe_mode`` is the value of :ref:`config:core__dag_discovery_safe_mode`, except on a worker, which always
  passes ``False``. To skip files that clearly hold no Dag before a parsing process is started for them,
  override :meth:`~airflow.sdk.importers.AbstractDagImporter.might_contain_dag` and call it in
  ``list_dag_definitions``.
* Airflow creates one importer instance per bundle and reuses it for every definition. The Dag processor
  creates it before it forks the parsing processes, so keep ``__init__`` cheap and do not open connections or
  start threads there.

For a definition nested in a file, such as a member of an archive, subclass
:class:`~airflow.sdk.importers.FileDagDefinition`. See the :doc:`Task SDK API reference <task-sdk:api>` for
the full interface.
