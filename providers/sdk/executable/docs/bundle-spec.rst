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

Bundle Spec Format
==================

This document specifies the on-disk format of a build artifact produced by an
Airflow native-executable SDK (Go, Rust, C++, Zig, ...) and consumed by
:class:`~airflow.providers.sdk.executable.coordinator.ExecutableCoordinator`
at deployment time.

The goal is a single, language-agnostic *bundle* shape so that scheduler,
worker, and UI behave identically regardless of which compiled SDK produced
the DAG.

Format version: ``1.0``.

Container
---------

A bundle is a **ZIP archive**. Each DAG file is built into exactly one ZIP,
and the ZIP is the deployment unit. The runtime reads the archive in place
and is **not required to unpack** it ahead of time.

The recommended file extension is ``.zip``. There is no fixed naming
convention; consumers identify a bundle by inspecting the archive's
``airflow-metadata.yaml``.

Required Members
----------------

A conforming bundle MUST contain the following entries at the archive root:

``airflow-metadata.yaml``
    Build-time manifest declaring ``dag_id``\ s and ``task_id``\ s exposed by
    this bundle, and the locations of the source and executable files within
    the archive. See :ref:`bundle-metadata-schema` below.

A primary DAG source file
    The original DAG definition source file, included verbatim. Used by the
    Airflow UI for source display and by operators for debugging. The file
    name is flexible and is recorded in the manifest's ``source`` field.

    Examples: ``example.go``, ``main.rs``, ``pipeline.cpp``, ``tasks.zig``.

A compiled executable
    The native, runnable artifact for the target deployment platform. It
    MUST honour the SDK coordinator protocol (``--comm=<addr>`` /
    ``--logs=<addr>`` socket-based IPC). The file name is flexible and is
    recorded in the manifest's ``executable`` field.

    The archive entry SHOULD preserve the executable bit via the ZIP
    external-attributes field. Runtimes that cannot rely on it MUST set
    the bit (``chmod +x``) after extraction.

A bundle MAY embed additional source files, dependency libraries, debug
symbols, or other resources.

.. _bundle-metadata-schema:

``airflow-metadata.yaml`` schema
--------------------------------

Produced at build time from a static scan of the DAG source.

A machine-readable JSON Schema is published at
:download:`airflow-metadata.schema.json` for use by build tooling, validators,
and editors.

.. code-block:: yaml

    format_version: "1.0"
    sdk:
      language: go
      version: "0.1.0"
    source: example.go
    executable: example
    dags:
      example_dag:
        tasks:
          - extract
          - transform
          - load
      another_dag:
        tasks:
          - run

Top-level keys:

``format_version`` (string, required)
    The bundle-spec version this manifest conforms to. Currently ``"1.0"``.

``sdk`` (mapping, required)
    Identifies the SDK that produced the bundle.

    - ``language`` (string, required): lower-case source-language identifier
      (e.g. ``go``, ``rust``, ``cpp``, ``zig``).
    - ``version`` (string, required): SDK version used at build time.

``source`` (string, required)
    Archive-relative path to the primary DAG source file. The Airflow UI
    uses this entry to render the DAG source view.

``executable`` (string, required)
    Archive-relative path to the compiled executable.

``dags`` (mapping, required)
    Mapping of ``dag_id`` to a *DAG entry*. Every ``dag_id`` the bundle
    exposes MUST appear here. The scanner uses these keys to match a DAG
    parsing or task-execution request to the bundle that owns it.

DAG entry fields:

``tasks`` (list of strings, required)
    Static list of ``task_id``\ s declared in the DAG. Empty lists are
    permitted but discouraged.

Unrecognised top-level or DAG-entry keys MUST be ignored by the consumer so
that future SDK versions can extend the manifest without breaking older
runtimes.

Examples
--------

Go bundle::

    example.zip
    ├── airflow-metadata.yaml   (source: example.go, executable: example)
    ├── example.go
    └── example                 (compiled executable)

Rust bundle::

    pipeline.zip
    ├── airflow-metadata.yaml   (source: main.rs, executable: pipeline)
    ├── main.rs
    └── pipeline

Deployment Layout
-----------------

Bundle archives are placed **as-is** in the directory configured by
``[executable] bundles_folder``. The scanner enumerates ZIP files in this
directory, reads each archive's ``airflow-metadata.yaml``, and matches the
requested ``dag_id`` against the manifest's ``dags`` keys::

    /opt/airflow/executable-bundles/
    ├── example.zip
    ├── pipeline.zip
    └── analytics.zip

At task-execution time, the runtime locates the archive entry referenced by
the manifest's ``executable`` field, materialises it (typically into a
transient cache directory), and invokes it with the coordinator arguments.

See :class:`~airflow.providers.sdk.executable.bundle_scanner.BundleScanner`
for the consumer-side scanner.

Compatibility and Versioning
----------------------------

- The current format version is ``1.0``.
- Backward-incompatible changes increment the major component of
  ``format_version`` and are gated behind an explicit opt-in on the
  consumer side.
- New optional fields MAY be added in minor versions and MUST be ignored by
  older consumers.
