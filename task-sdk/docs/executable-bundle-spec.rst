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

Executable Bundle Spec
======================

This document specifies the on-disk format of a build artifact produced by an
Airflow native-executable SDK (Go, Rust, C++, Zig, ...) and consumed by
:class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator`
at deployment time.

The goal is a single, language-agnostic *bundle* shape so that scheduler,
worker, and UI behave identically regardless of which compiled SDK produced
the DAG.

Bundle-spec version: ``1.0``.

Container
---------

A bundle is **the compiled executable itself, with a fixed-format footer
appended after the binary's normal end-of-file**. The executable remains
directly runnable; the footer is data that follows the last byte the OS
loader cares about and is invisible to ``exec()``. There is no enclosing
archive.

A bundle file therefore has three regions, in order from offset 0:

1. The native executable (ELF / Mach-O / PE), including any code-signing
   structures the platform appends.
2. The primary DAG source file, embedded verbatim (UTF-8). MAY have length 0.
3. The build-time manifest (``airflow-metadata.yaml`` content, UTF-8).

The file ends with a fixed 64-byte trailer that locates regions (2) and (3),
carries an integrity hash of the binary region, and identifies the file as a
bundle. See :ref:`bundle-trailer-layout`.

Filenames follow OS conventions for executables: no extension on Linux/macOS,
``.exe`` on Windows. The scanner identifies bundles by the trailer's magic,
not by the filename.

The complete bundle file regions are:

.. code-block:: text

    [0,            source_start)    native binary (must be non-empty)
    [source_start, metadata_start)  embedded source (may be zero length)
    [metadata_start, file_size-64)  build-time manifest
    [file_size-64, file_size)       64-byte trailer

where ``metadata_start = file_size - 64 - metadata_len`` and
``source_start = metadata_start - source_len``.

Reference Implementation
------------------------

Below is a simple implementation to append the trailer with Python as a
reference when building your own packer. A language SDK is encouraged to
integrate trailer-packing into the build process to streamline the experience
for SDK users. Go SDK's ``airflow-go-pack`` is a good example.

.. code-block:: python

    #!/usr/bin/env python3

    import hashlib
    import shutil
    import struct

    BINARY = pathlib.Path(...)  # Path to the compiled executable.
    OUTPUT = pathlib.Path(...)  # Where to put the processed executable.
    SOURCE = b"..."  # Source code to embed.
    METADATA = b"..."  # UTF-8-encoded YAML metadata.

    # SHA-256 covers the binary region only: bytes [0, source_start).
    binary_sha256 = hashlib.sha256(BINARY.read_bytes()).digest()

    trailer = struct.pack(
        "<III 32s 12s 8s",
        len(SOURCE),  # source_len
        len(METADATA),  # metadata_len
        1,  # footer_ver
        binary_sha256,
        bytes(12),  # reserved
        b"AFBNDL01",  # magic
    )
    assert len(trailer) == 64

    shutil.copy(BINARY, OUTPUT)
    with OUTPUT.open("ab") as fh:
        fh.write(SOURCE)  # Embedded source region.
        fh.write(METADATA)  # Metadata region.
        fh.write(trailer)
    OUTPUT.chmod(0o755)


.. _bundle-trailer-layout:

Trailer Layout
--------------

The last 64 bytes of a conforming bundle are the trailer. All multi-byte
integers are little-endian.

.. code-block:: text

    bytes  0..3    source_len     uint32     length of the source region in bytes
    bytes  4..7    metadata_len   uint32     length of the metadata region in bytes
    bytes  8..11   footer_ver     uint32     currently 1
    bytes 12..43   binary_sha256  32 bytes   SHA-256 of the binary region [0, source_start)
    bytes 44..55   reserved       12 bytes   MUST be zero
    bytes 56..63   magic          8 bytes    ASCII "AFBNDL01"

The magic is the byte sequence ``0x41 0x46 0x42 0x4E 0x44 0x4C 0x30 0x31``
(``"AFBNDL01"``). The trailing ``01`` is the footer-format version repeated
in ASCII so a human can identify a bundle at a glance
(``tail -c 8 ./mybundle | xxd``); the binary ``footer_ver`` field is the
authoritative source of truth for parsing.

``binary_sha256`` is the SHA-256 digest computed over the **binary region
only** — bytes ``[0, source_start)``. The hash field sits inside the trailer
and therefore cannot cover the bytes it occupies; it provides *integrity*
(the binary region has not been truncated, corrupted, or naively edited
between packing and exec) rather than *authenticity*
(see :ref:`bundle-code-signing` for how authenticity layers on top).

Reader algorithm:

1. Open the file. Seek to ``EOF - 64``. Read 64 bytes.
2. Compare bytes ``56..63`` against ``"AFBNDL01"``. If different, the file
   is not a bundle; the scanner MUST ignore it.
3. Parse ``footer_ver``. If unknown, fail with a versioning error.
4. Compute ``metadata_start = filesize - 64 - metadata_len`` and
   ``source_start = metadata_start - source_len``.
5. Validate ``source_start >= 0`` and that the implied binary region
   (``[0, source_start)``) is non-empty.
6. Compute SHA-256 over the binary region ``[0, source_start)`` and compare
   to ``binary_sha256``. Mismatch is a hard failure handled identically to
   a magic-check failure: the scanner logs and skips the file. The result
   MAY be cached by ``(path, inode, mtime, size)`` so the runtime does not
   re-hash on every exec; a cache miss (file replaced, mtime bumped)
   triggers re-verification.
7. Read ``metadata_len`` bytes from ``metadata_start`` for the manifest.
8. Read ``source_len`` bytes from ``source_start`` for the source view.
   If ``source_len == 0``, no source is embedded; the UI displays
   "(source not available)".

Source comes *before* metadata so a future ``footer_ver`` MAY introduce
additional trailing blobs (e.g. signed checksums, compressed deps) by
extending the trailer rather than inserting between existing blobs.

.. _bundle-metadata-schema:

``airflow-metadata.yaml`` schema
--------------------------------

The metadata region carries the same YAML manifest documented previously,
produced at build time from a static scan of the DAG source. A
machine-readable JSON Schema is published at
:download:`airflow-metadata.schema.json` for use by build tooling, validators,
and editors.

.. code-block:: yaml

    airflow_bundle_metadata_version: "1.0"
    sdk:
      language: go
      version: "0.1.0"
      supervisor_schema_version: "2026-06-16"
    source: example.go
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

``airflow_bundle_metadata_version`` (string, required)
    The bundle-spec version this manifest conforms to. Currently ``"1.0"``.

``sdk`` (mapping, required)
    Identifies the SDK that produced the bundle.

    - ``language`` (string, required): lower-case source-language identifier
      (e.g. ``go``, ``rust``, ``cpp``, ``zig``).
    - ``version`` (string, required): SDK version used at build time.
    - ``supervisor_schema_version`` (string, required): dated AIP-72
      supervisor wire-schema version the bundle was compiled against, in
      ``YYYY-MM-DD`` format (e.g. ``"2026-06-16"``). The coordinator passes
      this value to the supervisor so it can downgrade outbound messages /
      upgrade inbound messages to a shape the bundle understands. The value
      MUST resolve against the supervisor's schema bundle; the coordinator
      validates it lazily when matching a bundle to a task at
      task-execution time, and an unknown version causes that bundle to be
      skipped.

``source`` (string, required)
    Original filename of the primary DAG source file (e.g. ``example.go``).
    The file's bytes live in the source region of the bundle, not at this
    path; this field is a display name the Airflow UI uses to label the
    source-view panel and pick a syntax-highlighting mode from the
    extension.

``dags`` (mapping, required)
    Mapping of ``dag_id`` to a *DAG entry*. Every ``dag_id`` the bundle
    exposes MUST appear here. The scanner uses these keys to match a DAG
    parsing or task-execution request to the bundle that owns it.

DAG entry fields:

``tasks`` (list of strings, required)
    Static list of ``task_id``\ s declared in the DAG. Empty lists are
    permitted but discouraged.

Unrecognized top-level or DAG-entry keys MUST be ignored by the consumer so
that future SDK versions can extend the manifest without breaking older
runtimes.

Examples
--------

Go bundle::

    example
    ├── ELF/Mach-O/PE executable
    ├── source region:   contents of example.go
    ├── metadata region: airflow-metadata.yaml (source: example.go)
    └── trailer (64 B):  lengths + binary_sha256 + AFBNDL01 magic

Rust bundle::

    pipeline
    ├── ELF/Mach-O/PE executable
    ├── source region:   contents of main.rs
    ├── metadata region: airflow-metadata.yaml (source: main.rs)
    └── trailer (64 B):  lengths + binary_sha256 + AFBNDL01 magic

The bundle is one file. ``./example`` runs the binary; the appended data
is invisible to ``exec()``.

Build Pipeline Ordering
-----------------------

The footer is appended after the executable is otherwise complete. Producers
that perform additional post-build steps MUST observe the following order:

- **Strip** debug symbols *before* appending the footer. Strip
  implementations operate on the binary's defined end and either leave
  trailing data intact or truncate it; do not rely on either behaviour.
- **Compute binary_sha256** over the on-disk bytes *as they stand
  immediately before the append*. At that moment the whole file is the
  binary region; nothing has been written past its OS-defined end yet, so
  the digest matches what the reader will recompute over
  ``[0, source_start)`` after the append.
- **Append** ``<source><metadata><trailer>`` in a single write so a
  partially written file fails the magic or hash check rather than
  appearing as a half-valid bundle.

.. _bundle-code-signing:

Code Signing
~~~~~~~~~~~~

The bundle format itself does not require OS-level code signing.
``binary_sha256`` provides integrity against truncation, in-flight
corruption, and naive tampering, and Airflow's threat model treats
``executables_root`` as Deployment-Manager-controlled — *authenticity*
(signed by a trusted identity) is a deployment-time concern rather than a
bundle-format one.

**Compressors** such as UPX are NOT supported. They rewrite the file
end-to-end, destroying both the trailer and the hash invariant.

Determinism: the trailer is byte-identical for byte-identical inputs, so a
deterministic build plus a canonical (sorted-key) manifest serialization
yields a byte-identical bundle file (and therefore a stable
``binary_sha256``).

Deployment Layout
-----------------

Bundle files are placed **as-is** in any of the directories configured as the
``executables_root`` kwarg on the
:class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator` entry
under ``[sdk] coordinators``. The scanner walks each directory **recursively**
and considers only regular files whose **executable bit is set** for the
invoking user; files without the executable bit are skipped without reading
their trailer. For each candidate it reads the last 64 bytes and treats files
whose magic matches ``"AFBNDL01"`` as bundles. Matched files are then
SHA-256-verified per the reader algorithm; a mismatch demotes the file back
to "ignored, with an error log." Files without the magic are silently
ignored, so non-bundle files (READMEs, dotfiles) MAY share the directory
without interfering with the scan.

::

    /opt/airflow/executable-bundles/
    ├── example
    ├── team-a/
    │   └── pipeline
    └── analytics

At task-execution time the runtime execs the bundle file directly with the
coordinator arguments (``--comm=<addr>`` / ``--logs=<addr>``). No extraction,
no transient cache directory, no chmod-after-extract step is required: the
file is already a runnable executable with the appropriate permission bits
preserved by the build pipeline. The integrity check runs at scan/discovery
time and is cached by ``(path, inode, mtime, size)``, so the exec hot path
does not re-hash.

The compiled executable MUST honor the SDK coordinator protocol —
``--comm=<host:port>`` / ``--logs=<host:port>`` socket-based IPC.

See :class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator`
for the consumer-side coordinator.

Inspection
----------

Because the bundle is a single executable rather than an archive,
inspecting the embedded source and manifest requires a small CLI rather
than an off-the-shelf ``unzip``. The Go SDK's ``airflow-go-pack`` tool
provides an ``inspect`` subcommand that dumps both regions; equivalent
helpers are expected from each language's packer.

Compatibility and Versioning
----------------------------

- The current bundle-spec format version is ``1.0``
  (``airflow_bundle_metadata_version``); the current trailer format version is
  ``1`` (``footer_ver = 1``).
- Backward-incompatible bundle-spec changes increment the major component
  of ``airflow_bundle_metadata_version`` and are gated behind an explicit opt-in
  on the consumer side.
- New optional manifest fields MAY be added in minor versions and MUST be
  ignored by older consumers.
- New trailer-format versions append fields after ``binary_sha256``
  (consuming the reserved region) or extend the trailer with additional
  trailing blobs ahead of the magic. Older readers MUST reject unknown
  ``footer_ver`` rather than guessing.
