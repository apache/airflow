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

TypeScript Bundle Format
========================

This document specifies the bundle format produced by ``airflow-ts-pack`` and consumed by
:class:`~airflow.sdk.coordinators.node.NodeCoordinator`.

Artifact Name
-------------

A bundle's name must end in ``.min.mjs``. Nothing else about it is significant: the coordinator searches each
configured root recursively and routes on embedded metadata, so one root may hold several differently named bundles.
``airflow-ts-pack`` writes ``bundle.min.mjs`` by default and accepts ``--outfile`` for any other name ending in
that suffix.

Container
---------

The bundle remains an ECMAScript module that runs directly with ``node bundle.min.mjs``. It has four regions:

.. code-block:: text

    //# airflowBundle=<compact JSON layout>\n
    //# airflowMetadata=<compact JSON>\n
    /*# airflowSource\n<escaped entrypoint source>\n#*/\n
    <minified, bundled ECMAScript code>

The layout comes first so readers can locate and verify the other regions.

Layout Header
-------------

The ``airflowBundle`` payload is a compact UTF-8 JSON object:

.. code-block:: json

    {
      "code": {
        "start": "0000000000000401",
        "end": "0000000000001200",
        "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
      },
      "metadata": {
        "start": "0000000000000300",
        "end": "0000000000000400",
        "sha256": "123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0"
      },
      "source": {
        "start": "0000000000000412",
        "end": "00000000000003f0",
        "sha256": "23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"
      }
    }

Offsets are bytes from the beginning of the bundle file. They use exactly 16 lowercase hexadecimal digits and
describe half-open ranges: ``start`` is included and ``end`` is excluded.

The layout stays on one line and contains only controlled ASCII field names, fixed-width hexadecimal offsets, and
SHA-256 digests. It therefore needs no additional encoding layer.

The first-line layout is a stable bootstrap descriptor, not a separately versioned payload. The
``airflow_bundle_metadata_version`` stored in the metadata region versions the entire TypeScript bundle contract,
including this physical framing and the decoded metadata schema. A reader parses the bounded, named ranges before it
can locate and verify that version.

The metadata range points to the UTF-8 JSON payload only, excluding the JavaScript comment marker and newline. Its
digest therefore covers the exact JSON bytes stored in that range. The source range likewise points to the escaped
entrypoint payload only, excluding the ``/*# airflowSource\n`` opener and the ``\n#*/\n`` closer. The code range
covers every byte after the source comment through the end of the file, and its digest covers those raw JavaScript
bytes.

The file begins with the layout line. The metadata marker immediately follows that line, exactly one newline
separates the metadata payload from the source comment's opener, and the source comment's closer is immediately
followed by the code range. These prescribed framing bytes are outside the hashed ranges, and no additional bytes are
permitted before, between, or after them. Post-pack formatters, compressors, source-map injectors, and other tools
that rewrite the bundle invalidate the offsets or digests.

Unlike the metadata range, the source range's length is declared rather than derivable from a newline, so a reader
pins it by checking that the prescribed opener and closer sit exactly where the declared range implies.

Source
------

The source region carries the entrypoint as its author wrote it, so the Airflow UI has something readable to show
for a natively authored TypeScript Dag. The shipped code region is minified and is not the code anyone wrote. Only
the entrypoint is embedded, not the module graph behind it:
`ADR-0006 <https://github.com/apache/airflow/blob/main/airflow-core/adr/lang-sdk/0006-no-lang-sdk-source-display.md>`__
declined multi-file source display for mixed-language Dags.

It is a block comment rather than the line comments the layout and metadata use, because the entrypoint spans the
lines it was written on and a ``//`` comment would end at the first of them. Line terminators, including ``\r`` and
U+2028/U+2029, are therefore legal inside the region and need no escaping.

``*/`` is the one sequence that must not appear: it would end the comment where Node reads the file, putting the rest
of the payload into executable position while the layout still calls those bytes source and both digests still match.
The packer escapes it by inserting a ``\`` between the two characters, and escapes ``*\`` the same way so the
transformation is reversible. A reader recovers the entrypoint by dropping the ``\`` that follows a ``*`` and keeping
the character behind it, and MUST reject a bundle whose source range contains an unescaped ``*/``.

The region is capped at 1 MiB. Larger entrypoints should move code into imported modules, which are bundled into the
code region as usual.

Metadata
--------

The ``airflowMetadata`` payload is compact UTF-8 JSON with this logical shape:

.. code-block:: json

    {
      "airflow_bundle_metadata_version": "1.0",
      "sdk": {
        "language": "typescript",
        "version": "0.1.0-beta1",
        "supervisor_schema_version": "2026-06-16"
      },
      "source": "main.ts",
      "task_handlers": {
        "example": {
          "tasks": ["extract", "load"]
        }
      }
    }

The packer serializes this object without insignificant whitespace and escapes the ECMAScript line and paragraph
separators (U+2028 and U+2029), keeping it in one newline-terminated JavaScript comment without a second encoding
layer. The SHA-256 digest detects changes to the exact serialized bytes.

``task_handlers`` is keyed by Dag ID and lists the task IDs the bundle handles for each. It is named for what a
TypeScript bundle actually provides: handlers for Dags declared elsewhere, not Dag definitions of its own. The
coordinator uses its keys to choose a bundle for a task instance.

The metadata ``source`` value is the logical authoring name displayed for the Dag, a filename rather than content.
The source region named in the layout header is what carries the content. The two are separate fields in separate
documents, and neither is used to execute the bundle.

Reader and Selection Algorithm
------------------------------

For each candidate in ``bundles_root``, the coordinator:

1. Opens it once. Candidates are the files whose name ends in ``.min.mjs``, found by walking each root recursively,
   roots in configured order and each directory's entries in sorted order, so selection does not depend on the order
   a filesystem returns entries in. Directories are deduplicated by ``(st_dev, st_ino)``, so a symlink loop
   terminates the walk instead of exhausting the interpreter stack.
2. Reads a bounded first line and decodes the named metadata and code ranges.
3. Reads the bounded metadata line and checks that the declared metadata range matches its physical location.
4. Reads the bounded source region, checks that its prescribed opener and closer frame the declared range, rejects an
   unescaped ``*/`` inside it, and checks that the declared code range matches its physical location and file size.
5. Computes SHA-256 for all three ranges before parsing or using metadata.
6. Confirms with ``fstat`` that the open file did not change during verification.
7. Parses metadata and requires a supported bundle contract major version from
   ``airflow_bundle_metadata_version``.
8. Skips the verified bundle if its ``task_handlers`` mapping does not contain the requested ``dag_id``.
9. Resolves the supervisor schema version and selects the first usable match.

A missing, unrelated, unreadable, malformed, corrupt, or incompatible earlier candidate does not prevent selection
of a later usable match. When more than one usable bundle declares the same Dag, the first configured match wins. If
none matches, the error identifies the requested Dag, searched roots, and rejected candidates.

Every ``.min.mjs`` file under a root is therefore opened, and one that is not a usable bundle is named among those
rejected candidates. ``bundles_root`` names directories of deployed Airflow bundles, so unrelated minified modules do
not belong there.

The coordinator does not cache Dag-to-path routing. It checks root ordering and the current deployed files for each
task selection. It may reuse section digests from a bounded process-local cache when the open file identity,
timestamps, size, layout ranges, and declared digests have not changed.

Integrity, Authenticity, and Provenance
---------------------------------------

The digests detect truncation, corruption, or modification when the stored
digests remain unchanged. They do not authenticate the producer: someone able
to replace the bundle can replace its header and recompute both digests.

The format also makes no provenance claim about which TypeScript sources or
build process produced the JavaScript. Authenticity requires a signature or a
digest delivered through a separately trusted channel. Provenance requires a
build attestation or reproducible-build verification.

The coordinator launches Node using the verified path. Replacing that path
between verification and process launch remains a time-of-check/time-of-use
window. Deployments should use controlled write permissions and atomic artifact
replacement. The digest cache is a performance optimization, not a trust anchor.

Versioning and Compatibility
----------------------------

The Node coordinator accepts TypeScript bundle contract versions with major
version 1 and ignores unknown optional header or metadata fields added by later
minor versions. It rejects a missing, malformed, or different major version.
Any incompatible change to either metadata or the meaning, encoding, or order
of physical regions requires a new major version and an explicit coordinator
change.

The current strict marker, range, adjacency, file-size, and digest checks make
older readers fail closed when they encounter incompatible physical framing,
even when they cannot reach the metadata version. A future container that
cannot preserve the readable first-line descriptor must use a new marker rather
than reinterpret the current one.

The TypeScript packing workflow was unreleased when this format was added. The coordinator therefore does not accept
the earlier metadata-first prototype.
