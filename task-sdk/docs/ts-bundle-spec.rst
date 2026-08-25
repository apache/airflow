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

This document specifies the ``bundle.mjs`` format produced by
``airflow-ts-pack`` and consumed by
:class:`~airflow.sdk.coordinators.node.NodeCoordinator`.

Container
---------

The bundle remains an ECMAScript module that runs directly with
``node bundle.mjs``. It has three regions:

.. code-block:: text

    //# airflowBundle=<base64 JSON layout>\n
    //# airflowMetadata=<compact JSON>\n
    <bundled ECMAScript code>

The layout comes first so readers can locate and verify the other regions. The
current format has no embedded source region.

Layout Header
-------------

The ``airflowBundle`` payload is a base64-encoded UTF-8 JSON object:

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
      }
    }

Offsets are bytes from the beginning of ``bundle.mjs``. They use exactly 16
lowercase hexadecimal digits and describe half-open ranges: ``start`` is
included and ``end`` is excluded.

The first-line layout is a stable bootstrap descriptor, not a separately
versioned payload. The ``airflow_bundle_metadata_version`` stored in the
metadata region versions the entire TypeScript bundle contract, including this
physical framing and the decoded metadata schema. A reader parses the bounded,
named ranges before it can locate and verify that version.

The metadata range points to the UTF-8 JSON payload only, excluding the
JavaScript comment marker and newline. Its digest therefore covers the exact
JSON bytes stored in that range. The code range covers every byte after the
metadata line through the end of the file. Its digest covers those raw
JavaScript bytes.

The file begins with the layout line. The metadata marker immediately follows
that line, and exactly one newline separates the metadata payload from the code
range. These prescribed framing bytes are outside the hashed metadata and code
ranges; no additional bytes are permitted before, between, or after them.
Post-pack formatters, minifiers, source-map injectors, and other tools that
rewrite ``bundle.mjs`` invalidate the offsets or digests.

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
      "dags": {
        "example": {
          "tasks": ["extract", "load"]
        }
      }
    }

The packer serializes this object without insignificant whitespace and escapes
the ECMAScript line and paragraph separators (U+2028 and U+2029), keeping it in
one newline-terminated JavaScript comment without a second encoding layer. The
SHA-256 digest detects changes to the exact serialized bytes.

The coordinator uses the ``dags`` keys to choose a bundle for a task instance.
The ``source`` value is a logical authoring name only; it is not embedded source
content and is not used to execute the bundle.

Reader and Selection Algorithm
------------------------------

For each directory in ``bundles_root``, in configured order, the coordinator:

1. Looks for ``bundle.mjs`` and opens it once.
2. Reads a bounded first line and decodes the named metadata and code ranges.
3. Reads the bounded metadata line and checks that the declared metadata and
   code ranges exactly match their physical locations and the file size.
4. Computes SHA-256 for both ranges before parsing or using metadata.
5. Confirms with ``fstat`` that the open file did not change during
   verification.
6. Parses metadata and requires a supported TypeScript bundle contract major
   version from ``airflow_bundle_metadata_version``.
7. Skips the verified bundle if its ``dags`` mapping does not contain the
   requested ``dag_id``.
8. Resolves the supervisor schema version and selects the first usable match.

A missing, unrelated, unreadable, malformed, corrupt, or incompatible earlier
candidate does not prevent selection of a later usable match. When more than one
usable bundle declares the same Dag, the first configured match wins. If none
matches, the error identifies the requested Dag, searched roots, and rejected
candidates.

The coordinator does not cache Dag-to-path routing. It checks root ordering and
the current deployed files for each task selection. It may reuse section digests
from a bounded process-local cache when the open file identity, timestamps,
size, layout ranges, and declared digests have not changed.

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

The TypeScript packing workflow was unreleased when this format was added. The
coordinator therefore does not accept the earlier metadata-first prototype.
