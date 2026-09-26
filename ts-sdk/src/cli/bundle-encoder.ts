/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Encodes a self-contained TypeScript Dag bundle that remains directly
 * executable by Node.
 *
 * Final byte order:
 *
 *   airflowBundle header
 *   -> airflowMetadata
 *   -> airflowSource region, one block per source file, in the order they
 *      appear in `entrypointSources`
 *   -> executable JavaScript
 *
 * The header records each region's byte range and digest, and each source
 * region also carries the path it was compiled from. Metadata describes what
 * the bundle serves, source regions carry the author's original files, and
 * the executable JavaScript runs the task handlers.
 *
 * This module owns the on-disk encoding. Readers must use the header's named
 * byte ranges and paths rather than incidental line positions.
 */

import { createHash } from "node:crypto";

import type { BundleManifest } from "../coordinator/manifest.js";

const AIRFLOW_BUNDLE_METADATA_VERSION = "1.0";
const EMBEDDED_METADATA_MAX_BYTES = 1024 * 1024;
const EMBEDDED_SOURCE_MAX_BYTES = 1024 * 1024;
const OFFSET_HEX_WIDTH = 16;

export const EMBEDDED_METADATA_PREFIX = "//# airflowMetadata=";
export const EMBEDDED_LAYOUT_PREFIX = "//# airflowBundle=";
/** Opens each source block, followed by the region's path and a newline. */
export const EMBEDDED_SOURCE_MARKER = "/*# airflowSource:";
export const EMBEDDED_SOURCE_CLOSE = "\n#*/\n";

export interface BundleEncoderInput {
  bundleManifest: BundleManifest;
  sdkVersion: string;
  /** Author-owned source file per native Dag, keyed by its path in
   *  `BundleManifest.dag_source_paths`. Empty for a mixed-lang bundle. */
  sourceFiles: Record<string, string>;
  executable: Uint8Array;
}

interface BundleMetadata {
  airflow_bundle_metadata_version: string;
  sdk: { language: string; version: string; supervisor_schema_version: string };
  dag_source_paths: BundleManifest["dag_source_paths"];
  task_handlers: BundleManifest["task_handlers"];
}

interface VerifiedByteRange {
  end: string;
  sha256: string;
  start: string;
}

interface VerifiedSourceRegion extends VerifiedByteRange {
  path: string;
}

interface BundleHeader {
  code: VerifiedByteRange;
  metadata: VerifiedByteRange;
  sources: VerifiedSourceRegion[];
}

/** Per-source-region metadata within the concatenated sources buffer. */
interface SourceRegion {
  path: string;
  payloadStart: number;
  payloadEnd: number;
  sha256: string;
}

interface EncodedSources {
  buffer: Buffer;
  regions: SourceRegion[];
}

export function encodeBundle(input: BundleEncoderInput): Buffer {
  const metadata = encodeMetadata(input);
  const sources = encodeSources(input.sourceFiles);
  const executable = encodeExecutable(input.executable);
  const header = encodeHeader({ metadata, sources, executable });

  return Buffer.concat([header, metadata, sources.buffer, executable]);
}

function encodeHeader(regions: {
  metadata: Buffer;
  sources: EncodedSources;
  executable: Buffer;
}): Buffer {
  // Each digest covers the payload only. The framing markers and newlines are re-derived.
  const metadataPayload = regions.metadata.subarray(
    Buffer.byteLength(EMBEDDED_METADATA_PREFIX),
    -1,
  );
  const metadataDigest = computeSha256(metadataPayload);
  const codeDigest = computeSha256(regions.executable);
  const zeroOffset = "0".repeat(OFFSET_HEX_WIDTH);
  // Placeholder header with zeroed offsets and real digests, to measure its
  // length without recursing. Every source path is present, so the array
  // length is what it will be in the final header.
  const placeholderHeader = renderHeader({
    code: { start: zeroOffset, end: zeroOffset, sha256: codeDigest },
    metadata: { start: zeroOffset, end: zeroOffset, sha256: metadataDigest },
    sources: regions.sources.regions.map((region) => ({
      path: region.path,
      start: zeroOffset,
      end: zeroOffset,
      sha256: region.sha256,
    })),
  });
  const metadataStart = placeholderHeader.length + Buffer.byteLength(EMBEDDED_METADATA_PREFIX);
  const metadataEnd = metadataStart + metadataPayload.length;
  const sourcesBaseOffset = placeholderHeader.length + regions.metadata.length;
  const codeStart = sourcesBaseOffset + regions.sources.buffer.length;
  const codeEnd = codeStart + regions.executable.length;
  const header = renderHeader({
    code: {
      start: formatOffset(codeStart),
      end: formatOffset(codeEnd),
      sha256: codeDigest,
    },
    metadata: {
      start: formatOffset(metadataStart),
      end: formatOffset(metadataEnd),
      sha256: metadataDigest,
    },
    sources: regions.sources.regions.map((region) => ({
      path: region.path,
      start: formatOffset(sourcesBaseOffset + region.payloadStart),
      end: formatOffset(sourcesBaseOffset + region.payloadEnd),
      sha256: region.sha256,
    })),
  });
  if (header.length !== placeholderHeader.length) {
    throw new Error("Bundle header changed length while resolving section offsets");
  }
  return header;
}

/**
 * Wrap each source in its own block comment, one per author-owned Dag file.
 *
 * A comment terminator would splice the rest of the payload into executable position, so it is
 * escaped. `*\\` is escaped too, which keeps the transformation reversible.
 *
 * The returned regions are keyed by path and hold byte offsets within the
 * concatenated sources buffer (not the final bundle); the header adds the
 * bundle-relative base offset when it renders.
 */
function encodeSources(sources: Record<string, string>): EncodedSources {
  const chunks: Buffer[] = [];
  const regions: SourceRegion[] = [];
  let offset = 0;

  for (const [path, content] of Object.entries(sources)) {
    // A `*/` in the path would close the marker's own comment early, so the
    // packer refuses rather than trying to escape it — no real filesystem path
    // holds one, and rejecting keeps the marker line trivially readable.
    if (path.includes("*/") || path.includes("\n")) {
      throw new Error(
        `Source path ${JSON.stringify(path)} contains a block-comment terminator or newline; ` +
          `airflow-ts-pack cannot embed it`,
      );
    }
    const openBytes = Buffer.from(`${EMBEDDED_SOURCE_MARKER}${path}\n`, "utf-8");
    const payloadBytes = Buffer.from(escapeBlockComment(content), "utf-8");
    const closeBytes = Buffer.from(EMBEDDED_SOURCE_CLOSE, "ascii");
    // Per-file cap: one large source cannot drown the others, and the total
    // is only bounded by however many files a bundle declares.
    if (payloadBytes.length > EMBEDDED_SOURCE_MAX_BYTES) {
      throw new Error(
        `Embedded source ${JSON.stringify(path)} is ${payloadBytes.length} bytes, ` +
          `over the ${EMBEDDED_SOURCE_MAX_BYTES} byte limit; move code out of that file into ` +
          `imported modules`,
      );
    }

    chunks.push(openBytes, payloadBytes, closeBytes);
    const payloadStart = offset + openBytes.length;
    const payloadEnd = payloadStart + payloadBytes.length;
    regions.push({
      path,
      payloadStart,
      payloadEnd,
      sha256: computeSha256(payloadBytes),
    });
    offset = payloadEnd + closeBytes.length;
  }

  return { buffer: Buffer.concat(chunks), regions };
}

function escapeBlockComment(source: string): string {
  return source.replaceAll(/\*([\\/])/g, "*\\$1");
}

function encodeMetadata(input: BundleEncoderInput): Buffer {
  const payload = Buffer.from(
    JSON.stringify(buildBundleMetadata(input))
      .replaceAll("\u2028", "\\u2028")
      .replaceAll("\u2029", "\\u2029"),
    "utf-8",
  );
  const metadata = Buffer.concat([
    Buffer.from(EMBEDDED_METADATA_PREFIX, "ascii"),
    payload,
    Buffer.from("\n", "ascii"),
  ]);
  if (metadata.length > EMBEDDED_METADATA_MAX_BYTES) {
    throw new Error(
      `Embedded airflow metadata is ${metadata.length} bytes, ` +
        `over the ${EMBEDDED_METADATA_MAX_BYTES} byte limit; reduce the number of registered tasks`,
    );
  }
  return metadata;
}

function encodeExecutable(executable: Uint8Array): Buffer {
  const bytes = Buffer.from(executable);
  if (bytes[0] !== 0x23 || bytes[1] !== 0x21) return bytes;
  const newline = bytes.indexOf(0x0a);
  return newline === -1 ? Buffer.alloc(0) : bytes.subarray(newline + 1);
}

function buildBundleMetadata(input: BundleEncoderInput): BundleMetadata {
  return {
    airflow_bundle_metadata_version: AIRFLOW_BUNDLE_METADATA_VERSION,
    sdk: {
      language: "typescript",
      version: input.sdkVersion,
      supervisor_schema_version: input.bundleManifest.supervisor_schema_version,
    },
    dag_source_paths: input.bundleManifest.dag_source_paths,
    task_handlers: input.bundleManifest.task_handlers,
  };
}

function renderHeader(header: BundleHeader): Buffer {
  // utf-8 rather than ascii because source paths (`sources[i].path`) may hold
  // non-ASCII characters — a Latin-1 filename otherwise loses bytes here and
  // the metadata-to-region mapping stops round-tripping.
  const payload = JSON.stringify(header);
  return Buffer.from(`${EMBEDDED_LAYOUT_PREFIX}${payload}\n`, "utf-8");
}

function formatOffset(offset: number): string {
  const value = offset.toString(16);
  if (value.length > OFFSET_HEX_WIDTH) {
    throw new Error(`Bundle offset ${offset} exceeds the 16-digit hexadecimal layout limit`);
  }
  return value.padStart(OFFSET_HEX_WIDTH, "0");
}

function computeSha256(contents: Uint8Array): string {
  return createHash("sha256").update(contents).digest("hex");
}
