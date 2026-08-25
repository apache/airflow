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
 *   -> executable JavaScript
 *
 * The header tells Airflow where each region begins and ends and carries the
 * digest used to verify each one. Metadata describes what the bundle can serve,
 * and executable JavaScript runs its task handlers.
 *
 * This module owns the on-disk encoding. Readers must use the header's named
 * byte ranges rather than relying on incidental line positions.
 */

import { createHash } from "node:crypto";

import type { BundleManifest } from "../coordinator/manifest.js";

const AIRFLOW_BUNDLE_METADATA_VERSION = "1.0";
const EMBEDDED_METADATA_MAX_BYTES = 1024 * 1024;
const OFFSET_HEX_WIDTH = 16;

export const EMBEDDED_METADATA_PREFIX = "//# airflowMetadata=";
export const EMBEDDED_LAYOUT_PREFIX = "//# airflowBundle=";

export interface BundleEncoderInput {
  bundleManifest: BundleManifest;
  sdkVersion: string;
  entrypointName: string;
  executable: Uint8Array;
}

interface BundleMetadata {
  airflow_bundle_metadata_version: string;
  sdk: { language: string; version: string; supervisor_schema_version: string };
  source: string;
  dags: BundleManifest["dags"];
}

interface VerifiedByteRange {
  end: string;
  sha256: string;
  start: string;
}

interface BundleHeader {
  code: VerifiedByteRange;
  metadata: VerifiedByteRange;
}

export function encodeBundle(input: BundleEncoderInput): Buffer {
  const metadata = encodeMetadata(input);
  const executable = encodeExecutable(input.executable);
  const header = encodeHeader({ metadata, executable });

  return Buffer.concat([header, metadata, executable]);
}

function encodeHeader(regions: { metadata: Buffer; executable: Buffer }): Buffer {
  const metadataPayload = regions.metadata.subarray(
    Buffer.byteLength(EMBEDDED_METADATA_PREFIX),
    -1,
  );
  const digests = {
    code: computeSha256(regions.executable),
    metadata: computeSha256(metadataPayload),
  };
  const zeroOffset = "0".repeat(OFFSET_HEX_WIDTH);
  const placeholderHeader = renderHeader({
    code: { start: zeroOffset, end: zeroOffset, sha256: digests.code },
    metadata: { start: zeroOffset, end: zeroOffset, sha256: digests.metadata },
  });
  const metadataStart = placeholderHeader.length + Buffer.byteLength(EMBEDDED_METADATA_PREFIX);
  const metadataEnd = metadataStart + metadataPayload.length;
  const codeStart = placeholderHeader.length + regions.metadata.length;
  const codeEnd = codeStart + regions.executable.length;
  const header = renderHeader({
    code: {
      start: formatOffset(codeStart),
      end: formatOffset(codeEnd),
      sha256: digests.code,
    },
    metadata: {
      start: formatOffset(metadataStart),
      end: formatOffset(metadataEnd),
      sha256: digests.metadata,
    },
  });
  if (header.length !== placeholderHeader.length) {
    throw new Error("Bundle header changed length while resolving section offsets");
  }
  return header;
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
    source: input.entrypointName,
    dags: input.bundleManifest.dags,
  };
}

function renderHeader(header: BundleHeader): Buffer {
  const payload = Buffer.from(JSON.stringify(header), "ascii").toString("base64");
  return Buffer.from(`${EMBEDDED_LAYOUT_PREFIX}${payload}\n`, "ascii");
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
