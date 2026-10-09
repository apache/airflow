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
 * What an {@link Asset} is built from: a `name`, a `uri`, or both.
 *
 * As in Python's `Asset`, a missing `name` defaults to the `uri` and a missing
 * `uri` to the `name`.
 */
export type AssetSpec =
  | { readonly name: string; readonly uri?: string }
  | { readonly name?: never; readonly uri: string };

/** What {@link Asset.ref} is built from: exactly one of `name` or `uri`. */
export type AssetRefSpec =
  { readonly name: string; readonly uri?: never } | { readonly uri: string; readonly name?: never };

function requireIdentifier(field: string, value: unknown): string {
  if (typeof value !== "string" || value === "") {
    throw new TypeError(`Asset ${field} must be a non-empty string`);
  }
  return value;
}

/**
 * A data asset, identified as Python's `Asset` identifies one.
 *
 * ```ts
 * const orders = new Asset({ name: "orders", uri: "s3://warehouse/orders" });
 * ```
 *
 * Unlike Python, the URI is kept as written rather than normalized.
 */
export class Asset {
  readonly name: string;
  readonly uri: string;
  // Nominal typing: a plain object with the same fields is not an Asset.
  declare private readonly assetKind: "asset";

  constructor({ name, uri }: AssetSpec) {
    if (name === undefined && uri === undefined) {
      throw new TypeError("Asset requires a name or a uri");
    }
    this.name = requireIdentifier("name", name ?? uri);
    this.uri = requireIdentifier("uri", uri ?? name);
  }

  /**
   * Refer to an asset by its name or by its URI alone, as Python's
   * `Asset.ref(name=...)` and `Asset.ref(uri=...)` do.
   */
  static ref({ name, uri }: AssetRefSpec): AssetRef {
    if (name !== undefined && uri !== undefined) {
      throw new TypeError("Asset.ref() takes either a name or a uri, not both");
    }
    if (name !== undefined) return new AssetNameRef(name);
    if (uri !== undefined) return new AssetUriRef(uri);
    throw new TypeError("Asset.ref() requires a name or a uri");
  }
}

/** A reference to an asset by name, from {@link Asset.ref}. */
export class AssetNameRef {
  readonly name: string;
  declare private readonly assetKind: "name";

  constructor(name: string) {
    this.name = requireIdentifier("name", name);
  }
}

/** A reference to an asset by URI, from {@link Asset.ref}. */
export class AssetUriRef {
  readonly uri: string;
  declare private readonly assetKind: "uri";

  constructor(uri: string) {
    this.uri = requireIdentifier("uri", uri);
  }
}

/** A reference to an asset, from {@link Asset.ref}. */
export type AssetRef = AssetNameRef | AssetUriRef;
