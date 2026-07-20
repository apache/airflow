<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 2. Serialized output must be deterministic

Date: 2026-07-18

## Status

Accepted

## Context

The serialized representation of a Dag is hashed to detect change: a Dag is only
assigned a new `DagVersion` when its serialized bytes actually differ from the
previously stored version. This makes the *stability* of the serialized output a
correctness property, not merely a cosmetic one.

When serialization falls back to `str(obj)` / `repr(obj)` for a value it does
not know how to encode, and the object has no stable `__repr__`, Python emits
the default object representation — `<module.Class object at 0x7f3c…>`. That
string embeds a memory address that is different on every process and every
parse cycle. The serialized payload then changes on every parse even though the
Dag is unchanged, so the Dag processor mints a fresh `DagVersion` each cycle.
The result is unbounded version churn: version tables grow without limit, the UI
version history becomes noise, and downstream consumers keying off DagVersion
thrash.

Because a single Dag can reach a non-deterministic value through many code paths
(default args, template fields, trigger kwargs, callback references, operator
attributes), fixing one path is not enough — any remaining path that can carry
an unstable repr into the JSON reintroduces the churn.

## Decision

Serialization must emit **stable, deterministic output** for identical input.

- No `<... object at 0x...>` memory addresses, and no other unstable `str()` /
  `repr()` fallbacks, may reach the serialized JSON. A value that cannot be
  encoded deterministically must be handled explicitly (encode its meaningful
  fields, resolve it to a stable identifier, or reject it) — never smuggled
  through a default `repr`.
- When a fix is made, **every** path that can carry the offending value must be
  fixed, not just the one that surfaced the bug. A partial fix that leaves a
  sibling path unstable does not resolve the churn.
- The fix must be covered by a test that exercises the full
  **serialize → deserialize → hash** path and asserts stability across repeated
  serialization. The test must **fail without the change** — i.e. it must
  reproduce the version churn / address leak when the fix is reverted.

## Consequences

- A given Dag serializes to identical bytes every parse cycle, so DagVersion is
  created only on genuine change. Version history stays meaningful and the
  version tables stop growing without cause.
- Contributors adding new serializable value types must provide a deterministic
  encoding for them rather than relying on the generic string fallback.

A **violating change** looks like any of:

- A diff that lets a memory address (`at 0x…`) or any other unstable `repr()`
  reach the serialized JSON — directly, or by adding a new value type that
  falls through to the generic `str()`/`repr()` path.
- A fix that patches only the path from the reported bug while leaving another
  path able to serialize the same unstable value. Adding the deterministic
  encoding itself is what this ADR asks for; `0004` governs *where* it is added —
  in the shared helper, not at one call site.
- A test that still **passes while the bug is present** — e.g. one that checks a
  single serialization call, or mocks out the address, instead of asserting that
  two independent serialize cycles of the same Dag produce identical, stable
  output.

## Evidence

- #69243 — stops a new DagVersion from being minted every parse by preventing a
  `retry_policy` object from serializing via its unstable default repr.
- #63871 — fixes `serialize_template_field` handling of a callable value nested
  in a dict, closing a path that otherwise leaked a non-deterministic value into
  the serialized template field.
