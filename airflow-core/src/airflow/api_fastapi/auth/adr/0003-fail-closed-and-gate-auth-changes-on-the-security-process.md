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

# 3. Fail closed, secure by default, and gate auth changes on the security process

Date: 2026-07-19

## Status

Accepted

## Context

The auth manager decides who may do what and validates who a caller *is*. Its
defining property is a posture: when anything is uncertain, it must **deny**. A
malformed or expired token, a token whose `kid` matches no key, a payload that will
not deserialize, an unsupported capability — each is an error path, and every error
path here must resolve to *no access*, never a fall-through that grants it.

This is easy to get wrong because the insecure version still passes the happy-path
test: a check that returns the request unauthenticated instead of rejecting it, a
secret compared with `==`, a password from a non-cryptographic RNG, a login cookie
without `Secure` / `SameSite`, a `kid` mismatch that falls back to another key, a
stale `_token` cookie re-authenticating after logout — all "work" until an adversary
is present. The repo `CLAUDE.md` security model is explicit about this posture and
about what is and is not a vulnerability.

The second half is process. Because a change here can weaken the whole deployment in
ways no unit test catches, **widening** what a caller can reach — honouring a
public/anonymous role, relaxing a validation, broadening a token's scope or lifetime,
changing the security model — is a security-model decision that belongs on the
devlist / with the security team *before* code, not in a late-stage review of a diff
that already shipped the widening.

## Decision

The auth manager fails closed and its security posture is changed only through the
security process:

- **Deny by default on every error and unhandled path.** An invalid, expired,
  revoked, or wrong-`kid` token rejects (raise `InvalidTokenError`); a
  deserialization failure rejects; an unsupported capability rejects (raise
  `NotImplementedError` / return `False`) — none fall through to access. A
  non-matching `kid` does not fall back to another key.
- **Use secure primitives on the auth path.** Compare secrets with
  `hmac.compare_digest`, generate secrets/passwords with a cryptographically secure
  RNG, set cookie flags correctly (`Secure` when the request is HTTPS, an
  appropriate `SameSite`), and scope cookies to the correct base/root path so a
  stale token cannot re-authenticate.
- **Anonymous / public access is explicit and configured, never accidental.**
  Injecting a user into request state, or treating a request as public, happens
  only behind a deliberate, trust-gated configuration — not as a default an error
  path can reach.
- **Widening access or changing the security model is gated on the security
  process.** Relaxing a check, broadening token scope/lifetime, adding an anonymous
  path, or otherwise changing what the model allows is agreed on the devlist / with
  the security team before the code — consistent with the repo `CLAUDE.md` security
  model, including its list of what is *not* a vulnerability.

## Consequences

- A caller who cannot be positively authorized gets nothing, even when something
  upstream failed — the safe direction on every error.
- Security primitives (comparison, RNG, cookie flags, claim validation) are correct
  by policy, not by a reviewer happening to notice.
- Access-widening changes arrive with prior agreement and a threat model, so review
  confirms an agreed decision rather than discovering a silent one.
- A public/anonymous path takes explicit configuration and sign-off. That friction is
  the point.

A change **violates** this decision when it:

- lets an error path (bad/expired/revoked/wrong-`kid` token, failed
  deserialization, unsupported capability) fall through to access instead of
  rejecting — where *access* means the request reaches a protected resource
  without a successful authorization check, not merely that the request continues
  to a dependency that re-checks. Middleware that swallows an error, leaves
  `request.state.user` unset, and lets the route's own auth dependency revalidate
  is failing closed, not open;
- compares a secret non-constant-time, uses a non-cryptographic RNG for a secret,
  or ships a login/refresh cookie without the correct `Secure` / `SameSite` / path
  scoping;
- widens what an unauthenticated or anonymous caller can reach, or otherwise
  changes the security model, without prior devlist / security-team agreement.

A reviewer should reject any auth change whose failure direction is "allow", and
should send any access-widening or security-model change back to the security
process before it is reviewed as code.

## Evidence

- #66556 — constant-time password compare via `hmac.compare_digest` (CWE-208).
- #66500 — moved generated passwords onto a cryptographically secure RNG.
- #66502 — set `SameSite=Lax` on the all-admins login cookie (merged fail-closed fix).
- #65348 — set the JWT refresh cookie `Secure` on HTTPS (merged fail-closed fix).
- #62771 — scoped the session token cookie to `base_url` so it cannot reuse outside
  its path (merged fail-closed fix).
- #64955 — kept a stale root-path `_token` cookie from silently re-authenticating.
- #66562 — made `state.user` injection explicit and trust-gated, not an accidental
  default.
- #66563 — warned when SimpleAuthManager runs in a production-shaped deployment.
- #67909 — raised when a `kid` does not match instead of falling back (merged
  fail-closed fix).
- #48056 — turned a deserialization failure into a rejection, not access.
- #61339 — ensured a logged-out/revoked token is actually rejected.
