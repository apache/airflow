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

# Developing the Airflow TypeScript SDK

Contributor and release-manager reference for `@apache-airflow/ts-sdk`. See
[`README.md`](README.md) for the user-facing package documentation.

## Building and testing

```bash
pnpm install
pnpm test
pnpm run typecheck
pnpm run build
pnpm run verify:package
```

The committed lockfile and `pnpm-workspace.yaml` define the dependency security
policy. Newly released dependency versions must age for 14 days before they
can enter the lockfile, transitive dependencies cannot use Git or arbitrary
tarball sources, and only explicitly approved dependencies can run lifecycle
build scripts. Review changes to both files together when updating dependencies.

`verify:package` creates the npm tarball, rejects files outside the published
runtime allowlist, installs it into a clean temporary project, and smoke-tests
every `exports` entry point and the `bin` executable. The required paths are
derived from `package.json`, so a new export subpath is covered automatically.

`tsconfig.build.json` turns off `sourceMap` and `declarationMap` that the base
`tsconfig.json` enables. The published tarball ships `dist` but not `src`, so
emitted maps would point at files the consumer never receives — the allowlist
rejects them rather than shipping dangling maps. Local `pnpm run typecheck`
still uses the base config, so editor tooling is unaffected.

Without a local pnpm install, [prek](https://prek.j178.dev) can compile the SDK
or verify the package with its own managed node + pnpm toolchain:

```bash
prek run compile-ts-sdk --all-files
prek run --hook-stage manual verify-ts-sdk-package --all-files
```

## API reference

The public API reference is generated from the TypeScript sources with
[TypeDoc](https://typedoc.org/) and published to
<https://airflow.apache.org/docs/ts-sdk/stable/>.

Build it locally (runs the pinned toolchain in a Node container, so no local
Node install is needed):

```bash
breeze build-docs --sdk-docs-only --sdk=typescript
```

The rendered site is staged at `generated/_build/docs/ts-sdk/stable/`, alongside
a `stable.txt` holding the version from `ts-sdk/package.json`. To iterate on the
docs directly instead, `npm ci && npm run build` inside `ts-sdk/docs/` writes to
`ts-sdk/docs/_build/html/`, and `npm start` rebuilds on change.

`typedoc.config.mjs` pins the theme's `basePath` to `/docs/ts-sdk/<version>`, so
the generated HTML expects to be served from that prefix and looks unstyled when
opened straight off disk. Override it for any local preview, `npm start`
included:

```bash
TS_SDK_DOCS_BASE_PATH=/ npm run build && npx serve _build/html
```

`npm run build` also strips the theme's Google Fonts tags and then fails if the
output still has root-relative asset URLs or remote font requests. `npm test`
covers those checks; `npm start` skips them, so run a full build before
publishing.

CI builds the reference on every change under `ts-sdk/src/` or `ts-sdk/docs/`,
so a broken docs build fails the PR rather than the release.

### Publishing the API docs

Publishing is a separate, deliberate step — a providers-only publish wave will
not refresh the SDK docs as a side effect. Trigger the *Publish Docs to S3*
workflow for the release ref:

```bash
gh workflow run "Publish Docs to S3" --repo apache/airflow --ref main \
  -f ref=<RELEASE_REF> \
  -f include-docs=ts-sdk \
  -f destination=live
```

Use `destination=staging` first and check
<https://airflow.staged.apache.org/docs/ts-sdk/stable/>, then publish to `live`
and confirm that <https://airflow.apache.org/docs/ts-sdk/stable/> resolves (allow
time for cache invalidation) and that `/docs/ts-sdk/` redirects to it.

## Publishing

The manually dispatched `Release TypeScript SDK` workflow first builds, tests,
and hashes one package tarball without OIDC permissions. Its protected publish
job then either uses [npm's staged-publishing flow](https://docs.npmjs.com/staged-publishing/)
or publishes the formal release directly.

npm's registry supports only one trusted-publisher configuration per package,
so both release paths share a single configuration pinned to a single GitHub
environment:

```bash
npm trust github @apache-airflow/ts-sdk \
  --repo apache/airflow \
  --file ts-sdk-release.yml \
  --environment ts-sdk-npm-release \
  --allow-stage-publish \
  --allow-publish
```

Confirm it with `npm trust list @apache-airflow/ts-sdk`. A second `npm trust
github` for the same package is rejected, so replace an outdated configuration
with `npm trust revoke` first.

Because the registry cannot express "stage only" and "publish only" as separate
relationships, what separates the two paths is the GitHub environment gate
rather than an npm-side permission split. Require reviewers and prevent
self-review on `ts-sdk-npm-release`, restrict its deployment tags to
`ts-sdk/*`, and protect those tags from updates and deletion with a repository
ruleset. The pinned `--environment` claim means a workflow edit that drops the
gate loses the ability to publish at all.

Create and push a `ts-sdk/<version>` tag whose version exactly matches
`package.json`, then dispatch the workflow on that same tag so npm provenance
names the source commit that produced the tarball. The tag must already exist
on `apache/airflow` before the dispatch below can reference it as `--ref`:

```bash
git tag ts-sdk/1.0.0-beta2 <commit-sha>
git push upstream ts-sdk/1.0.0-beta2
```

The commit you tag must be on `main` or on a core release branch (`vX-Y-test`
or `vX-Y-stable`); the workflow rejects anything else. Releasing from a release
branch is the normal path once a core minor has branched: cherry-pick the SDK
change there first, then tag that branch's commit.

To submit the package to npm's private staging area for review, run:

```bash
gh workflow run ts-sdk-release.yml --repo apache/airflow --ref ts-sdk/1.0.0-beta2 \
  -f release_type=staged \
  -f tag=ts-sdk/1.0.0-beta2 \
  -f npm_tag=beta
```

The publish job calls `npm stage publish` only after the unprivileged build job
has uploaded a checksummed tarball. The version is not publicly installable
until a maintainer reviews and approves it with 2FA. The following commands
require npm 11.15 or later:

```bash
npm stage list @apache-airflow/ts-sdk
npm stage view <stage-id>
npm stage download <stage-id>
npm stage approve <stage-id>
```

The approval cannot run through the trusted-publisher workflow because npm
requires interactive proof of presence. Reject an unsuitable staged version
with `npm stage reject <stage-id>`. Do not run the formal workflow for a version
that is already staged; approve or reject that staged version instead.

To publish directly without npm's staging review, trigger the formal path:

```bash
gh workflow run ts-sdk-release.yml --repo apache/airflow --ref ts-sdk/1.0.0-beta2 \
  -f release_type=formal \
  -f tag=ts-sdk/1.0.0-beta2 \
  -f npm_tag=beta
```

Use `latest` for stable releases. A prerelease may only use the channel derived
from its own prerelease identifier (`1.0.0-beta2` → `beta`) or `next` —
`scripts/validate-release-inputs.mjs` rejects any other tag. The workflow also
rejects a dispatch that would move the selected npm dist-tag backward as of
when it runs. For a staged release, the dist-tag only actually moves later, at
`npm stage approve` time, which is not re-validated — approve staged versions
in the order they were requested so the dist-tag does not regress. Both
publication paths use short-lived npm OIDC credentials and automatically
publish provenance. After verifying the trusted-publisher setup, disable
token-based publishing and revoke obsolete npm automation tokens.

## Calling a release vote

Every release needs a `[VOTE]` on `dev@airflow.apache.org`. The vote is
formally on the signed source package in `dist/dev`; the npm package is the
convenience binary.

The release candidate is tagged `ts-sdk/<VERSION>-rc<N>` with `package.json`
set to the same `<VERSION>-rc<N>`, then published with the `formal` workflow
above using the `rc` dist-tag, so reviewers can install it with `npm install`.

Put the signed source package in the dist dev area as
`ts-sdk/<VERSION>-rc<N>/apache-airflow-ts-sdk-<VERSION>-src.tar.gz`, with its
`.asc` signature and `.sha512` checksum.

### Vote email template

```text
Subject: [VOTE] Release Apache Airflow TypeScript SDK <VERSION> based on <VERSION>-rc<N>

Hi,

I would like to call a vote to release Apache Airflow TypeScript SDK <VERSION>,
based on release candidate <VERSION>-rc<N>.

Changes since <the last release>: <a one-line description>

The release candidate contains the following npm package:

- @apache-airflow/ts-sdk

Git information:

- Tag: ts-sdk/<VERSION>-rc<N>
- Commit: <full-commit-sha>
- https://github.com/apache/airflow/releases/tag/ts-sdk%2F<VERSION>-rc<N>

Source release, signatures and checksums:
https://dist.apache.org/repos/dist/dev/airflow/ts-sdk/<VERSION>-rc<N>/

Convenience binary (published on npm under the rc dist-tag):
https://www.npmjs.com/package/@apache-airflow/ts-sdk/v/<VERSION>-rc<N>

To try the release candidate in a project:

    npm install @apache-airflow/ts-sdk@<VERSION>-rc<N>

KEYS file (public keys used to sign the release):
https://downloads.apache.org/airflow/KEYS

Please review and vote. The vote will remain open for at least 72 hours, until <YYYY-MM-DD HH:MM UTC>, or until the necessary number of binding votes is reached.

[ ] +1 Release this package as Apache Airflow TypeScript SDK <VERSION>
[ ] +0 No opinion
[ ] -1 Do not release, because ...

Only votes from Airflow PMC members are binding, but everyone is welcome and encouraged to test the release and vote.

The verification process can be found in the main repository:
https://github.com/apache/airflow/blob/ts-sdk/<VERSION>-rc<N>/ts-sdk/DEVELOPMENT.md#verifying-a-release

For more details on ASF release verification, see: https://www.apache.org/info/verification.html

Best,
<your name>
```

Pre-send checklist:

* `npm view @apache-airflow/ts-sdk@<VERSION>-rc<N> version` prints
  `<VERSION>-rc<N>`.
* `Changes since <the last release>` and the vote deadline are filled in.
* Run `grep '<' email.txt` on the rendered email and confirm **no output** —
  any match means a template placeholder was left unfilled.

### Verifying a release

Run these against the source package in `dist/dev`.

1. **Checksum.**

   ```bash
   sha512sum -c apache-airflow-ts-sdk-<VERSION>-src.tar.gz.sha512
   ```

2. **Signature.**

   ```bash
   curl -O https://downloads.apache.org/airflow/KEYS
   gpg --import KEYS
   gpg --verify apache-airflow-ts-sdk-<VERSION>-src.tar.gz.asc \
       apache-airflow-ts-sdk-<VERSION>-src.tar.gz
   ```

3. **Diff against the git tag.** The extracted directory should match
   `ts-sdk/` at the tag, apart from files excluded via `export-ignore`:

   ```bash
   tar xzf apache-airflow-ts-sdk-<VERSION>-src.tar.gz
   git clone --branch ts-sdk/<VERSION>-rc<N> \
     https://github.com/apache/airflow.git tag-checkout
   diff -rq apache-airflow-ts-sdk-<VERSION>/ tag-checkout/ts-sdk/
   ```

   Any diff output is unexpected and should block the vote.

4. **No binary files.**

   ```bash
   find apache-airflow-ts-sdk-<VERSION>/ -type f \
     -exec sh -c 'file -b "$1" | grep -qviE "text|json|xml|empty" && echo "$1"' _ {} \;
   ```

   This should print nothing.

5. **Build and test from source.**

   ```bash
   cd apache-airflow-ts-sdk-<VERSION>
   pnpm install --frozen-lockfile
   pnpm test
   pnpm run build
   pnpm run verify:package
   ```
