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

# Apache Airflow — release-build configuration (providers train)

Build and verification facts for a providers wave, read by `release-verify-rc`.
Scope and source of truth are the same as in
[`release-management-config.md`](release-management-config.md): providers train
only, taken from [`dev/README_RELEASE_PROVIDERS.md`](../dev/README_RELEASE_PROVIDERS.md).

## Source archive

| Key | Value |
|---|---|
| `source_archive_method` | `custom` |
| `source_archive_format` | `tar.gz` |
| `source_archive_prefix` | `apache_airflow_providers-<YYYY-MM-DD>` |

The source tarball is produced by Breeze, not by `repro-archive`; Breeze honours
the `export-ignore` entries in the root `.gitattributes`.

## Build invocation

Run at the `providers/<YYYY-MM-DD>` tag:

```bash
breeze release-management prepare-provider-distributions --include-removed-providers \
  --distribution-format both --version-suffix ""
breeze release-management prepare-tarball --tarball-type apache_airflow_providers \
  --version "<YYYY-MM-DD>"
```

## Convenience artefacts

```yaml
convenience_artefacts:
  - name: apache_airflow_providers_<provider>-<version>-py3-none-any.whl
    kind: wheel
    staging: dist-dev
    reproducibility: byte-identical
    publish_channel: pypi
  - name: apache_airflow_providers_<provider>-<version>.tar.gz
    kind: sdist
    staging: dist-dev
    reproducibility: byte-identical
    publish_channel: pypi
```

Both are built by the second command of the build invocation above.

## Source-tree validators

None configured.

## Expected artefact list

Under `https://dist.apache.org/repos/dist/dev/airflow/providers/<YYYY-MM-DD>/`:

- `apache_airflow_providers-<YYYY-MM-DD>-source.tar.gz` — canonical source artefact.
- `apache_airflow_providers_<provider>-<version>.tar.gz` — sdist per provider.
- `apache_airflow_providers_<provider>-<version>-py3-none-any.whl` — wheel per provider.

Each with a detached `.asc` signature and a `.sha512` checksum.

## Digest set

- `sha512`, required.

## Reproducibility checks

| Key | Value |
|---|---|
| `reproducibility_source` | `on` |
| `reproducibility_binaries` | `byte-identical` |

Rebuild with the build invocation at the tag and compare byte-for-byte, per
[`README_RELEASE_PROVIDERS.md` § Reproducible package builds checks](../dev/README_RELEASE_PROVIDERS.md#reproducible-package-builds-checks).

## Binary-exclude list

Baseline only; no additional globs or accepted exceptions.

## Apache RAT configuration

- **RAT excludes file:** `.rat-excludes` at the repository root.
- **RAT invocation:** standalone Apache RAT jar, per
  [`README_RELEASE_PROVIDERS.md` § Licence check](../dev/README_RELEASE_PROVIDERS.md#licence-check).
