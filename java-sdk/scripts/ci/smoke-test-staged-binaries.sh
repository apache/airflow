#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Smoke-test the STAGED convenience binaries: resolve the airflow-sdk-bom and
# every artifact it manages from the ASF Nexus staging repository. This is only
# meaningful for a staged release candidate, so it lives apart from
# verify-source-release.sh (which checks the source package and also runs on PRs
# and tag pushes, where nothing is staged).
#
# Usage:
#   smoke-test-staged-binaries.sh --nexus-repo-id <NNNN> --version <VERSION>
#
# Requirements:
#   Bash with common shell tools, Gradle, a JDK, and network access to
#   repository.apache.org and Maven Central.

set -euo pipefail

nexus_repo_id="" version=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --nexus-repo-id) nexus_repo_id="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [ -z "$nexus_repo_id" ] || [ -z "$version" ]; then
  echo "ERROR: --nexus-repo-id and --version are required" >&2
  exit 2
fi

nexus_url="https://repository.apache.org/content/repositories/orgapacheairflow-$nexus_repo_id"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "==> Staged-binary smoke test (Nexus repo orgapacheairflow-$nexus_repo_id)"

# Derive the artifact list from the published BOM rather than hard-coding it. The
# BOM is the authoritative set (kept honest against the published modules by
# :bom:verifyBomCoverage), so this never drifts as modules are added/removed.
# Fetching the BOM pom also confirms the BOM itself is staged.
bom_pom="$work/airflow-sdk-bom.pom"
curl -fsSL -o "$bom_pom" \
  "$nexus_url/org/apache/airflow/airflow-sdk-bom/$version/airflow-sdk-bom-$version.pom"
artifacts="$(sed -n '/<dependencyManagement>/,/<\/dependencyManagement>/p' "$bom_pom" \
  | grep -oE '<artifactId>[^<]+</artifactId>' | sed -E 's#</?artifactId>##g' | sort -u)"
if [ -z "$artifacts" ]; then
  echo "ERROR: no managed artifacts found in the BOM ($bom_pom)" >&2
  exit 1
fi
echo "    BOM manages:"
while IFS= read -r a; do echo "      $a"; done <<< "$artifacts"

smoke="$work/smoke"
mkdir -p "$smoke"
echo 'rootProject.name = "airflow-sdk-nexus-smoke"' > "$smoke/settings.gradle.kts"
{
  echo 'plugins { id("java-library") }'
  echo 'repositories {'
  echo "    maven { url = uri(\"$nexus_url/\") }"
  echo '    mavenCentral()'
  echo '}'
  echo 'dependencies {'
  echo "    implementation(platform(\"org.apache.airflow:airflow-sdk-bom:$version\"))"
  for a in $artifacts; do
    echo "    implementation(\"org.apache.airflow:$a\")"
  done
  echo '}'
} > "$smoke/build.gradle.kts"

(
  cd "$smoke"
  # Forces resolution of every BOM-managed artifact from the staging repo.
  gradle --no-daemon dependencies --configuration runtimeClasspath
)
echo "    OK"
echo "Staged-binary smoke test passed."
