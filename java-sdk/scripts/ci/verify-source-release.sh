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

# Run the reviewer checklist from java-sdk/README.md against an Apache Airflow
# Java SDK source-release tarball (except the GPG signature).
#
# Usage:
#   verify-source-release.sh --tarball <path> --tag java-sdk/<version>-rc<N> \
#       [--sha512 <path>] [--nexus-repo-id <NNNN>]
#
# Requirements:
#   Bash with common shell tools, Git, Gradle, JDK 11,
#   and (for --nexus-repo-id) network access.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"

tarball="" tag="" sha512="" nexus_repo_id=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --tarball) tarball="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --sha512) sha512="$2"; shift 2 ;;
    --nexus-repo-id) nexus_repo_id="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [ -z "$tarball" ] || [ -z "$tag" ]; then
  echo "ERROR: --tarball and --tag are required" >&2
  exit 2
fi
tarball="$(cd "$(dirname "$tarball")" && pwd)/$(basename "$tarball")"
[ -z "$sha512" ] || sha512="$(cd "$(dirname "$sha512")" && pwd)/$(basename "$sha512")"

work="$(mktemp -d)"
tagco="$work/tagco"
cleanup() {
  git -C "$REPO_ROOT" worktree remove --force "$tagco" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

echo "==> 1. Checksum"
if [ -n "$sha512" ]; then
  ( cd "$(dirname "$tarball")" && sha512sum -c "$sha512" )
else
  echo "    no --sha512 given; skipping"
fi

echo "==> 2. Extract to a clean directory outside the repo"
tar -xzf "$tarball" -C "$work"
extracted="$(find "$work" -maxdepth 1 -type d -name 'apache-airflow-java-sdk-*' | head -n1)"
[ -n "$extracted" ] || { echo "ERROR: no apache-airflow-java-sdk-* directory inside the tarball" >&2; exit 1; }
echo "    $extracted"

echo "==> 3. No compiled/binary files (ASF source releases must be source-only)"
bins="$(find "$extracted" -type f -exec sh -c 'file -b "$1" | grep -qviE "text|json|xml|empty" && echo "$1"' _ {} \; || true)"
if [ -n "$bins" ]; then
  echo "ERROR: non-source files found in the tarball:" >&2
  echo "$bins" >&2
  exit 1
fi
echo "    OK"

echo "==> 4. Diff against the tagged source (only export-ignored files may differ)"
git -C "$REPO_ROOT" worktree add --detach --quiet "$tagco" "$tag"
diffout="$(diff -rq "$extracted" "$tagco/java-sdk" \
  | grep -vE ': (gradlew|gradlew\.bat|gradle-wrapper\.jar|\.editorconfig|scripts)$' || true)"
if [ -n "$diffout" ]; then
  echo "ERROR: tarball differs from the tag beyond the export-ignored files:" >&2
  echo "$diffout" >&2
  exit 1
fi
echo "    OK"

echo "==> 5. Build from scratch (regenerate the wrapper, then ./gradlew build)"
props="$extracted/gradle/wrapper/gradle-wrapper.properties"
gv="$(sed -n 's#^distributionUrl=.*/gradle-\(.*\)-bin\.zip#\1#p' "$props")"
gsum="$(sed -n 's#^distributionSha256Sum=##p' "$props")"
if [ -z "$gv" ] || [ -z "$gsum" ]; then
  echo "ERROR: could not read Gradle version/sha256 from $props" >&2
  exit 1
fi
(
  cd "$extracted"
  gradle --no-daemon wrapper --gradle-version "$gv" --gradle-distribution-sha256-sum "$gsum"
  ./gradlew --no-daemon build   # `build` runs `check`, which runs :bom:verifyBomCoverage
)
echo "    OK"

echo "==> 6. LICENSE and NOTICE present in every built jar"
missing=0
while IFS= read -r jar; do
  for entry in META-INF/LICENSE META-INF/NOTICE; do
    if ! unzip -l "$jar" | grep -qE "${entry}$"; then
      echo "ERROR: ${entry} missing from ${jar##*/}" >&2
      missing=1
    fi
  done
done < <(find "$extracted" -path '*/build/libs/*.jar')
[ "$missing" -eq 0 ] || exit 1
echo "    OK"

if [ -n "$nexus_repo_id" ]; then
  echo "==> 7. Staged-binary smoke test (Nexus repo orgapacheairflow-$nexus_repo_id)"
  version="$(basename "$extracted" | sed 's/^apache-airflow-java-sdk-//')"
  smoke="$work/smoke"
  mkdir -p "$smoke"
  cat > "$smoke/settings.gradle.kts" <<'SETTINGS'
rootProject.name = "airflow-sdk-nexus-smoke"
SETTINGS
  cat > "$smoke/build.gradle.kts" <<BUILD
plugins { id("java-library") }
repositories {
    maven { url = uri("https://repository.apache.org/content/repositories/orgapacheairflow-${nexus_repo_id}/") }
    mavenCentral()
}
dependencies {
    implementation(platform("org.apache.airflow:airflow-sdk-bom:${version}"))
    implementation("org.apache.airflow:airflow-sdk")
    implementation("org.apache.airflow:airflow-sdk-jpl")
    implementation("org.apache.airflow:airflow-sdk-jul")
    implementation("org.apache.airflow:airflow-sdk-log4j2")
    implementation("org.apache.airflow:airflow-sdk-slf4j")
    implementation("org.apache.airflow:airflow-sdk-processor")
}
BUILD
  (
    cd "$smoke"
    # Forces resolution of every BOM-managed artifact from the staging repo.
    gradle --no-daemon dependencies --configuration runtimeClasspath
  )
  echo "    OK"
fi

echo "All release-verification checks passed."
