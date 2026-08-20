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

# Rewrite java-sdk/gradle/verification-metadata.xml from an empty component
# list. Gradle only ever appends to that file, so a version bump leaves the
# superseded entries behind and they stay trusted forever; starting from an
# empty list drops them.
#
# Usage:
#   regenerate-verification-metadata.sh           # rewrite the file in place
#   regenerate-verification-metadata.sh --check   # report drift, leave the file alone

set -euo pipefail

JAVA_SDK_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
METADATA_FILE="$JAVA_SDK_ROOT/gradle/verification-metadata.xml"

# Only what these tasks resolve gets recorded, so the list has to cover
# everything CI builds — anything missing here fails strict verification later.
GRADLE_TASKS=(
  build
  :sdk:dokkaGeneratePublicationHtml
  :sdk:dokkaGeneratePublicationJavadoc
  sourceTarball
  checksumSourceTarball
  publishToMavenLocal
)

# One shot at Maven Central and the Gradle Plugin Portal makes this a flaky
# check rather than a useful one.
MAX_ATTEMPTS=3
RETRY_DELAY_SECONDS=30

check_only=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    --check) check_only=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

work_dir="$(mktemp -d)"
committed="$work_dir/committed"
regenerated="$work_dir/regenerated"
cp "$METADATA_FILE" "$committed"

# Gradle rewrites the whole file and drops the ASF header the insert-license
# hook requires, so carry the committed one over.
license_header="$(awk 'NR > 1 { if ($0 ~ /^<verification-metadata/) exit; print }' "$committed")"

restore_committed() {
  cp "$committed" "$METADATA_FILE"
}

cleanup() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    restore_committed
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

write_empty_metadata() {
  awk '
    /<components>/ { print "   <components/>"; print "</verification-metadata>"; exit }
    { print }
  ' "$committed" > "$METADATA_FILE"
}

restore_license_header() {
  grep -q "Licensed to the Apache Software Foundation" "$METADATA_FILE" && return 0
  awk -v header="$license_header" '
    NR == 1 { print; if (header != "") print header; next }
    { print }
  ' "$METADATA_FILE" > "$METADATA_FILE.tmp"
  mv "$METADATA_FILE.tmp" "$METADATA_FILE"
}

attempt=1
while true; do
  echo "==> Regenerating verification metadata (attempt $attempt/$MAX_ATTEMPTS)"
  write_empty_metadata
  # Both properties are needed for the run to reach the end: signing has no key
  # here, and sourceTarball has no default ref.
  if (cd "$JAVA_SDK_ROOT" && ./gradlew --no-daemon \
        --write-verification-metadata sha256 --refresh-dependencies \
        "${GRADLE_TASKS[@]}" -PskipSigning=true -PgitRef=HEAD); then
    break
  fi
  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "ERROR: regeneration failed after $MAX_ATTEMPTS attempts" >&2
    exit 1
  fi
  echo "Regeneration failed, retrying in ${RETRY_DELAY_SECONDS}s" >&2
  sleep "$RETRY_DELAY_SECONDS"
  attempt=$((attempt + 1))
done

restore_license_header

if [ "$check_only" = false ]; then
  echo "==> Wrote $METADATA_FILE"
  echo "Review every entry in the diff before committing: generating the file"
  echo "records what the repositories served, it does not make those bytes"
  echo "trustworthy."
  exit 0
fi

cp "$METADATA_FILE" "$regenerated"
restore_committed

if diff -u "$committed" "$regenerated"; then
  echo "==> Verification metadata is exactly what a clean regeneration produces"
  exit 0
fi

cat >&2 <<'MESSAGE'

ERROR: gradle/verification-metadata.xml does not match a clean regeneration.

Entries only present in the committed file are trusted checksums that nothing
resolves any more — usually versions superseded by a dependency bump. They stay
trusted, so a future direct or transitive dependency can pull that exact version
back in without anyone reviewing its checksum.

Run this from java-sdk/ and review every entry in the resulting diff:

    ./scripts/ci/regenerate-verification-metadata.sh
MESSAGE
exit 1
