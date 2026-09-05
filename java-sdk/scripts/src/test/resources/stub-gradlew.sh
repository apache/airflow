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

# Stands in for `gradlew --write-verification-metadata`. Two behaviours matter:
# it only ever adds to the component list, and it rewrites the file without the
# ASF header. Dropping an entry is therefore something only the caller can
# arrange, by emptying the list first.
#
# Fails its first N invocations, N being the content of stub-failing-attempts.
# Keeps the header instead of dropping it when stub-keeps-header exists.

set -euo pipefail

metadata="$PWD/gradle/verification-metadata.xml"
echo "call" >> "$PWD/gradlew-calls.log"

if [ "$(wc -l < "$PWD/gradlew-calls.log")" -le "$(cat "$PWD/stub-failing-attempts")" ]; then
  echo "stub gradlew: simulated failure" >&2
  exit 1
fi

header=""
if [ -f "$PWD/stub-keeps-header" ]; then
  header="$(sed -n '/^<!--/,/^-->/p' "$metadata")"
fi
kept="$(grep '<component ' "$metadata" || true)"

{
  echo '<?xml version="1.0" encoding="UTF-8"?>'
  [ -z "$header" ] || printf '%s\n' "$header"
  echo '<verification-metadata xmlns="https://schema.gradle.org/dependency-verification">'
  echo '   <configuration>'
  echo '      <verify-metadata>true</verify-metadata>'
  echo '      <verify-signatures>false</verify-signatures>'
  echo '   </configuration>'
  echo '   <components>'
  [ -z "$kept" ] || printf '%s\n' "$kept"
  printf '%s\n' "$kept" | grep -q 'name="current"' \
    || echo '      <component group="org.example" name="current" version="2.0"/>'
  echo '   </components>'
  echo '</verification-metadata>'
} > "$metadata.stub"
mv "$metadata.stub" "$metadata"
