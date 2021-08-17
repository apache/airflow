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
set -euo pipefail

# Use this to sign the tar balls generated from
# python setup.py sdist --formats=gztar
# ie. sign.sh <my_tar_ball>
# you will still be required to type in your signing key password
# or it needs to be available in your keychain

# Which key to sign releases with? This can be a (partial) email address or a
# key id. By default use any apache.org key
SIGN_WITH="${SIGN_WITH:-apache.org}"

for name in "${@}"
do
    gpg --yes --armor --local-user "$SIGN_WITH" --output "${name}.asc" --detach-sig "${name}"
    shasum -a 512 "${name}" > "${name}.sha512"
done
