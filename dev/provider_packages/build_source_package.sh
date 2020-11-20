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

# Use this to sign the tar balls generated from
# python setup.py sdist --formats=gztar
# ie. sign.sh <my_tar_ball>
# you will still be required to type in your signing key password
# or it needs to be available in your keychain
set -euo pipefail

PROVIDER_ID_PACKAGES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${PROVIDER_ID_PACKAGES_DIR}"/../..

function check_version() {
    : "${VERSION:?"Please export VERSION variable with the version of source package to prepare"}"
}

function tag_release() {
  echo
  echo "Tagging the sources with ${BACKPORT_PREFIX}providers-${VERSION} tag"
  echo

  git tag "${BACKPORT_PREFIX}providers-${VERSION}"
}

function clean_repo() {
  ./confirm "Cleaning the repository sources - that might remove some of your unchanged files"

  git clean -fxd
}


function prepare_combined_changelog() {
  echo
  echo "Preparing the changelog"
  echo
  CHANGELOG_FILE="provider_packages/CHANGELOG.txt"
  PATTERN="airflow\/providers\/(.*)\/${BACKPORT_CAPITAL_PREFIX}PROVIDER_CHANGES_.*.md"
  echo > "${CHANGELOG_FILE}"
  CHANGES_FILES=$(find "airflow/providers/" -name "${BACKPORT_CAPITAL_PREFIX}PROVIDER_CHANGES_*.md" | sort -r)
  LAST_PROVIDER_ID=""
  for FILE in ${CHANGES_FILES}
  do
      echo "Adding ${FILE}"
      [[ ${FILE} =~ ${PATTERN} ]]
      PROVIDER_ID=${BASH_REMATCH[1]//\//.}
      {
          if [[ ${LAST_PROVIDER_ID} != "${PROVIDER_ID}" ]]; then
              echo
              echo "Provider: ${BASH_REMATCH[1]//\//.}"
              echo
              LAST_PROVIDER_ID=${PROVIDER_ID}
          else
              echo
          fi
          cat "${FILE}"
          echo
      } >> "${CHANGELOG_FILE}"
  done


  echo
  echo "Changelog prepared in ${CHANGELOG_FILE}"
  echo
}

function prepare_archive(){
  echo
  echo "Preparing the archive ${ARCHIVE_FILE_NAME}"
  echo

  git archive \
      --format=tar.gz \
      "backport-providers-${VERSION}" \
      "--prefix=apache-airflow-${BACKPORT_PREFIX}providers-${VERSION%rc?}/" \
      -o "${ARCHIVE_FILE_NAME}"

  echo
  echo "Prepared the archive ${ARCHIVE_FILE_NAME}"
  echo

}


function replace_install_changelog(){
  DIR=$(mktemp -d)

  echo
  echo "Replacing INSTALL CHANGELOG.txt in ${ARCHIVE_FILE_NAME} "
  echo
  tar -f "${ARCHIVE_FILE_NAME}" -xz -C "${DIR}"

  cp "provider_packages/INSTALL" "provider_packages/CHANGELOG.txt" \
      "${DIR}/apache-airflow-${BACKPORT_PREFIX}providers-${VERSION%rc?}/"

  tar -f "${ARCHIVE_FILE_NAME}" -cz -C "${DIR}" \
      "apache-airflow-${BACKPORT_PREFIX}providers-${VERSION%rc?}/"

  echo
  echo "Replaced INSTALL CHANGELOG.txt in ${ARCHIVE_FILE_NAME} "
  echo

}

BACKPORTS="false"
if (( $# > 0 )); then
    if [[ "$1" == "--backports" ]]; then
        BACKPORTS="true"
    else
        >&2 echo
        >&2 echo "You can run the script with '--backports' flag only"
        >&2 echo
        exit 1
    fi
fi
readonly BACKPORTS

BACKPORT_PREFIX=""
BACKPORT_CAPITAL_PREFIX=""
if [[ ${BACKPORTS} == "true" ]]; then
    BACKPORT_PREFIX='backport-'
    BACKPORT_CAPITAL_PREFIX="BACKPORT_"
fi

check_version

export ARCHIVE_FILE_NAME="apache-airflow-${BACKPORT_PREFIX}providers-${VERSION}-source.tar.gz"

tag_release
clean_repo
prepare_archive
prepare_combined_changelog
replace_install_changelog
