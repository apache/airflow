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

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${MY_DIR}"/..

function check_version() {
  if [[ ${VERSION:=} == "" ]]; then
    echo
    echo "Please export VERSION variable with the version of source package to prepare"
    echo
    exit 1
  fi
}

function tag_release() {
  echo
  echo "Tagging the sources with backport-providers-${VERSION} tag"
  echo

  git tag "backport-providers-${VERSION}"
}

function clean_repo() {
  ./confirm "Cleaning the repository sources - that might remove some of your unchanged files"

  git clean -fxd
}


function prepare_combined_changelog() {
  echo
  echo "Preparing the changelog"
  echo
  CHANGELOG_FILE="backport_packages/CHANGELOG.txt"
  PATTERN="airflow\/providers\/(.*)\/PROVIDERS_CHANGES_.*.md"
  echo > "${CHANGELOG_FILE}"
  CHANGES_FILES=$(find "airflow/providers/" -name 'PROVIDERS_CHANGES_*.md' | sort -r)
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
      "--prefix=apache-airflow-backport-providers-${VERSION%rc?}/" \
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
  tar -f "apache-airflow-backport-providers-${VERSION}-source.tar.gz" -xz -C "${DIR}"

  cp "backport_packages/INSTALL" "backport_packages/CHANGELOG.txt" \
      "${DIR}/apache-airflow-backport-providers-${VERSION%rc?}/"

  tar -f "apache-airflow-backport-providers-${VERSION}-source.tar.gz" -cz -C "${DIR}" \
      "apache-airflow-backport-providers-${VERSION%rc?}/"

  echo
  echo "Replaced INSTALL CHANGELOG.txt in ${ARCHIVE_FILE_NAME} "
  echo

}



check_version

export ARCHIVE_FILE_NAME="apache-airflow-backport-providers-${VERSION}-source.tar.gz"

tag_release
clean_repo
prepare_archive
prepare_combined_changelog
replace_install_changelog
