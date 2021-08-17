#!/usr/bin/env python3
#
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
"""
Module to verify that migrations added have been documented in
docs/apache-airflow/migrations-ref.rst
"""
import glob
import os
import re
import sys


def check_migration_is_documented(migration_file, doc_path):

    with open(migration_file) as m_file:
        file_contents = m_file.read()
    match = re.search(r"revision\s*=\s*['\"](\w*)['\"]", file_contents)
    if match:
        revision_id = match.group(1)
        with open(doc_path) as doc_file:
            doc_file_contents = doc_file.read()
        if revision_id not in doc_file_contents:
            return False, revision_id
    return True, None


if __name__ == '__main__':
    project_root = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)

    airflow_migrations_dir = os.path.join(project_root, "airflow/migrations/versions")

    doc_path = os.path.abspath(os.path.join(project_root, "docs/apache-airflow/migrations-ref.rst"))
    assert os.path.isfile(doc_path)

    airflow_migrations_dir = os.path.abspath(airflow_migrations_dir)
    migration_files = [f for f in glob.glob(f"{airflow_migrations_dir}/*.py")]

    undocumented_migrations = []

    for migration_file in migration_files:
        is_documented, rev = check_migration_is_documented(migration_file=migration_file, doc_path=doc_path)
        if not is_documented:
            undocumented_migrations.append((migration_file, rev))

    if undocumented_migrations:
        print()
        print(f"DB Migrations in following files have not been documented in '{doc_path}'")
        print()
        for undocumented_migration, rev in undocumented_migrations:
            print(f"\t- {undocumented_migration} (Revision ID: '{rev}')")
        sys.exit(1)
