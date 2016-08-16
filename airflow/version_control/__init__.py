#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# hardcoded symlink to git


from git import GitDagFolderVersionManager
from noop import NoopDagFolderVersionManager
from airflow import configuration as conf

VERSION_CONTROL_STRATEGY = conf.get('core', "version_control_strategy")
DAGS_FOLDER = conf.get('core', "dags_folder")

if VERSION_CONTROL_STRATEGY == "git":
    dagFolderVersionManager = GitDagFolderVersionManager(DAGS_FOLDER)
elif VERSION_CONTROL_STRATEGY == "noop":
    dagFolderVersionManager = NoopDagFolderVersionManager(DAGS_FOLDER)
else:
    raise ValueError('unrecognized version control strategy')


def checkout_dags_folder(git_sha_hash):
    return dagFolderVersionManager.checkout_dags_folder(git_sha_hash)


def get_version_control_hash_of(filepath):
    return dagFolderVersionManager.get_version_control_hash_of(filepath)


def on_worker_start(celery_pid):
	return dagFolderVersionManager.on_worker_start(celery_pid)