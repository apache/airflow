#!/usr/bin/env python

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
