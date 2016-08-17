#!/usr/bin/env python

from airflow.version_control.dag_folder_version_manager import DagFolderVersionManager


class NoopDagFolderVersionManager(DagFolderVersionManager):

    def __init__(self, master_dags_folder):
        self.master_dags_folder = master_dags_folder

    def get_version_control_hash_of(self, _):
        return None

    def checkout_dags_folder(self, _):
        return self.master_dags_folder

    def on_worker_start(self):
        return
