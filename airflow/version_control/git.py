#!/usr/bin/env python

import errno
import os
import subprocess
import time

from airflow.version_control.dag_folder_version_manager import DagFolderVersionManager


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class GitDagFolderVersionManager(DagFolderVersionManager):


    def __init__(self, master_dags_folder_path):
        self.master_dags_folder_path = master_dags_folder_path
        self.dags_folder_container = "/tmp/airflow_versioned_dag_folders"

    def checkout_dags_folder(self, git_sha_hash):
        mkdir_p(self.dags_folder_container)

        master_dags_folder_path = os.path.expanduser(self.master_dags_folder_path)

        dags_folder_path = self.dags_folder_container + "/" + git_sha_hash

        # todo(xuanji): maybe check the return code
        proc = subprocess.Popen(
            ['git', 'clone', '-q', master_dags_folder_path, dags_folder_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        _, err = proc.communicate()
        if err:
            if 'already exists and is not an empty directory' in err:
                pass
            else:
                print(err)
                raise ValueError('oops')

        proc = subprocess.Popen(
            ['git', 'checkout', git_sha_hash],
            cwd=dags_folder_path,
            stdout=subprocess.PIPE
        )

        return dags_folder_path

    def get_version_control_hash_of(self, filepath):
        # TODO(xuanji): check for dirty
        # (https://github.com/oohlaf/oh-my-zsh/blob/master/lib/git.zsh#L11)

        proc = subprocess.Popen(
            ['git', 'rev-parse', 'HEAD'],
            cwd=os.path.dirname(filepath),
            stdout=subprocess.PIPE
        )
        out, err = proc.communicate()
        assert err is None

        return out.replace('\n', '')

    def on_worker_start(self):
        while True:
            print('collecting garbage...')
            time.sleep(1)
