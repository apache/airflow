#!/usr/bin/env python

import errno
import os
import subprocess
import time
import psutil
import shutil

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
        # todo(xuanji): retry if another git operation is in progress
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

    def on_worker_start(self, celery_pid):
        while True:
            print('collecting garbage for', celery_pid, 'I am', os.getpid())
            celery_workers = psutil.Process(celery_pid).children()

            dag_versions_in_use = set()

            for celery_worker in celery_workers:
                for celery_child in psutil.Process(celery_worker.pid).children():
                    cmdline = ' '.join(celery_child.cmdline()).split(' ')
                    for arg in cmdline:
                        if arg.startswith('--dag-version='):
                            dag_versions_in_use.add(arg[len('--dag-version='):])

            checked_out_dags = set(os.listdir(self.dags_folder_container))

            shas_to_reap =checked_out_dags - dag_versions_in_use

            for sha in shas_to_reap:
                directory_to_reap = self.dags_folder_container + '/' + sha

                shutil.rmtree(directory_to_reap)

            time.sleep(1)
