#!/usr/bin/env python

import errno
import os
import subprocess
import time
import psutil
import shutil
import sys
import errno
import fcntl

from airflow.version_control.dag_folder_version_manager import DagFolderVersionManager

# these must be a module-level variable so that the file handles are not
# garbage collected
sh_lock_fh = {}
ex_lock_fh = {}

def acquire_lock(path, lock_type):
    """
    Acquire a shared or exclusive flock, and block until the lock is taken
    Not thread-safe
    The same process can acquire a lock on the same file twice
    """
    assert lock_type in ['sh', 'ex']

    if lock_type == 'ex':
        if path in ex_lock_fh:
            return
        fh = open(path, 'w+')
        ex_lock_fh[path] = fh
        fcntl.flock(fh, fcntl.LOCK_EX)
    else:
        if path in sh_lock_fh:
            return
        fh = open(path, 'w+')
        sh_lock_fh[path] = fh
        fcntl.flock(fh, fcntl.LOCK_SH)


def release_lock(path, lock_type):
    assert lock_type in ['sh', 'ex']

    if lock_type == 'ex':
        del ex_lock_fh[path]
    else:
        del sh_lock_fh[path]


def is_lock_held(path):
    """
    Returns true when path is held by any lock (either sh or ex) by
    any process, including this one
    """
    if path in sh_lock_fh or path in ex_lock_fh:
        return True

    # local variable - lock will be released once this fh is closes
    fh = open(path, 'w+')
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return False
    except IOError:
        return True

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def creat(path):

    """Creates a file if it does not exist and opens it"""

    try:
        file_handle = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY
        )
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

def git_clone_retry(source, target):
    """
    Run `git clone source target` and retry if another
    git operation is in progress. Ignores errors if
    target already exist. Raises otherwise.
    """

    proc = subprocess.Popen(
        ['git', 'clone', '-q', source, target],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    # todo(xuanji): maybe check the return code
    _, err = proc.communicate()
    if err:
        if 'already exists and is not an empty directory' in err:
            return
        elif 'Another git process seems to be running in this repository' in err:
            time.sleep(1)
            return git_clone_retry(source, target)
        else:
            print(os.getpid(), 'git clone failed with ', err)
            raise ValueError('oops')


def git_checkout_retry(source, git_sha_hash):
    proc = subprocess.Popen(
        ['git', 'checkout', git_sha_hash],
        cwd=source,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    _, err = proc.communicate()
    if err:
        if 'HEAD is now at' in err:
            return
        elif 'Another git process seems to be running' in err:
            time.sleep(1)
            return git_checkout_retry(source, git_sha_hash)
        else:
            print(os.getpid(), 'git checkout failed with', err, source, git_sha_hash)
            raise ValueError('oops')


def is_in_use(dags_folder_container, git_sha_hash):
    lock_path = dags_folder_container + '/locks/' + git_sha_hash
    return is_lock_held(lock_path)

class GitDagFolderVersionManager(DagFolderVersionManager):


    def __init__(self, master_dags_folder_path):
        self.master_dags_folder_path = master_dags_folder_path
        self.dags_folder_container = "/tmp/airflow_versioned_dag_folders"

    def checkout_dags_folder(self, git_sha_hash):
        mkdir_p(self.dags_folder_container)

        dags_folder_container_lock = self.dags_folder_container + '/lock'
        checked_out_dag_locks_dir = self.dags_folder_container + '/locks'

        mkdir_p(checked_out_dag_locks_dir)

        creat(dags_folder_container_lock)
        acquire_lock(dags_folder_container_lock, 'ex') # lock for git checkout/clone

        master_dags_folder_path = os.path.expanduser(self.master_dags_folder_path)

        dags_folder_path = self.dags_folder_container + "/" + git_sha_hash

        git_clone_retry(master_dags_folder_path, dags_folder_path)
        sys.stdout.flush()
        git_checkout_retry(dags_folder_path, git_sha_hash)
        sys.stdout.flush()

        release_lock(dags_folder_container_lock, 'ex')   # done with git, release lock

        checked_out_dag_lock = checked_out_dag_locks_dir + '/' + git_sha_hash

        creat(checked_out_dag_lock)
        acquire_lock(checked_out_dag_lock, 'sh')

        # hold lock until process exit

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

            checked_out_dags = set(d for d in os.listdir(self.dags_folder_container) if (not d.endswith('locks')) and (not d.endswith('lock')))

            print('Checked out DAG folders:')
            for checked_out_dag in checked_out_dags:
                sha_is_in_use = is_in_use(self.dags_folder_container, checked_out_dag)
                print(checked_out_dag, sha_is_in_use)
                if not sha_is_in_use:
                    directory_to_reap = self.dags_folder_container + '/' + checked_out_dag
                    print('reaping...')
                    # todo: lock this operation
                    shutil.rmtree(directory_to_reap)

            time.sleep(1)
