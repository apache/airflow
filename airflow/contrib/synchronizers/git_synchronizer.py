from base_synchronizer import BaseSynchronizer
from airflow.models import DAGS_FOLDER

import logging

import git

LOG = logging.getLogger(__name__)


class GitSynchronizer(BaseSynchronizer):
    def sync(self):
        g = git.cmd.Git(DAGS_FOLDER)
        try:
            g.pull()
        except Exception:
            LOG.error("Cannot git pull to synchronize dag folder")
            LOG.exception(Exception)
            return False

        return True


def get_instance():
    return GitSynchronizer()

