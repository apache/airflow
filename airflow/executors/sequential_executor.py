import logging
import subprocess

from airflow.executors.base_executor import BaseExecutor
from airflow.utils import State


class SequentialExecutor(BaseExecutor):
    """
    This executor will only run one task instance at a time, can be used
    for debugging. It is also the only executor that can be used with sqlite
    since sqlite doesn't support multiple connections.

    Since we want airflow to work out of the box, it defaults to this
    SequentialExecutor alongside sqlite as you first install it.
    """
    def __init__(self):
        super(SequentialExecutor, self).__init__()
        self.commands_to_run = []

    def execute_async(self, key, command, queue=None):
        self.commands_to_run.append((key, command,))

    def sync(self):
        for key, command in self.commands_to_run:
            logging.info("command" + str(command))
            try:
                sp = subprocess.Popen(command, shell=True)
                sp.wait()
            except Exception as e:
                self.change_state(key, State.FAILED)
                raise e
            self.change_state(key, State.SUCCESS)
        self.commands_to_run = []

    def end(self):
        self.heartbeat()
