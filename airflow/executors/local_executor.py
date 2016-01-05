import multiprocessing
import subprocess
import time

from builtins import range

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.utils import State, LoggingMixin

PARALLELISM = configuration.get('core', 'PARALLELISM')


class LocalWorker(multiprocessing.Process, LoggingMixin):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            key, command = self.task_queue.get()
            if key is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break
            self.logger.info("{} running {}".format(
                self.__class__.__name__, command))
            command = "exec bash -c '{0}'".format(command)
            try:
                subprocess.Popen(command, shell=True).wait()
                state = State.SUCCESS
            except Exception as e:
                state = State.FAILED
                self.logger.error("failed to execute task {}:".format(str(e)))
                # raise e
            self.result_queue.put((key, state))
            self.task_queue.task_done()
            time.sleep(1)


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
    """

    def start(self):
        self.queue = multiprocessing.JoinableQueue()
        self.result_queue = multiprocessing.Queue()
        self.workers = [
            LocalWorker(self.queue, self.result_queue)
            for i in range(self.parallelism)
        ]

        for w in self.workers:
            w.start()

    def execute_async(self, key, command, queue=None):
        self.queue.put((key, command))

    def sync(self):
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        # Sending poison pill to all worker
        [self.queue.put((None, None)) for w in self.workers]
        # Wait for commands to finish
        self.queue.join()
