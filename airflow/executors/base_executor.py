from builtins import range
from builtins import object
import logging

from airflow.utils import State
from airflow.configuration import conf

PARALLELISM = conf.getint('core', 'PARALLELISM')


class BaseExecutor(object):

    def __init__(self, parallelism=PARALLELISM):
        """
        Class to derive in order to interface with executor-type systems
        like Celery, Mesos, Yarn and the likes.

        :param parallelism: how many jobs should run at one time. Set to
            ``0`` for infinity
        :type parallelism: int
        """
        self.parallelism = parallelism
        self.queued_tasks = {}
        self.running = {}
        self.event_buffer = {}

    def start(self):  # pragma: no cover
        """
        Executors may need to get things started. For example LocalExecutor
        starts N workers.
        """
        pass

    def queue_command(self, key, command, priority=1, queue=None):
        if key not in self.queued_tasks and key not in self.running:
            logging.info("Adding to queue: " + command)
            self.queued_tasks[key] = (command, priority, queue)

    def queue_task_instance(
            self, task_instance, mark_success=False, pickle_id=None,
            force=False, ignore_dependencies=False, task_start_date=None):
        command = task_instance.command(
            local=True,
            mark_success=mark_success,
            force=force,
            ignore_dependencies=ignore_dependencies,
            task_start_date=task_start_date,
            pickle_id=pickle_id)
        self.queue_command(
            task_instance.key,
            command,
            priority=task_instance.task.priority_weight_total,
            queue=task_instance.task.queue)

    def sync(self):
        """
        Sync will get called periodically by the heartbeat method.
        Executors should override this to perform gather statuses.
        """
        pass

    def heartbeat(self):
        # Calling child class sync method
        logging.debug("Calling the {} sync method".format(self.__class__))
        self.sync()

        # Triggering new jobs
        if not self.parallelism:
            open_slots = len(self.queued_tasks)
        else:
            open_slots = self.parallelism - len(self.running)

        logging.debug("{} running task instances".format(len(self.running)))
        logging.debug("{} in queue".format(len(self.queued_tasks)))
        logging.debug("{} open slots".format(open_slots))

        sorted_queue = sorted(
            [(k, v) for k, v in self.queued_tasks.items()],
            key=lambda x: x[1][1],
            reverse=True)
        for i in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, priority, queue) = sorted_queue.pop(0)
            self.running[key] = command
            del self.queued_tasks[key]
            self.execute_async(key, command=command, queue=queue)

    def change_state(self, key, state):
        del self.running[key]
        self.event_buffer[key] = state

    def fail(self, key):
        self.change_state(key, State.FAILED)

    def success(self, key):
        self.change_state(key, State.SUCCESS)

    def get_event_buffer(self):
        """
        Returns and flush the event buffer
        """
        d = self.event_buffer
        self.event_buffer = {}
        return d

    def execute_async(self, key, command, queue=None):  # pragma: no cover
        """
        This method will execute the command asynchronously.
        """
        raise NotImplementedError()

    def end(self):  # pragma: no cover
        """
        This method is called when the caller is done submitting job and is
        wants to wait synchronously for the job submitted previously to be
        all done.
        """
        raise NotImplementedError()
