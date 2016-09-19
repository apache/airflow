from builtins import range

from airflow import configuration
from airflow.utils.state import State
from airflow.utils.logging import LoggingMixin

PARALLELISM = configuration.getint('core', 'PARALLELISM')


class BaseExecutor(LoggingMixin):

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

    def queue_command(self, task_instance, command, priority=1, queue=None):
        key = task_instance.key
        if key not in self.queued_tasks and key not in self.running:
            self.logger.info("Adding to queue: {}".format(command))
            self.queued_tasks[key] = (command, priority, queue, task_instance)

    def queue_task_instance(
            self,
            task_instance,
            mark_success=False,
            pickle_id=None,
            force=False,
            ignore_dependencies=False,
            ignore_depends_on_past=False,
            pool=None):
        pool = pool or task_instance.pool
        command = task_instance.command(
            local=True,
            mark_success=mark_success,
            force=force,
            ignore_dependencies=ignore_dependencies,
            ignore_depends_on_past=ignore_depends_on_past,
            pool=pool,
            pickle_id=pickle_id)
        self.queue_command(
            task_instance,
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

        # Triggering new jobs
        if not self.parallelism:
            open_slots = len(self.queued_tasks)
        else:
            open_slots = self.parallelism - len(self.running)

        self.logger.debug("{} running task instances".format(len(self.running)))
        self.logger.debug("{} in queue".format(len(self.queued_tasks)))
        self.logger.debug("{} open slots".format(open_slots))

        sorted_queue = sorted(
            [(k, v) for k, v in self.queued_tasks.items()],
            key=lambda x: x[1][1],
            reverse=True)
        for i in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue, ti) = sorted_queue.pop(0)
            # TODO(jlowin) without a way to know what Job ran which tasks,
            # there is a danger that another Job started running a task
            # that was also queued to this executor. This is the last chance
            # to check if that hapened. The most probable way is that a
            # Scheduler tried to run a task that was originally queued by a
            # Backfill. This fix reduces the probability of a collision but
            # does NOT eliminate it.
            self.queued_tasks.pop(key)
            ti.refresh_from_db()
            if ti.state != State.RUNNING:
                self.running[key] = command
                self.execute_async(key, command=command, queue=queue)
            else:
                self.logger.debug(
                    'Task is already running, not sending to '
                    'executor: {}'.format(key))

        # Calling child class sync method
        self.logger.debug("Calling the {} sync method".format(self.__class__))
        self.sync()

    def change_state(self, key, state):
        self.running.pop(key)
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

    def terminate(self):
        """
        This method is called when the daemon receives a SIGTERM
        """
        raise NotImplementedError()
