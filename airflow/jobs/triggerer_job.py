# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
import os
import signal
import sys
import threading
import time
from collections import deque
from typing import Deque, Dict, Set, Tuple, Type

from sqlalchemy import func

from airflow.compat.asyncio import create_task
from airflow.configuration import conf
from airflow.jobs.base_job import BaseJob
from airflow.models.trigger import Trigger
from airflow.stats import Stats
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.typing_compat import TypedDict
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.session import provide_session


class TriggererJob(BaseJob):
    """
    TriggererJob continuously runs active triggers in asyncio, watching
    for them to fire off their events and then dispatching that information
    to their dependent tasks/DAGs.

    It runs as two threads:
     - The main thread does DB calls/checkins
     - A subthread runs all the async code
    """

    __mapper_args__ = {'polymorphic_identity': 'TriggererJob'}

    def __init__(self, capacity=None, *args, **kwargs):
        # Call superclass
        super().__init__(*args, **kwargs)

        if capacity is None:
            self.capacity = conf.getint('triggerer', 'default_capacity', fallback=1000)
        elif isinstance(capacity, int) and capacity > 0:
            self.capacity = capacity
        else:
            raise ValueError(f"Capacity number {capacity} is invalid")

        # Set up runner async thread
        self.runner = TriggerRunner()

    def register_signals(self) -> None:
        """Register signals that stop child processes"""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    @classmethod
    @provide_session
    def is_needed(cls, session) -> bool:
        """
        Tests if the triggerer job needs to be run (i.e., if there are triggers
        in the trigger table).
        This is used for the warning boxes in the UI.
        """
        return session.query(func.count(Trigger.id)).scalar() > 0

    def on_kill(self):
        """
        Called when there is an external kill command (via the heartbeat
        mechanism, for example)
        """
        self.runner.stop = True

    def _exit_gracefully(self, signum, frame) -> None:
        """Helper method to clean up processor_agent to avoid leaving orphan processes."""
        # The first time, try to exit nicely
        if not self.runner.stop:
            self.log.info("Exiting gracefully upon receiving signal %s", signum)
            self.runner.stop = True
        else:
            self.log.warning("Forcing exit due to second exit signal %s", signum)
            sys.exit(os.EX_SOFTWARE)

    def _execute(self) -> None:
        self.log.info("Starting the triggerer")
        try:
            # Kick off runner thread
            self.runner.start()
            # Start our own DB loop in the main thread
            self._run_trigger_loop()
        except Exception:
            self.log.exception("Exception when executing TriggererJob._run_trigger_loop")
            raise
        finally:
            self.log.info("Waiting for triggers to clean up")
            # Tell the subthread to stop and then wait for it.
            # If the user interrupts/terms again, _graceful_exit will allow them
            # to force-kill here.
            self.runner.stop = True
            self.runner.join(30)
            self.log.info("Exited trigger loop")

    def _run_trigger_loop(self) -> None:
        """
        The main-thread trigger loop.

        This runs synchronously and handles all database reads/writes.
        """
        while not self.runner.stop:
            # Clean out unused triggers
            Trigger.clean_unused()
            # Load/delete triggers
            self.load_triggers()
            # Handle events
            self.handle_events()
            # Handle failed triggers
            self.handle_failed_triggers()
            # Handle heartbeat
            self.heartbeat(only_if_necessary=True)
            # Collect stats
            self.emit_metrics()
            # Idle sleep
            time.sleep(1)

    def load_triggers(self):
        """
        Queries the database to get the triggers we're supposed to be running,
        adds them to our runner, and then removes ones from it we no longer
        need.
        """
        Trigger.assign_unassigned(self.id, self.capacity)
        ids = Trigger.ids_for_triggerer(self.id)
        self.runner.update_triggers(set(ids))

    def handle_events(self):
        """
        Handles outbound events from triggers - dispatching them into the Trigger
        model where they are then pushed into the relevant task instances.
        """
        while self.runner.events:
            # Get the event and its trigger ID
            trigger_id, event = self.runner.events.popleft()
            # Tell the model to wake up its tasks
            Trigger.submit_event(trigger_id=trigger_id, event=event)
            # Emit stat event
            Stats.incr('triggers.succeeded')

    def handle_failed_triggers(self):
        """
        Handles "failed" triggers - ones that errored or exited before they
        sent an event. Task Instances that depend on them need failing.
        """
        while self.runner.failed_triggers:
            # Tell the model to fail this trigger's deps
            trigger_id, saved_exc = self.runner.failed_triggers.popleft()
            Trigger.submit_failure(trigger_id=trigger_id, exc=saved_exc)
            # Emit stat event
            Stats.incr('triggers.failed')

    def emit_metrics(self):
        Stats.gauge('triggers.running', len(self.runner.triggers))


class TriggerDetails(TypedDict):
    """Type class for the trigger details dictionary"""

    task: asyncio.Task
    name: str
    events: int


class TriggerRunner(threading.Thread, LoggingMixin):
    """
    Runtime environment for all triggers.

    Mainly runs inside its own thread, where it hands control off to an asyncio
    event loop, but is also sometimes interacted with from the main thread
    (where all the DB queries are done). All communication between threads is
    done via Deques.
    """

    # Maps trigger IDs to their running tasks and other info
    triggers: Dict[int, TriggerDetails]

    # Cache for looking up triggers by classpath
    trigger_cache: Dict[str, Type[BaseTrigger]]

    # Inbound queue of new triggers
    to_create: Deque[Tuple[int, BaseTrigger]]

    # Inbound queue of deleted triggers
    to_cancel: Deque[int]

    # Outbound queue of events
    events: Deque[Tuple[int, TriggerEvent]]

    # Outbound queue of failed triggers
    failed_triggers: Deque[Tuple[int, BaseException]]

    # Should-we-stop flag
    stop: bool = False

    def __init__(self):
        super().__init__()
        self.triggers = {}
        self.trigger_cache = {}
        self.to_create = deque()
        self.to_cancel = deque()
        self.events = deque()
        self.failed_triggers = deque()

    def run(self):
        """Sync entrypoint - just runs arun in an async loop."""
        asyncio.run(self.arun())

    async def arun(self):
        """
        Main (asynchronous) logic loop.

        The loop in here runs trigger addition/deletion/cleanup. Actual
        triggers run in their own separate coroutines.
        """
        watchdog = create_task(self.block_watchdog())
        last_status = time.time()
        while not self.stop:
            # Run core logic
            await self.create_triggers()
            await self.cancel_triggers()
            await self.cleanup_finished_triggers()
            # Sleep for a bit
            await asyncio.sleep(1)
            # Every minute, log status if at least one trigger is running.
            if time.time() - last_status >= 60:
                count = len(self.triggers)
                if count > 0:
                    self.log.info("%i triggers currently running", count)
                last_status = time.time()
        # Wait for watchdog to complete
        await watchdog

    async def create_triggers(self):
        """
        Drain the to_create queue and create all triggers that have been
        requested in the DB that we don't yet have.
        """
        while self.to_create:
            trigger_id, trigger_instance = self.to_create.popleft()
            if trigger_id not in self.triggers:
                self.triggers[trigger_id] = {
                    "task": create_task(self.run_trigger(trigger_id, trigger_instance)),
                    "name": f"{trigger_instance!r} (ID {trigger_id})",
                    "events": 0,
                }
            else:
                self.log.warning("Trigger %s had insertion attempted twice", trigger_id)
            await asyncio.sleep(0)

    async def cancel_triggers(self):
        """
        Drain the to_cancel queue and ensure all triggers that are not in the
        DB are cancelled, so the cleanup job deletes them.
        """
        while self.to_cancel:
            trigger_id = self.to_cancel.popleft()
            if trigger_id in self.triggers:
                # We only delete if it did not exit already
                self.triggers[trigger_id]["task"].cancel()
            await asyncio.sleep(0)

    async def cleanup_finished_triggers(self):
        """
        Go through all trigger tasks (coroutines) and clean up entries for
        ones that have exited, optionally warning users if the exit was
        not normal.
        """
        for trigger_id, details in list(self.triggers.items()):
            if details["task"].done():
                # Check to see if it exited for good reasons
                saved_exc = None
                try:
                    result = details["task"].result()
                except (asyncio.CancelledError, SystemExit, KeyboardInterrupt):
                    # These are "expected" exceptions and we stop processing here
                    # If we don't, then the system requesting a trigger be removed -
                    # which turns into CancelledError - results in a failure.
                    del self.triggers[trigger_id]
                    continue
                except BaseException as e:
                    # This is potentially bad, so log it.
                    self.log.exception("Trigger %s exited with error %s", details["name"], e)
                    saved_exc = e
                else:
                    # See if they foolishly returned a TriggerEvent
                    if isinstance(result, TriggerEvent):
                        self.log.error(
                            "Trigger %s returned a TriggerEvent rather than yielding it", details["name"]
                        )
                # See if this exited without sending an event, in which case
                # any task instances depending on it need to be failed
                if details["events"] == 0:
                    self.log.error(
                        "Trigger %s exited without sending an event. Dependent tasks will be failed.",
                        details["name"],
                    )
                    self.failed_triggers.append((trigger_id, saved_exc))
                del self.triggers[trigger_id]
            await asyncio.sleep(0)

    async def block_watchdog(self):
        """
        Watchdog loop that detects blocking (badly-written) triggers.

        Triggers should be well-behaved async coroutines and await whenever
        they need to wait; this loop tries to run every 100ms to see if
        there are badly-written triggers taking longer than that and blocking
        the event loop.

        Unfortunately, we can't tell what trigger is blocking things, but
        we can at least detect the top-level problem.
        """
        while not self.stop:
            last_run = time.monotonic()
            await asyncio.sleep(0.1)
            # We allow a generous amount of buffer room for now, since it might
            # be a busy event loop.
            time_elapsed = time.monotonic() - last_run
            if time_elapsed > 0.2:
                self.log.error(
                    "Triggerer's async thread was blocked for %.2f seconds, "
                    "likely by a badly-written trigger. Set PYTHONASYNCIODEBUG=1 "
                    "to get more information on overrunning coroutines.",
                    time_elapsed,
                )
                Stats.incr('triggers.blocked_main_thread')

    # Async trigger logic

    async def run_trigger(self, trigger_id, trigger):
        """
        Wrapper which runs an actual trigger (they are async generators)
        and pushes their events into our outbound event deque.
        """
        self.log.info("Trigger %s starting", self.triggers[trigger_id]['name'])
        try:
            async for event in trigger.run():
                self.log.info("Trigger %s fired: %s", self.triggers[trigger_id]['name'], event)
                self.triggers[trigger_id]["events"] += 1
                self.events.append((trigger_id, event))
        finally:
            # CancelledError will get injected when we're stopped - which is
            # fine, the cleanup process will understand that, but we want to
            # allow triggers a chance to cleanup, either in that case or if
            # they exit cleanly.
            trigger.cleanup()

    # Main-thread sync API

    def update_triggers(self, requested_trigger_ids: Set[int]):
        """
        Called from the main thread to request that we update what
        triggers we're running.

        Works out the differences - ones to add, and ones to remove - then
        adds them to the deques so the subthread can actually mutate the running
        trigger set.
        """
        # Note that `triggers` could be mutated by the other thread during this
        # line's execution, but we consider that safe, since there's a strict
        # add -> remove -> never again lifecycle this function is already
        # handling.
        running_trigger_ids = set(self.triggers.keys())
        known_trigger_ids = (
            running_trigger_ids.union(x[0] for x in self.events)
            .union(self.to_cancel)
            .union(x[0] for x in self.to_create)
            .union(trigger[0] for trigger in self.failed_triggers)
        )
        # Work out the two difference sets
        new_trigger_ids = requested_trigger_ids - known_trigger_ids
        cancel_trigger_ids = running_trigger_ids - requested_trigger_ids
        # Bulk-fetch new trigger records
        new_triggers = Trigger.bulk_fetch(new_trigger_ids)
        # Add in new triggers
        for new_id in new_trigger_ids:
            # Check it didn't vanish in the meantime
            if new_id not in new_triggers:
                self.log.warning("Trigger ID %s disappeared before we could start it", new_id)
                continue
            # Resolve trigger record into an actual class instance
            try:
                trigger_class = self.get_trigger_by_classpath(new_triggers[new_id].classpath)
            except BaseException as e:
                # Either the trigger code or the path to it is bad. Fail the trigger.
                self.failed_triggers.append((new_id, e))
                continue
            self.to_create.append((new_id, trigger_class(**new_triggers[new_id].kwargs)))
        # Enqueue orphaned triggers for cancellation
        for old_id in cancel_trigger_ids:
            self.to_cancel.append(old_id)

    def get_trigger_by_classpath(self, classpath: str) -> Type[BaseTrigger]:
        """
        Gets a trigger class by its classpath ("path.to.module.classname")

        Uses a cache dictionary to speed up lookups after the first time.
        """
        if classpath not in self.trigger_cache:
            self.trigger_cache[classpath] = import_string(classpath)
        return self.trigger_cache[classpath]
