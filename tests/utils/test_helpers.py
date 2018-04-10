# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import multiprocessing
import os
import psutil
import signal
import time
import unittest

from airflow.utils import helpers

class TestHelpers(unittest.TestCase):

    @staticmethod
    def _ignores_sigterm(child_pid, child_setup_done):
        def signal_handler(signum, frame):
            pass
        signal.signal(signal.SIGTERM, signal_handler)
        child_pid.value = os.getpid()
        child_setup_done.release()
        while True:
            time.sleep(1)

    @staticmethod
    def _parent_of_ignores_sigterm(parent_pid, child_pid, setup_done):
        def signal_handler(signum, frame):
            pass
        signal.signal(signal.SIGTERM, signal_handler)
        child_setup_done = multiprocessing.Semaphore(0)
        child = multiprocessing.Process(target=TestHelpers._ignores_sigterm,
                                        args=[child_pid, child_setup_done])
        child.start()
        child_setup_done.acquire(timeout=5.0)
        parent_pid.value = os.getpid()
        setup_done.release()
        while True:
            time.sleep(1)

    def test_kill_process_tree(self):
        """Spin up a process that can't be killed by SIGTERM and make sure it gets killed anyway."""
        parent_setup_done = multiprocessing.Semaphore(0)
        parent_pid = multiprocessing.Value('i', 0)
        child_pid = multiprocessing.Value('i', 0)
        args = [parent_pid, child_pid, parent_setup_done]
        parent = multiprocessing.Process(target=TestHelpers._parent_of_ignores_sigterm,
                                         args=args)
        try:
            parent.start()
            self.assertTrue(parent_setup_done.acquire(timeout=5.0))
            self.assertTrue(psutil.pid_exists(parent_pid.value))
            self.assertTrue(psutil.pid_exists(child_pid.value))

            helpers.kill_process_tree(logging.getLogger(), parent_pid.value, timeout=1)

            self.assertFalse(psutil.pid_exists(parent_pid.value))
            self.assertFalse(psutil.pid_exists(child_pid.value))
        finally:
            try:
                os.kill(parent_pid.value, signal.SIGKILL)  # terminate doesnt work here
                os.kill(child_pid.value, signal.SIGKILL)  # terminate doesnt work here
            except OSError:
                pass

    def test_kill_processes(self):
        """Test when no process exists."""
        child_pid = multiprocessing.Value('i', 0)
        setup_done = multiprocessing.Semaphore(0)
        args = [child_pid, setup_done]
        child = multiprocessing.Process(target=TestHelpers._ignores_sigterm, args=args)
        child.start()

        self.assertTrue(setup_done.acquire(timeout=1.0))
        pid_to_kill = child_pid.value
        p = psutil.Process(pid_to_kill)
        dead, alive = helpers.kill_processes(logging.getLogger(), [p], sig=signal.SIGKILL)
        self.assertEqual(len(dead), 1)
        self.assertEqual(len(alive), 0)

        child.join() # remove orphan process

        # can kill already dead process
        dead, alive = helpers.kill_processes(logging.getLogger(), [p], sig=signal.SIGKILL)
        self.assertEqual(len(dead), 1)
        self.assertEqual(len(alive), 0)


if __name__ == '__main__':
    unittest.main()
