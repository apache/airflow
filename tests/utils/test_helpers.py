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
    def _ignores_sigterm(child_pid, setup_done):
        def signal_handler(signum, frame):
            pass

        signal.signal(signal.SIGTERM, signal_handler)
        child_pid.value = os.getpid()
        setup_done.release()
        while True:
            time.sleep(1)

    @staticmethod
    def _parent_of_ignores_sigterm(child_process_killed, child_pid,
                                   process_done, setup_done):
        child = multiprocessing.Process(target=TestHelpers._ignores_sigterm,
                                        args=[child_pid, setup_done])
        child.start()
        if setup_done.acquire(timeout=1.0):
            helpers.kill_process_tree(logging.getLogger(), os.getpid(), timeout=1.0)
            # Process.is_alive doesnt work with SIGKILL
            if not psutil.pid_exists(child_pid.value):
                child_process_killed.value = 1

        process_done.release()

    def test_kill_process_tree(self):
        """Spin up a process that can't be killed by SIGTERM and make sure it gets killed anyway."""
        child_process_killed = multiprocessing.Value('i', 0)
        process_done = multiprocessing.Semaphore(0)
        child_pid = multiprocessing.Value('i', 0)
        setup_done = multiprocessing.Semaphore(0)
        args = [child_process_killed, child_pid, process_done, setup_done]
        child = multiprocessing.Process(target=TestHelpers._parent_of_ignores_sigterm, args=args)
        try:
            child.start()
            self.assertTrue(process_done.acquire(timeout=5.0))
            self.assertEqual(1, child_process_killed.value)
        finally:
            try:
                os.kill(child_pid.value, signal.SIGKILL) # terminate doesnt work here
            except OSError:
                pass

    def test_kill_using_shell(self):
        """Test when no process exists."""
        child_pid = multiprocessing.Value('i', 0)
        setup_done = multiprocessing.Semaphore(0)
        args = [child_pid, setup_done]
        child = multiprocessing.Process(target=TestHelpers._ignores_sigterm, args=args)
        child.start()

        self.assertTrue(setup_done.acquire(timeout=1.0))
        pid_to_kill = child_pid.value
        self.assertTrue(helpers.kill_using_shell(logging.getLogger(), pid_to_kill,
                                                 signal=signal.SIGKILL))
        child.join() # remove orphan process
        self.assertFalse(helpers.kill_using_shell(logging.getLogger(), pid_to_kill,
                                                  signal=signal.SIGKILL))


if __name__ == '__main__':
    unittest.main()
