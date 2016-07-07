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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import signal
import threading
import datetime
import sys

from builtins import object

from airflow.exceptions import AirflowTaskTimeout


class timeout(object):
    """
    To be used in a ``with`` block and timeout its content.
    """
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds_before_timeout = seconds
        self.error_message = error_message
        self.use_signal = isinstance(
            threading.current_thread(), threading._MainThread)

    def handle_timeout(self, signum, frame):
        logging.error("Process timed out")
        raise AirflowTaskTimeout(self.error_message)

    # Signal-based implementation

    def enter_signal(self):
        try:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds_before_timeout)
        except ValueError as e:
            logging.warning("timeout can't be used in the current context")
            logging.exception(e)

    def exit_signal(self):
        try:
            signal.alarm(0)
        except ValueError as e:
            logging.warning("timeout can't be used in the current context")
            logging.exception(e)

    # Settrace-based implementation

    def enter_settrace(self):

        self.start_time = datetime.datetime.now()
        self.counter = 0

        def trace(frame, event, arg):
            self.counter += 1

            if self.counter == 1000:
                self.counter = 0
                if datetime.datetime.now() - self.start_time > datetime.timedelta(seconds=self.seconds_before_timeout):
                    raise AirflowTaskTimeout(self.error_message)

            return trace

        sys.settrace(trace)

    def exit_settrace(self):
        sys.settrace(None)

    # Dispatch

    def __enter__(self):
        if self.use_signal:
            self.enter_signal()
        else:
            self.enter_settrace()

    def __exit__(self, type, value, traceback):
        if self.use_signal:
            self.exit_signal()
        else:
            self.exit_settrace()
