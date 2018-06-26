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
"""
Define classes, constants, and functions used for
retrying certain types of failures.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

from enum import Enum
from datetime import datetime, timedelta
import time
import random

from airflow import configuration as conf
from airflow.exceptions import AirflowFaultException
from airflow.configuration import AirflowConfigException
from airflow.settings import Stats
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

INFRA_RETRIES_FOR_CLASSES = None
try:
    INFRA_RETRIES_FOR_CLASSES = conf.get('operators', 'INFRA_RETRIES_FOR_CLASSES')
except AirflowConfigException:
    # Expected this when config is not defined
    pass


class RetryType(Enum):
    """
    Enum for type of retry based on the failure
    """
    SUCCESS = 0             # no need to retry
    USER_ERROR = 1          # do not retry since will result in same error
    TRANSIENT_ERROR = 2     # retry based on Operator settings
    INFRA_ERROR = 3         # retry based on Infra settings
    UNKNOWN = 4             # retry based on Operator settings


TIMEDELTA_CAP = timedelta.max.total_seconds() - 1


class FaultManager(object):
    """
    Provide classmethods for determining if a failure
    should be retried based on the Hook class and
    the type of error.
    """

    @classmethod
    def is_infra_failure(cls, op_exception):
        """
        Determine if the
        :param op_exception:
        :return:
        """
        log.info("%s: checking if the error was due to infra failure. "
                 "Throwing class: %s", cls.__name__,
                 str(op_exception.instance.__class__))

        # "instance" is a reference to a Hook object that must
        # implement two class methods: get_fault, get_retry_type
        retry_type = op_exception.instance.get_retry_type(op_exception)
        return retry_type == RetryType.INFRA_ERROR

    @classmethod
    def is_enabled_for_class(cls, cls_name):
        """
        Determine if infra failures are enabled to retry
        for the given class name based on the config value.
        INFRA_RETRIES_FOR_CLASSES is a comma-separated list of
        classes that are enabled for retries.
        If the value is "*", then all are supported.
        :param cls_name: Class name of the instance raising the Exception.
        :return: Boolean indicating if the cls_name is enabled for infra retries.
        """
        if INFRA_RETRIES_FOR_CLASSES is None:
            return False

        if INFRA_RETRIES_FOR_CLASSES.strip() == "*":
            return True

        classes = set([name.strip() for name in INFRA_RETRIES_FOR_CLASSES.split(",")])
        return cls_name in classes


# Function decorator for robustness
def retry_infra_failure(exponential_backoff=True,
                        retry_delay_td=timedelta(minutes=5),
                        max_retry_delay_td=timedelta(minutes=40),
                        max_retry_window_td=timedelta(hours=6),
                        jitter=False,
                        jitter_factor=1.0):
    """
    :param exponential_backoff: Boolean indicating whether to
    perform exponential backoff.
    :type exponential_backoff: bool

    :param retry_delay_td: Initial retry delay. If not using exponential
    backoff, will always sleep this amount of time between retry delays.
    If using exponential backoff, delay will be
    retry_delay_td * (2^ try number), where try_number=0 on the first failure.
    :type retry_delay_td: timedelta

    :param max_retry_delay_td: Upperbound on retry_delay when using
    exponential backoff.
    :type max_retry_delay_td: timedelta

    :param max_retry_window_td: Upperbound on total time spent retrying.
    :type max_retry_window_td: timedelta

    :param jitter: Whether to perform jittering by multiplying the sleep
    time by a random number.
    :type: jitter: bool

    :param jitter_factor: If jitter is True, will pick a random number
    between 1 and the jitter_factor and multiply the effective sleep time
    by that value. Ideally, jitter_factor should be in the range [.5, 1.5]
    :type: jitter_factor: float
    """

    def real_decorator(func):
        """
        Internal function used by the retry decorator.
        :param func: Original calling function.
        :return: Return a wrapper function that can be called with "func"
        """
        def wrapper(*args, **kwargs):
            """
            Internal wrapper that actually calls func with the provides args.
            :param args: List of arguments to original function.
            :param kwargs: Dictionary of arguments to original function.
            :return: Result of calling func with the arguments.
            """
            # Initial delay in seconds
            delay_secs = retry_delay_td.total_seconds()
            max_retry_delay_secs = max_retry_delay_td.total_seconds()

            try_num = 0
            curr_time = datetime.now()
            expiration_time = curr_time + max_retry_window_td

            while curr_time < expiration_time:
                time_left = expiration_time - curr_time
                try:
                    if try_num >= 1:
                        log.info("Retrying due to infra failure. Try: %d. "
                                 "Time left: %s",
                                 try_num, str(time_left))

                    return func(*args, **kwargs)
                except AirflowFaultException as legitimate_err:
                    # This will only catch AirflowFaultException that has more
                    # information such as return code, stdout, and stderr so it
                    # can determine if the failure was user error or
                    # infrastructure-related.
                    # Any function that uses this decorator without raising an
                    # AirflowOperatorException will not see a change in behavior.

                    # Only retry if the class name of the instance raising the
                    # exception has been enabled in the Airflow config.

                    if FaultManager.is_infra_failure(legitimate_err):
                        # Useful to log this since enabling it may have
                        # prevented a failure.
                        cls_name = legitimate_err.instance.__class__.__name__
                        if not FaultManager.is_enabled_for_class(cls_name):
                            log.info("Retrying infra failures is not enabled "
                                     "for class %s", cls_name)
                            raise legitimate_err

                        Stats.incr('retry_infra_failure_{}'.
                                   format(legitimate_err.instance.__class__.__name__), 1)

                        # Calculate effective delay (sleep time)
                        actual_delay_secs = delay_secs
                        if exponential_backoff:
                            backoff_secs = int(delay_secs * (2 ** try_num))
                            # Ensure sleep time is within a reasonable range
                            backoff_secs = min(backoff_secs, TIMEDELTA_CAP)
                            actual_delay_secs = min(backoff_secs,
                                                    max_retry_delay_secs)

                        # Account for jittering
                        if jitter and jitter_factor > 0:
                            # Multiplier is random floating point number N such that
                            # a <= N <= b for a <= b
                            # and b <= N <= a for b < a
                            actual_delay_secs *= random.uniform(1.0, jitter_factor)
                            actual_delay_secs = min(actual_delay_secs,
                                                    max_retry_delay_secs)

                        curr_time = datetime.now()
                        if curr_time + timedelta(seconds=actual_delay_secs) \
                                >= expiration_time:
                            # No use in sleeping if when done the retry window will expire
                            raise legitimate_err

                        log.info("Caught an infra failure. Try %d finished. "
                                 "Sleeping for %d secs.", try_num, actual_delay_secs)

                        time.sleep(actual_delay_secs)
                        try_num += 1
                    else:
                        log.debug("Caught %s but it was not infra related",
                                  legitimate_err.__class__.__name__)
                        raise legitimate_err
                    curr_time = datetime.now()
        return wrapper
    return real_decorator
