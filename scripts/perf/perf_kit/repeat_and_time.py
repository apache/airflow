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
import contextlib
import functools
import math
import random
import signal
import time


class TimingResult:
    def __init__(self):
        self.start_time = 0
        self.end_time = 0
        self.value = 0


@contextlib.contextmanager
def timing(repeat_count: int = 1):
    """
    Measures code execution time.

    :param repeat_count: If passed, the result will be divided by the value.
    """
    result = TimingResult()
    result.start_time = time.monotonic()
    try:
        yield result
    finally:
        end_time = time.monotonic()
        diff = (end_time - result.start_time) * 1000.0

        result.end_time = end_time
        if repeat_count == 1:
            result.value = diff
            print(f"Loop time: {diff:.3f} ms")
        else:
            average_time = diff / repeat_count
            result.value = average_time
            print(f"Average time: {average_time:.3f} ms")


def repeat(repeat_count=5):
    """
    Function decorators that repeat function many times.

    :param repeat_count: The repeat count
    """

    def repeat_decorator(f):
        @functools.wraps(f)
        def wrap(*args, **kwargs):
            last_result = None
            for i in range(repeat_count):
                last_result = f(*args, **kwargs)
            return last_result

        return wrap

    return repeat_decorator


class TimeoutException(Exception):
    pass


@contextlib.contextmanager
def timeout(seconds=1):
    """
    Executes code only  limited seconds. If the code does not end during this time, it will be interrupted.

    :param seconds: Number of seconds
    """
    def handle_timeout(signum, frame):
        raise TimeoutException("Process timed out.")

    try:
        signal.signal(signal.SIGALRM, handle_timeout)
        signal.alarm(seconds)
    except ValueError:
        raise Exception("timeout can't be used in the current context")

    try:
        yield
    except TimeoutException:
        print("Process timed out.")

    finally:
        try:
            signal.alarm(0)
        except ValueError:
            raise Exception("timeout can't be used in the current context")


if __name__ == "__main__":

    def monte_carlo(total=10000):
        # Monte Carlo
        inside = 0

        for i in range(0, total):
            x2 = random.random() ** 2
            y2 = random.random() ** 2
            if math.sqrt(x2 + y2) < 1.0:
                inside += 1

        return (float(inside) / total) * 4

    # Example 1:s
    with timeout(1):
        print("Sleep 5s with 1s timeout")
        time.sleep(4)
        print(":-/")

    print()

    # Example 2:
    REPEAT_COUNT = 5

    @timing(REPEAT_COUNT)
    @repeat(REPEAT_COUNT)
    @timing()
    def pi():
        return monte_carlo()

    result = pi()
    print("PI: ", result)
    print()

    # Example 3:
    with timing():
        result = monte_carlo()

    print("PI: ", result)
