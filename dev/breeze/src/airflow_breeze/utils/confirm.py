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
import sys
from enum import Enum
from typing import Optional


class Answer(Enum):
    YES = (0,)
    NO = (1,)
    QUIT = 2


def user_confirm(
    message: str, timeout: float, default_answer: Optional[Answer] = None, quit_allowed: bool = True
) -> Answer:
    """
    Ask the user for confirmation.

    :param message: message to display to the user (should end with the question mark)
    :param timeout: time given user to answer
    :param default_answer: default value returned on timeout. If no default - the question is
        repeated until user answers.
    :param quit_allowed: whether quit answer is allowed
    :return:
    """
    from inputimeout import TimeoutOccurred, inputimeout

    allowed_answers = "y/n/q" if quit_allowed else "y/n"
    while True:
        try:
            user_status = inputimeout(
                prompt=f'\n{message} \nPress {allowed_answers} in {timeout} seconds: ',
                timeout=timeout,
            )
            if user_status.upper() in ['Y', 'YES']:
                return Answer.YES
            elif user_status.upper() in ['N', 'NO']:
                return Answer.NO
            elif user_status.upper() in ['Q', 'QUIT']:
                return Answer.QUIT
            else:
                print(f"Wrong answer given {user_status}. Should be one of {allowed_answers}. Try again.")
        except TimeoutOccurred:
            if default_answer is not None:
                return default_answer
            print(f"Timeout after {timeout} seconds. Try again.")
        except KeyboardInterrupt:
            if quit_allowed:
                return Answer.QUIT
            sys.exit(1)
