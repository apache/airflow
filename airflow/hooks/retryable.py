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
Abstract class for Hooks to inherit from in order
to implement certain methods needed for retrying failures.
"""


class Retryable(object):
    """
    Abstract class for Hooks to use with multiple-inheritance.
    A Hook is able to add robustness to a method by using a
    retry decorator and implementing this methods to determine
    if an error may be retried further during an infra outage.
    """

    @classmethod
    def get_retry_type(cls, fault_exc):
        """
        Determine what type of retry to perform based on the exception.
        :param fault_exc: AirflowFaultException object
        :return: Return the RetryType
        """
        raise NotImplementedError
