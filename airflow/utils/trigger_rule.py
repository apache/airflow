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
from __future__ import unicode_literals

from builtins import object


class TriggerRule(object):
    ALL_SUCCESS = 'all_success'
    ALL_FAILED = 'all_failed'
    ALL_DONE = 'all_done'
    ONE_SUCCESS = 'one_success'
    ONE_FAILED = 'one_failed'
    DUMMY = 'dummy'

    @classmethod
    def is_valid(cls, trigger_rule):
        return trigger_rule in cls.all_triggers()

    @classmethod
    def all_triggers(cls):
        return [getattr(cls, attr)
                for attr in dir(cls)
                if not attr.startswith("__") and not callable(getattr(cls, attr))]
