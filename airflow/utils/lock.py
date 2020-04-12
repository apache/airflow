# -*- coding: utf-8 -*-
"""
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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import uuid

from redlock import RedLock


class Lock(RedLock):
    """Sustainable lock
    """
    ACQUIRE_LUA_SCRIPT = """
        local key = KEYS[1]
        local content = KEYS[2]
        local ttl     = ARGV[1]
        local lockSet = redis.call('setnx', key, content)
        if lockSet == 1 then
            redis.call('pexpire', key, ttl)
        else
            local value = redis.call('get', key)
            if(value == content) then
                lockSet = 1;
                redis.call('pexpire', key, ttl)
            end
        end
        return lockSet
    """

    def __init__(self, *args, **kwargs):
        super(Lock, self).__init__(*args, **kwargs)
        self.stable_lock_key = uuid.uuid4().hex
        for node in self.redis_nodes:
            node._acquire_script = node.register_script(self.ACQUIRE_LUA_SCRIPT)

    def acquire_node(self, node):
        """Acquire resource by lua script
        """
        # overwrite lock_key by stable_lock_key
        self.lock_key = self.stable_lock_key
        # use the lua script to acquire the sustainable lock in a safe way
        return node._acquire_script(keys=[self.resource, self.lock_key], args=[self.ttl])

    def release_node(self, node):
        """
        release a single redis node
        """
        # overwrite lock_key by stable_lock_key
        self.lock_key = self.stable_lock_key
        # use the lua script to release the lock in a safe way
        node._release_script(keys=[self.resource], args=[self.lock_key])
