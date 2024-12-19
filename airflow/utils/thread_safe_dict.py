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
from __future__ import annotations

import threading


class ThreadSafeDict:
    """Dictionary that uses a lock during operations, to ensure thread safety."""

    def __init__(self):
        self.sync_dict = {}
        self.thread_lock = threading.Lock()

    def set(self, key, value):
        with self.thread_lock:
            self.sync_dict[key] = value

    def get(self, key):
        with self.thread_lock:
            return self.sync_dict.get(key)

    def delete(self, key):
        with self.thread_lock:
            if key in self.sync_dict:
                del self.sync_dict[key]

    def clear(self):
        with self.thread_lock:
            self.sync_dict.clear()

    def get_all(self):
        with self.thread_lock:
            # Return a copy to avoid exposing the internal dictionary.
            return self.sync_dict.copy()
