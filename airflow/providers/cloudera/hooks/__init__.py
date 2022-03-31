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
"""Covers root exception for CDP hooks"""
from typing import Optional


class CdpHookException(Exception):
    """Root exception for custom Cloudera hooks, which is used to handle any known exceptions"""

    def __init__(self, raised_from: Optional[Exception] = None, msg: Optional[str] = None) -> None:
        super().__init__(raised_from, msg)
        self.raised_from = raised_from

    def __str__(self) -> str:
        return self.__repr__()
