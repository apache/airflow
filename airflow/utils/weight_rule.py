# -*- coding: utf-8 -*-
#
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

from typing import Set


class WeightRule:
    """
    Weight rules.
    """
    DOWNSTREAM = 'downstream'
    UPSTREAM = 'upstream'
    ABSOLUTE = 'absolute'

    _ALL_WEIGHT_RULES = set()  # type: Set[str]

    @classmethod
    def is_valid(cls, weight_rule):
        """
        Check if weight rule is valid.
        """
        return weight_rule in cls.all_weight_rules()

    @classmethod
    def all_weight_rules(cls):
        """
        Returns all weight rules
        """
        if not cls._ALL_WEIGHT_RULES:
            cls._ALL_WEIGHT_RULES = {
                getattr(cls, attr)
                for attr in dir(cls)
                if not attr.startswith("_") and not callable(getattr(cls, attr))
            }
        return cls._ALL_WEIGHT_RULES
