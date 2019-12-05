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

"""
json_formatter module stores all related to ElasticSearch specific logger classes
"""

import logging
import json


def merge_dicts(d1, d2):
    """
    Merge two dicts
    """
    merged = d1.copy()
    merged.update(d2)
    return merged


class JSONFormatter(logging.Formatter):
    """
    JSONFormatter instances are used to convert a log record to json.
    """
    def __init__(self, fmt=None, datefmt=None, json_fields=None, extras=None):
        super(JSONFormatter, self).__init__(fmt, datefmt)
        if extras is None:
            extras = {}
        if json_fields is None:
            json_fields = []
        self.json_fields = json_fields
        self.extras = extras

    def format(self, record):
        super(JSONFormatter, self).format(record)
        record_dict = {label: getattr(record, label, None)
                       for label in self.json_fields}
        merged_record = merge_dicts(record_dict, self.extras)
        return json.dumps(merged_record)
