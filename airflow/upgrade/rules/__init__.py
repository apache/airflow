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
import os


def get_rules():
    """Automatically discover all rules"""
    rule_classes = []
    path = os.path.dirname(os.path.abspath(__file__))
    for file in sorted(os.listdir(path)):
        if not file.endswith(".py") or file in ("__init__.py", "base_rule.py"):
            continue
        py_file = file[:-3]
        mod = __import__(".".join([__name__, py_file]), fromlist=[py_file])
        classes = [getattr(mod, x) for x in dir(mod) if isinstance(getattr(mod, x), type)]
        for cls in classes:
            bases = [b.__name__ for b in cls.__bases__]
            if cls.__name__ != "BaseRule" and "BaseRule" in bases:
                rule_classes.append(cls)
    # Sort rules alphabetically by class name, while maintaining that the airflow version
    # check should remain first
    return rule_classes[:1] + sorted(rule_classes[1:], key=lambda r: r.__name__)
