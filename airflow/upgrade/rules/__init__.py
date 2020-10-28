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
    for root, subdirs, _ in os.walk(path):
        files = os.listdir(root)
        submodule = root.replace(path, "").replace("/", ".")
        rule_classes.extend(walk_files(files, submodule))
    return rule_classes


def walk_files(files, submodule):
    res = []
    for file in files:
        if not file.endswith(".py") or file in ("__init__.py", "base_rule.py"):
            continue
        py_file = file[:-3]
        if submodule:
            py_file = submodule[1:] + "." + py_file
        full_path = ".".join([__name__, py_file])
        mod = __import__(full_path, fromlist=[py_file])
        classes = [getattr(mod, x) for x in dir(mod) if isinstance(getattr(mod, x), type)]
        for cls in classes:
            bases = [b.__name__ for b in cls.__bases__]
            if cls.__name__ != "BaseRule" and "BaseRule" in bases:
                res.append(cls)
    return res
