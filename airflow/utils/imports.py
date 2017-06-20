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

import importlib


def import_class_by_name(name):
    """
    Return a reference to a class given its full class name.
    Example:
        name = module.submodule.MyClass
        will return a reference to MyClass, which can be instantiate
        using my_class = MyClass(<params>)
    """
    parsed = name.rsplit('.', 1)
    if len(parsed) != 2:
        raise ValueError("Invalid class name {} to import.".format(name))
    module_name, cls_name = parsed
    module = importlib.import_module(module_name)
    cls = getattr(module, cls_name)
    return cls
