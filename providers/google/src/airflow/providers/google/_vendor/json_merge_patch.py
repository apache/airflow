# BSD 3-Clause "New" or "Revised" License
#
# Copyright (c) 2015, Open Data Services Coop
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from collections import OrderedDict
import sys

def merge(*objs, **kw):
    result = objs[0]
    for obj in objs[1:]:
        result = merge_obj(result, obj, kw.get('position'))
    return result

def move_to_start(result, key):
    result_copy = result.copy()
    result.clear()
    result[key] = result_copy.pop(key)
    result.update(result_copy)

def merge_obj(result, obj, position=None):
    if not isinstance(result, dict):
        result = OrderedDict() if position else {}

    if not isinstance(obj, dict):
        return obj

    if position:
        if position not in ('first', 'last'):
            raise ValueError("position can either be first or last")
        if not isinstance(result, OrderedDict) or not isinstance(obj, OrderedDict):
            raise ValueError("If using position all dicts need to be OrderedDicts")

    for key, value in obj.items():
        if isinstance(value, dict):
            target = result.get(key)
            if isinstance(target, dict):
                merge_obj(target, value, position)
                continue
            result[key] = OrderedDict() if position else {}
            if position and position == 'first':
                if sys.version_info >= (3, 2):
                    result.move_to_end(key, False)
                else:
                    move_to_start(result, key)
            merge_obj(result[key], value, position)
            continue
        if value is None:
            result.pop(key, None)
            continue
        if key not in result and position == 'first':
            result[key] = value
            if sys.version_info >= (3, 2):
                result.move_to_end(key, False)
            else:
                move_to_start(result, key)
        else:
            result[key] = value

    return result

def create_patch(source, target):
    return create_patch_obj(source, target)

def create_patch_obj(source, target):
    if not isinstance(target, dict) or not isinstance(source, dict):
        return target

    result = {}

    for key in set(source.keys()) - set(target.keys()):
        result[key] = None

    for key, value in target.items():
        if key not in source:
            result[key] = value
            continue
        if value == source[key]:
            continue
        result[key] = create_patch_obj(source[key], value)
    return result
