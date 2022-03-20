"""
Copyright (c) 2014, CloudSigma AG
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the CloudSigma AG nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL CloudSigma AG BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
from collections import Iterable
from airflow._vendor.cgroupspy.contenttypes import BaseContentType


class BaseFileInterface(object):

    """
    Basic cgroups file interface, implemented as a python descriptor. Provides means to get and set cgroup properties.
    """
    readonly = False
    writeonly = False

    def __init__(self, filename, readonly=None, writeonly=None):
        if readonly and writeonly:
            raise RuntimeError("This interface cannot be both readonly and writeonly")

        try:
            self.filename = filename.encode()
        except AttributeError:
            self.filename = filename
        self.readonly = readonly or self.readonly
        self.writeonly = writeonly or self.writeonly

    def __get__(self, instance, owner):
        if self.writeonly:
            raise RuntimeError("This interface is writeonly")

        value = instance.get_property(self.filename)
        return self.sanitize_get(value)

    def __set__(self, instance, value):
        if self.readonly:
            raise RuntimeError("This interface is readonly")

        value = self.sanitize_set(value)
        if value is not None:
            return instance.set_property(self.filename, value)

    def sanitize_get(self, value):
        return value

    def sanitize_set(self, value):
        return value


class FlagFile(BaseFileInterface):

    """
    Converts True/False to 1/0 and vise versa.
    """

    def sanitize_get(self, value):
        return bool(int(value))

    def sanitize_set(self, value):
        return int(bool(value))


class BitFieldFile(BaseFileInterface):

    """
    Example: '2' becomes [False, True, False, False, False, False, False, False]
    """

    def sanitize_get(self, value):
        v = int(value)
        # Calculate the length of the value in bits by converting to hex
        length = (len(hex(v)) - 2) * 4
        # Increase length to the next multiple of 8
        length += (7 - (length - 1) % 8)
        return [bool((v >> i) & 1) for i in range(length)]

    def sanitize_set(self, value):
        try:
            value = value.encode()
        except AttributeError:
            pass
        if isinstance(value, bytes) or not isinstance(value, Iterable):
            return int(value)
        return sum((int(bool(value[i])) << i) for i in range(len(value)))


class IntegerFile(BaseFileInterface):

    """
    Get/set single integer values.
    """

    def sanitize_get(self, value):
        val = int(value)
        if val == -1:
            val = None
        return val

    def sanitize_set(self, value):
        if value is None:
            value = -1
        return int(value)


class DictFile(BaseFileInterface):
    def sanitize_get(self, value):
        res = {}
        for el in value.split("\n"):
            key, val = el.split()
            res[key] = int(val)
        return res

    def sanitize_set(self, value):
        if not isinstance(value, dict):
            raise ValueError("Value {} must be a dict".format(value))

        keys = sorted(value.keys())
        return "\n".join("{} {}".format(k, value[k]) for k in keys)


class ListFile(BaseFileInterface):
    readonly = True

    def sanitize_get(self, value):
        return value.split()


class IntegerListFile(ListFile):

    """
    ex: 253237230463342 317756630269369 247294096796305 289833051422078
    """

    def sanitize_get(self, value):
        value_list = super(IntegerListFile, self).sanitize_get(value)
        return list(map(int, value_list))

    def sanitize_set(self, value):
        if value is None:
            value = '-1'

        return " ".join([str(v) for v in value])


class CommaDashSetFile(BaseFileInterface):

    """
    Builds a set from files containing the following data format 'cpuset.cpus: 1-3,6,11-15',
    returning {1,2,3,5,11,12,13,14,15}
    """

    def sanitize_get(self, value):
        elems = []
        for el_group in value.strip().split(','):
            if "-" in el_group:
                start, end = el_group.split("-")
                for el in range(int(start), int(end) + 1):
                    elems.append(el)
            else:
                if el_group != '':
                    elems.append(int(el_group))
        return set(elems)

    def sanitize_set(self, value):
        if len(value) == 0:
            return ' '

        try:
            value = value.decode()
        except AttributeError:
            pass

        if isinstance(value, str):
            value = value.strip()
            if not value:
                return ' '
            value = value.split(',')

        if isinstance(value, Iterable):
            value = set(value)
        else:
            raise ValueError("Value {} must be a sequence of int".format(value))

        for k in value:
            if not isinstance(k, int):
                raise ValueError("Value {} must be a sequence of int".format(value))

        value = sorted(list(value))
        index = [i for i in range(len(value))]
        for i in range(len(value)):
            if index[i] != i:
                continue

            j = i

            while j < len(value) - 1:
                if value[j] + 1 == value[j + 1]:
                    index[j + 1] = i
                    j += 1
                else:
                    break

        parts = []
        for i in range(len(value)):
            if i > 0 and index[i - 1] == index[i]:
                continue

            j = i

            while j + 1 < len(value) and index[j + 1] == index[j]:
                j += 1

            if i == j:
                parts.append(str(value[i]))
            else:
                parts.append('{}-{}'.format(value[i], value[j]))

        return ','.join(parts)


class MultiLineIntegerFile(BaseFileInterface):

    def sanitize_get(self, value):
        int_list = [int(val) for val in value.strip().split("\n") if val]
        return int_list

    def sanitize_set(self, value):
        if value is None:
            return '-1'

        return '\n'.join(str(x) for x in value)


class SplitValueFile(BaseFileInterface):
    """
    Example: Getting int(10) for file with value 'Total 10'. Readonly.
    """
    readonly = True

    def __init__(self, filename, position, restype=None, splitchar=" ", prefix="Total"):
        super(SplitValueFile, self).__init__(filename)
        self.position = position
        self.restype = restype
        self.splitchar = splitchar
        self.prefix = prefix

    def sanitize_get(self, value):
        res = value.strip().split(self.splitchar)[self.position]
        if self.restype and not isinstance(res, self.restype):
            return self.restype(res)
        return res

    def sanitize_set(self, value):
        return '{}{}{}'.format(self.prefix, self.splitchar, value)


class TypedFile(BaseFileInterface):

    def __init__(self, filename, contenttype, readonly=None, writeonly=None, many=False):
        if not issubclass(contenttype, BaseContentType):
            raise RuntimeError("Contenttype should be a class inheriting "
                               "from BaseContentType, not {}".format(contenttype))

        self.contenttype = contenttype
        self.many = many
        super(TypedFile, self).__init__(filename, readonly=readonly, writeonly=writeonly)

    def sanitize_set(self, value):
        if isinstance(value, self.contenttype):
            return str(value)

        if self.many:
            items = []
            for entry in value:
                if isinstance(entry, str):
                    entry = entry.strip()
                    if not entry:
                        continue
                    items.append(str(self.contenttype.from_string(entry)))
                else:
                    items.append(str(entry))
            return "\n".join(items)
        else:
            return str(self.contenttype.from_string(value))

    def sanitize_get(self, value):
        if self.many:
            result = []
            for line in value.splitlines():
                line = line.strip()
                if not line:
                    continue
                result.append(self.contenttype.from_string(line))
            return result

        return self.contenttype.from_string(value)


class DictOrFlagFile(BaseFileInterface):
    def __init__(self, filename, readonly=None, writeonly=None):
        super(DictOrFlagFile, self).__init__(filename, readonly=readonly, writeonly=writeonly)
        self.interfaces = {
            'dict': DictFile(filename),
            'flag': FlagFile(filename),
        }

    def sanitize_get(self, value):
        try:
            return self.interfaces['dict'].sanitize_get(value)
        except Exception:
            return self.interfaces['flag'].sanitize_get(value)

    def sanitize_set(self, value):
        try:
            return self.interfaces['dict'].sanitize_set(value)
        except Exception:
            return self.interfaces['flag'].sanitize_set(value)
