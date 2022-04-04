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
DISCLAIMED. IN NO EVENT SHALL CLOUDSIGMA AG BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""


class BaseContentType(object):

    def __str__(self):
        raise NotImplementedError("Please implement this method in subclass")

    def __repr__(self):
        return "<{self.__class__.__name__}: {self}>".format(self=self)

    @classmethod
    def from_string(cls, value):
        raise NotImplementedError("This method should return an instance of the content type")


class DeviceAccess(BaseContentType):
    TYPE_ALL = "all"
    TYPE_CHAR = "c"
    TYPE_BLOCK = "b"

    ACCESS_UNSPEC = 0
    ACCESS_READ = 1
    ACCESS_WRITE = 2
    ACCESS_MKNOD = 4

    def __init__(self, dev_type=None, major=None, minor=None, access=None):
        self.dev_type = dev_type or self.TYPE_ALL

        # the default behaviour of device access cgroups if unspecified is as follows
        if major is not None:
            self.major = int(major)
        else:
            self.major = "*"

        if minor is not None:
            self.minor = int(minor)
        else:
            self.minor = "*"

        if isinstance(access, str):
            value = 0
            if 'r' in access:
                value |= self.ACCESS_READ
            if 'w' in access:
                value |= self.ACCESS_WRITE
            if 'm' in access:
                value |= self.ACCESS_MKNOD
            self.access = value
        else:
            self.access = access or (self.ACCESS_READ | self.ACCESS_WRITE | self.ACCESS_MKNOD)

    def _check_access_bit(self, offset):
        mask = 1 << offset
        return self.access & mask

    @property
    def can_read(self):
        return self._check_access_bit(0) == self.ACCESS_READ

    @property
    def can_write(self):
        return self._check_access_bit(1) == self.ACCESS_WRITE

    @property
    def can_mknod(self):
        return self._check_access_bit(2) == self.ACCESS_MKNOD

    @property
    def access_string(self):
        accstr = ""
        if self.can_read:
            accstr += "r"
        if self.can_write:
            accstr += "w"
        if self.can_mknod:
            accstr += "m"
        return accstr

    def __str__(self):
        return "{self.dev_type} {self.major}:{self.minor} {self.access_string}".format(self=self)

    def __eq__(self, other):
        return self.dev_type == other.dev_type and self.major == other.major \
               and self.minor == other.minor and self.access_string == other.access_string

    @classmethod
    def from_string(cls, value):
        dev_type, major_minor, access_string = value.split()
        major, minor = major_minor.split(":")
        major = int(major) if major != "*" else None
        minor = int(minor) if minor != "*" else None

        access_mode = 0
        for idx, char in enumerate("rwm"):
            if char in access_string:
                access_mode |= (1 << idx)
        return cls(dev_type, major, minor, access_mode)


class DeviceThrottle(BaseContentType):

    def __init__(self, limit, major=None, minor=None, ):
        self.limit = limit

        if major is not None and major != '*':
            self.major = int(major)
        else:
            self.major = '*'

        if minor is not None and minor != '*':
            self.minor = int(minor)
        else:
            self.minor = '*'

    def __eq__(self, other):
        return self.limit == other.limit and self.major == other.major and self.minor == other.minor

    def __str__(self):
        return "{self.major}:{self.minor} {self.limit}".format(self=self)

    @classmethod
    def from_string(cls, value):
        try:
            major_minor, limit = value.split()
            major, minor = major_minor.split(":")
            return cls(int(limit), major, minor)
        except Exception:
            raise RuntimeError("Value {} cannot be converted to a string that matches the pattern: "
                               "[device major]:[device minor] [throttle limit in bytes]".format(value))
