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
import os
import errno
from cgroupspy.contenttypes import DeviceAccess, DeviceThrottle

from .interfaces import BaseFileInterface, FlagFile, BitFieldFile, IntegerFile, SplitValueFile, DictOrFlagFile
from .interfaces import MultiLineIntegerFile, CommaDashSetFile, DictFile, IntegerListFile, TypedFile


class Controller(object):

    """
    Base controller. Provides access to general files, existing in all cgroups and means to get/set properties
    """

    tasks = MultiLineIntegerFile("tasks")
    procs = MultiLineIntegerFile("cgroup.procs")
    notify_on_release = FlagFile("notify_on_release")
    clone_children = FlagFile("cgroup.clone_children")

    def __init__(self, node):
        self.node = node

    def filepath(self, filename):
        """The full path to a file"""

        return os.path.join(self.node.full_path, filename)

    def list_interfaces(self):
        result = {}

        for data in [self.__class__.__dict__, Controller.__dict__]:
            for k, interface in data.items():
                if not issubclass(interface.__class__, BaseFileInterface):
                    continue

                result[k] = interface

        return result

    def get_interface(self, key):
        if key in self.__class__.__dict__:
            interface = self.__class__.__dict__[key]
        elif key in Controller.__dict__:
            interface = Controller.__dict__[key]
        else:
            return None

        if not issubclass(interface.__class__, BaseFileInterface):
            return None

        return interface

    def get_property(self, filename):
        """Opens the file and reads the value"""

        with open(self.filepath(filename)) as f:
            return f.read().strip()

    def get_content(self, key):
        interface = self.get_interface(key)

        if interface is None or interface.writeonly:
            return None

        try:
            content = self.get_property(interface.filename)
        except IOError as e:
            if e.errno == errno.ENOENT:
                # does not exist
                return None
            elif e.errno == errno.EACCES:
                # cannot be read
                return None
            elif e.errno == errno.EINVAL:
                # invalid argument
                return None
            raise

        if not content.strip():
            return ''

        return interface.sanitize_get(content)

    def set_property(self, filename, value):
        """Opens the file and writes the value"""

        with open(self.filepath(filename), "w") as f:
            return f.write(str(value))


class CpuController(Controller):

    """
    Cpu cGroup controller. Provides access to

    cpu.cfs_period_us
    cpu.cfs_quota_us
    cpu.rt_period_us
    cpu.rt_runtime_us
    cpu.shares
    cpu.stat
    """
    cfs_period_us = IntegerFile("cpu.cfs_period_us")
    cfs_quota_us = IntegerFile("cpu.cfs_quota_us")
    rt_period_us = IntegerFile("cpu.rt_period_us")
    rt_runtime_us = IntegerFile("cpu.rt_runtime_us")
    shares = IntegerFile("cpu.shares")
    stat = DictFile("cpu.stat", readonly=True)


class CpuAcctController(Controller):

    """
    cpuacct.stat
    cpuacct.usage
    cpuacct.usage_percpu
    """
    acct_stat = DictFile("cpuacct.stat", readonly=True)
    usage = IntegerFile("cpuacct.usage")
    usage_percpu = IntegerListFile("cpuacct.usage_percpu", readonly=True)


class CpuSetController(Controller):

    """
    CpuSet cGroup controller. Provides access to

    cpuset.cpu_exclusive
    cpuset.cpus
    cpuset.mem_exclusive
    cpuset.mem_hardwall
    cpuset.memory_migrate
    cpuset.memory_pressure
    cpuset.memory_pressure_enabled
    cpuset.memory_spread_page
    cpuset.memory_spread_slab
    cpuset.mems
    cpuset.sched_load_balance
    cpuset.sched_relax_domain_level
    """

    cpus = CommaDashSetFile("cpuset.cpus")
    mems = CommaDashSetFile("cpuset.mems")

    cpu_exclusive = FlagFile("cpuset.cpu_exclusive")
    mem_exclusive = FlagFile("cpuset.mem_exclusive")
    mem_hardwall = FlagFile("cpuset.mem_hardwall")
    memory_migrate = FlagFile("cpuset.memory_migrate")
    memory_pressure = FlagFile("cpuset.memory_pressure")
    memory_pressure_enabled = FlagFile("cpuset.memory_pressure_enabled")
    memory_spread_page = FlagFile("cpuset.memory_spread_page")
    memory_spread_slab = FlagFile("cpuset.memory_spread_slab")
    sched_load_balance = FlagFile("cpuset.sched_load_balance")

    sched_relax_domain_level = IntegerFile("cpuset.sched_relax_domain_level")


class MemoryController(Controller):

    """
    Memory cGroup controller. Provides access to

    memory.failcnt
    memory.force_empty
    memory.limit_in_bytes
    memory.max_usage_in_bytes
    memory.memsw.failcnt
    memory.memsw.limit_in_bytes
    memory.memsw.max_usage_in_bytes
    memory.memsw.usage_in_bytes
    memory.move_charge_at_immigrate
    memory.numa_stat
    memory.oom_control
    memory.pressure_level
    memory.soft_limit_in_bytes
    memory.stat
    memory.swappiness
    memory.usage_in_bytes
    memory.use_hierarchy
    """

    failcnt = IntegerFile("memory.failcnt")
    memsw_failcnt = IntegerFile("memory.memsw.failcnt")

    limit_in_bytes = IntegerFile("memory.limit_in_bytes")
    soft_limit_in_bytes = IntegerFile("memory.soft_limit_in_bytes")
    usage_in_bytes = IntegerFile("memory.usage_in_bytes")
    max_usage_in_bytes = IntegerFile("memory.max_usage_in_bytes")

    memsw_limit_in_bytes = IntegerFile("memory.memsw.limit_in_bytes")
    memsw_usage_in_bytes = IntegerFile("memory.memsw.usage_in_bytes")
    memsw_max_usage_in_bytes = IntegerFile("memory.memsw.max_usage_in_bytes")
    swappiness = IntegerFile("memory.swappiness")

    stat = DictFile("memory.stat", readonly=True)

    use_hierarchy = FlagFile("memory.use_hierarchy")
    force_empty = FlagFile("memory.force_empty")
    oom_control = DictOrFlagFile("memory.oom_control")

    move_charge_at_immigrate = BitFieldFile("memory.move_charge_at_immigrate")

    # Requires special file interface
    # numa_stat =

    # Requires eventfd handling - https://www.kernel.org/doc/Documentation/cgroups/memory.txt
    # pressure_level =


class DevicesController(Controller):
    """
    devices.allow
    devices.deny
    devices.list
    """

    allow = TypedFile("devices.allow", DeviceAccess, writeonly=True)
    deny = TypedFile("devices.deny", DeviceAccess, writeonly=True)
    list = TypedFile("devices.list", DeviceAccess, readonly=True, many=True)


class BlkIOController(Controller):
    """
    blkio.io_merged
    blkio.io_merged_recursive
    blkio.io_queued
    blkio.io_queued_recursive
    blkio.io_service_bytes
    blkio.io_service_bytes_recursive
    blkio.io_serviced
    blkio.io_serviced_recursive
    blkio.io_service_time
    blkio.io_service_time_recursive
    blkio.io_wait_time
    blkio.io_wait_time_recursive
    blkio.leaf_weight
    blkio.leaf_weight_device
    blkio.reset_stats
    blkio.sectors
    blkio.sectors_recursive
    blkio.throttle.io_service_bytes
    blkio.throttle.io_serviced
    blkio.throttle.read_bps_device
    blkio.throttle.read_iops_device
    blkio.throttle.write_bps_device
    blkio.throttle.write_iops_device
    blkio.time
    blkio.time_recursive
    blkio.weight
    blkio.weight_device
    """

    io_merged = SplitValueFile("blkio.io_merged", 1, int)
    io_merged_recursive = SplitValueFile("blkio.io_merged_recursive", 1, int)
    io_queued = SplitValueFile("blkio.io_queued", 1, int)
    io_queued_recursive = SplitValueFile("blkio.io_queued_recursive", 1, int)
    io_service_bytes = SplitValueFile("blkio.io_service_bytes", 1, int)
    io_service_bytes_recursive = SplitValueFile("blkio.io_service_bytes_recursive", 1, int)
    io_serviced = SplitValueFile("blkio.io_serviced", 1, int)
    io_serviced_recursive = SplitValueFile("blkio.io_serviced_recursive", 1, int)
    io_service_time = SplitValueFile("blkio.io_service_time", 1, int)
    io_service_time_recursive = SplitValueFile("blkio.io_service_time_recursive", 1, int)
    io_wait_time = SplitValueFile("blkio.io_wait_time", 1, int)
    io_wait_time_recursive = SplitValueFile("blkio.io_wait_time_recursive", 1, int)
    leaf_weight = IntegerFile("blkio.leaf_weight")
    # TODO: Uncomment the following properties after researching how to interact with files
    # leaf_weight_device =
    reset_stats = IntegerFile("blkio.reset_stats")
    # sectors =
    # sectors_recursive =
    # throttle_io_service_bytes =
    # throttle_io_serviced =
    throttle_read_bps_device = TypedFile("blkio.throttle.read_bps_device", contenttype=DeviceThrottle, many=True)
    throttle_read_iops_device = TypedFile("blkio.throttle.read_iops_device", contenttype=DeviceThrottle, many=True)
    throttle_write_bps_device = TypedFile("blkio.throttle.write_bps_device ", contenttype=DeviceThrottle, many=True)
    throttle_write_iops_device = TypedFile("blkio.throttle.write_iops_device ", contenttype=DeviceThrottle, many=True)
    # time =
    # time_recursive =
    weight = IntegerFile("blkio.weight")
    # weight_device =


class NetClsController(Controller):

    """
    net_cls.classid
    """
    class_id = IntegerFile("net_cls.classid")


class NetPrioController(Controller):

    """
    net_prio.prioidx
    net_prio.ifpriomap
    """
    prioidx = IntegerFile("net_prio.prioidx", readonly=True)
    ifpriomap = DictFile("net_prio.ifpriomap")
