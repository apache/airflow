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


class BaseDepContext(object):
    """
    A base class for contexts that specifies which dependencies should be evaluated in
    the context for a task instance to satisfy the requirements of the context. Also
    stores state related to the context that can be used by dependendency classes.

    For example there could be a SomeRunContext that subclasses this class which has
    dependencies for:
    - Making sure there are slots available on the infrastructure to run the task instance
    - A task-instance's task-specific dependencies are met (e.g. the previous task
      instance completed successfully)
    - ...

    :param flag_upstream_failed: This is a hack to generate the upstream_failed state
        creation while checking to see whether the task instance is runnable. It was the
        shortest path to add the feature. This is bad since this class should be pure (no
        side effects).
    :type flag_upstream_failed: boolean
    :param ignore_all_deps: Whether or not the context should ignore all ignoreable
        dependencies. Overrides the other ignore_* parameters
    :type ignore_all_deps: boolean
    :param ignore_depends_on_past: Whether or not the depends_on_past parameter of DAGs
        should be ignored (e.g. for Backfills)
    :type ignore_depends_on_past: boolean
    """
    def __init__(
            self,
            flag_upstream_failed=False,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False):
        self._ignore_all_deps = ignore_all_deps
        self._ignore_task_deps = ignore_task_deps
        self.flag_upstream_failed = flag_upstream_failed
        self.ignore_depends_on_past = ignore_depends_on_past

    def deps_for_ti(self, ti):
        """
        Get the context's dependencies for a given task instance. By default we just
        return the task instance's task's dependencies.

        :param ti: The task instance for which to get the dependencies for in the given
            context.
        :type ti: TaskInstance
        """
        if self._ignore_all_deps:
            return self.get_nonignoreable_deps()
        else:
            return self.get_nonignoreable_deps() | self.get_ignoreable_deps(ti)

    def get_ignoreable_deps(self, ti):
        """
        Get the dependencies for a given task that cannot be ignored by the context's
        'ignore_all_deps' attribute.

        :param ti: The task instance for which to get the dependencies for in the given
            context.
        :type ti: TaskInstance
        """
        return set() if self._ignore_task_deps else ti.task.deps

    def get_nonignoreable_deps(self):
        """
        Get the dependencies of the context that can be ignored by the context's
        'ignore_all_deps' attribute.
        """
        return set()
