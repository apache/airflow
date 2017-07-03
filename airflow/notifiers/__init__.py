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

from abc import ABCMeta, abstractmethod


class AirflowNotifier(object):
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def send_notification(self, task_instance, state, message, **kwargs):
        pass

    def render_template(self, template, task_instance, **kwargs):
        rt = task_instance.task.render_template
        jinja_context = task_instance.get_template_context()
        
        for key, value in kwargs:
            jinja_context[key] = value
            
        return rt(None, template, jinja_context)
            