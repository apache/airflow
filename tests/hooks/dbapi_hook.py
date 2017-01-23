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
#

import unittest
from airflow.hooks.jdbc_hook import JdbcHook

class TestDbApiHook(unittest.TestCase):
    
    def test_skip_commit_for_autocommit(self):
        hook = JdbcHook(jdbc_conn_id='jdbc_default')
        
        hook.supports_autocommit = True
        hook.run(sql='select 1 from dual', autocommit=False)
        self.assertTrue('Query ran success with supports_autocommit=True, autocommit=False')
        hook.run(sql='select 1 from dual', autocommit=True)
        self.assertTrue('Query ran success with supports_autocommit=True, autocommit=True')

        hook.supports_autocommit = False
        hook.run(sql='select 1 from dual', autocommit=False)
        self.assertTrue('Query ran success with supports_autocommit=False, autocommit=False')
        hook.run(sql='select 1 from dual', autocommit=True)
        self.assertTrue('Query ran success with supports_autocommit=False, autocommit=True')

if __name__ == '__main__':
    unittest.main()
