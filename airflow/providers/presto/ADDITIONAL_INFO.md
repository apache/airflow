<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

## Additional limitations

The presto provider might not work correctly with Kerberos + SSL when it is installed together with the
Snowflake provider. It's because of Snowflake monkeypatching the urllib3
library as described in [this issue](https://github.com/snowflakedb/snowflake-connector-python/issues/324)
the offending code is [here](https://github.com/snowflakedb/snowflake-connector-python/blob/133d6215f7920d304c5f2d466bae38127c1b836d/src/snowflake/connector/network.py#L89-L92)

In the future Snowflake plans to get rid of the monkeypatching.

You can keep track of [the issue](https://github.com/apache/airflow/issues/12881) in order to know when the
issue will be resolved.
