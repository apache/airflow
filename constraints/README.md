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

Sometimes you might want to update dependencies of airflow in bulk, when working on related set of
packages/dependencies. This might lead to excessive time needed to rebuild the image with new versions
of dependencies (due to `pip` backtracking) and necessity to rebuild the image multiple times
and solving conflicting dependencies.

For development use only, you can store your own version of constraint files used during CI image build
here and refer to them during building of the image using `--airflow-constraints-location constraints/constraints.txt`
This allows you to iterate on dependencies without having to run `--upgrade-to-newer-dependencies` flag continuously.

Typical workflow in this case is:

* download and copy the constraint file to the folder (for example via
[The GitHub Raw Link](https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.8.txt)
* modify the constraint file in "constraints" folder
* build the image using this command

```bash
breeze ci-image build --python 3.8 --airflow-constraints-location constraints/constraints-3.8txt
```

You can continue iterating and updating the constraint file (and rebuilding the image)
while iterating, you can also manually add/update the dependencies and after you are done,
you can regenerate the set of constraints based on your currently installed packages, using this command:

```bash
pip freeze | sort | \
    grep -v "apache_airflow" | \
    grep -v "apache-airflow==" | \
    grep -v "@" | \
    grep -v "/opt/airflow" > /opt/airflow/constraints/constraints-3.8.txt
```

If you are working with others on updating the dependencies, you can also commit the constraint
file to the repository and then anyone else working on the same branch/fork, can use it for local
image building (but the file should be removed before the branch you are working on is merged to
the main branch).
