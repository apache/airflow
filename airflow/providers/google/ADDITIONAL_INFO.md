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

# Migration Guide

## 2.0.0

### Update ``google-cloud-*`` libraries

This release of the provider package contains third-party library updates, which may require updating your DAG files or custom hooks and operators, if you were using objects from those libraries. Updating of these libraries is necessary to be able to use new features made available by new versions of the libraries and to obtain bug fixes that are only available for new versions of the library.

Details are covered in the UPDATING.md files for each library, but there are some details that you should pay attention to.

| Library name | Previous constraints | Current constraints | |
| --- | --- | --- | --- |
| [``google-cloud-automl``](https://pypi.org/project/google-cloud-automl/) | ``>=0.4.0,<2.0.0`` | ``>=2.1.0,<3.0.0``  | [`UPGRADING.md`](https://github.com/googleapis/python-bigquery-automl/blob/master/UPGRADING.md) |
| [``google-cloud-bigquery-datatransfer``](https://pypi.org/project/google-cloud-bigquery-datatransfer/) | ``>=0.4.0,<2.0.0`` | ``>=3.0.0,<4.0.0``  | [`UPGRADING.md`](https://github.com/googleapis/python-bigquery-datatransfer/blob/master/UPGRADING.md) |
| [``google-cloud-datacatalog``](https://pypi.org/project/google-cloud-datacatalog/) | ``>=0.5.0,<0.8`` | ``>=3.0.0,<4.0.0``  | [`UPGRADING.md`](https://github.com/googleapis/python-datacatalog/blob/master/UPGRADING.md) |
| [``google-cloud-kms``](https://pypi.org/project/google-cloud-os-login/) | ``>=1.2.1,<2.0.0`` | ``>=2.0.0,<3.0.0``  | [`UPGRADING.md`](https://github.com/googleapis/python-kms/blob/master/UPGRADING.md) |
| [``google-cloud-os-login``](https://pypi.org/project/google-cloud-os-login/) | ``>=1.0.0,<2.0.0`` | ``>=2.0.0,<3.0.0``  | [`UPGRADING.md`](https://github.com/googleapis/python-oslogin/blob/master/UPGRADING.md) |
| [``google-cloud-pubsub``](https://pypi.org/project/google-cloud-pubsub/) | ``>=1.0.0,<2.0.0`` | ``>=2.0.0,<3.0.0``  | [`UPGRADING.md`](https://github.com/googleapis/python-pubsub/blob/master/UPGRADING.md) |
| [``google-cloud-tasks``](https://pypi.org/project/google-cloud-tasks/) | ``>=1.2.1,<2.0.0`` | ``>=2.0.0,<3.0.0``  | [`UPGRADING.md`](https://github.com/googleapis/python-tasks/blob/master/UPGRADING.md) |

### The field names use the snake_case convention

If your DAG uses an object from the above mentioned libraries passed by XCom, it is necessary to update the naming convention of the fields that are read. Previously, the fields used the CamelSnake convention, now the snake_case convention is used.

**Before:**

```python
set_acl_permission = GCSBucketCreateAclEntryOperator(
    task_id="gcs-set-acl-permission",
    bucket=BUCKET_NAME,
    entity="user-{{ task_instance.xcom_pull('get-instance')['persistenceIamIdentity']"
    ".split(':', 2)[1] }}",
    role="OWNER",
)
```

**After:**

```python
set_acl_permission = GCSBucketCreateAclEntryOperator(
    task_id="gcs-set-acl-permission",
    bucket=BUCKET_NAME,
    entity="user-{{ task_instance.xcom_pull('get-instance')['persistence_iam_identity']"
    ".split(':', 2)[1] }}",
    role="OWNER",
)
```
