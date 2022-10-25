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

# Goals

This document aims to develop and maintain consistency across the system tests in
the Amazon provider package and sets forth guidelines to use when possible.  With
the expectation that there will always be some edge cases, please refer to this
document as a guide when contributing system tests for the Amazon provider package.

Contributions are welcome and are greatly appreciated! Every bit helps, and credit
will always be given.  For example, I stole that line from @potiuk!

# Scope

This guide is meant to be used in addition to the [community system test design
guide](/tests/system/README.md) and only applies to system tests within the Amazon
provider package, though other providers are welcome to adopt them as well.  In any
cases of conflicting information or confusion, this document should take precedence
within the Amazon provider package.

# Tenants

* Keep it Self-Contained
* Keep it DAMP
* Keep it Clean
* Use the Helpers

## Keep it Self-Contained

For system tests, we would prefer to see each test be a fully self-contained unit.
This means that any infrastructure which is needed, such as an S3 bucket, must be
created and destroyed in the test's setup and teardown steps.  Some exceptions are
outlined below in the `SystemTestContextBuilder()` section.  Please document all
prerequisites for a given system test in your code for easy replication.

## Keep it DAMP

Whenever possible, avoid adding new helper methods to reuse configuration or setup
code.  This means there will be duplicate code across tests, which violates the DRY
(Don't Repeat Yourself) programming principle, but ensures that any given test can
be modified easily without having to consider what those changes may mean for every
other test.

Similarly, whenever possible, a variable should be defined in the module instead of
checking for it in external locations.  For example, if you create and break down an
S3 bucket, the bucket name should be defined in the file rather than setting it with
an environment variable.

## Keep it Clean

Code snippets in the system tests are embedded in the [documentation pages](
https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/index.html).
In order to minimize confusion for new users looking at those doc pages, the code
snippets should be concise and not include parameters which are not needed for the
Operator to function.  Any parameters which are not needed for the Operator but are
needed for the system test (`wait_for_completion` or `trigger_rule` as two examples)
should be added outside the snippet.  For example:

```
# [START howto_operator_cloudformation_delete_stack]
delete_stack = CloudFormationDeleteStackOperator(
    task_id='delete_stack',
    stack_name=cloudformation_stack_name,
)
# [END howto_operator_cloudformation_delete_stack]
delete_stack.trigger_rule = TriggerRule.ALL_DONE
```

## Use the Helpers

Some helper methods for system tests have been provided in the [utils module](aws/utils/__init__.py).

### SystemTestContextBuilder()

`SystemTestContextBuilder()` should be used as the first task in every Amazon system
test.  When used without any stub methods, it will generate and set the test's `ENV_ID`.
It will check for an environment variable defining the system test environment ID.  If
one is found, it will validate that the ID meets system test requirements.  Otherwise,
it will generate one and export a value.  For more information, see `ENV_ID` below.

Any variables which need to be fetched from an external source, such as an environment
variable or SSM, should be added with the `.add_variable()` stub method.  The method
will check a number of sources to find a value before falling back to the default.
Use this to provide values for things which must be generated before the test can run.
Whenever possible, it is preferred to use default values when creating these resources
and document the settings in the code for easy reproduction.  For example, a comment
such as "This test requires the ARN of a default EC2 instance named `system_test_instance`."
is preferred over creating an EC2 instance using custom settings to try to improve
performance or reduce expense.

Some examples of when this should be used are:

1. A resource which needs special or elevated privileges to create.  For example, an IAM Role.
2. A resource which is very time-consuming to create and would slow down tests. For example, an RDS DB.
3. A default resource which is shared across many tests.  For example, the default VPC.

### set_env_id()  [*DEPRECATED*]

**NOTE:** `set_env_id()` is deprecated as this functionality is now handled by the
`SystemTestContextBuilder()`.  Early AIP-47 system tests used this method so the
below description will remain here until those tests can be updated.

`set_env_id()`  should ~~always~~ no longer be used to set the value for `ENV_ID`.  It
will check for an environment variable defining the system test environment ID.  If one
is not found, it will generate one and export it.  If one is found, it will validate that
the ID meets system requirements.  For more information, see `ENV_ID` below.

### fetch_variable()  [*DEPRECATED*]

**NOTE:** Calling `fetch_variable()` directly is deprecated.  Instead, please use the
`.add_variable()` stub method of `SystemTestContextBuilder()`.  Early AIP-47 system
tests used this method so the below description will remain here until those tests can
be updated.

`fetch_variable()` accepts the name of a variable and an optional default value, and
will check a number of sources to find a value for it before falling back to the default.
Use this for any constant whose value is being sourced from outside the module.  This is
used to pass values in for things which must be generated before the test can run.

Some examples of when this should be used are:

1. A resource which needs special or excessively elevated privileges to create.  For example, an IAM Role.
2. A resource which would be very time expensive to create and would slow down tests For example, an RDS DB.
3. A default resource which is shared across many tests.  For example, the default VPC.

# Conventions

## Location and Naming

All system tests for the Amazon provider package should be in `tests/system/providers/amazon/aws/`.
If there is only one system test for a given service, the module name should be `example_{service}`.
For example, `example_athena.py`.  If more than one module is required, the names should be
descriptive.  For example, `example_redshift_cluster.py` and `example_redshift_sql.py`.

## Environment ID

`ENV_ID` should be set via the the `SystemTestContextBuilder` and not manually.  This
value should be used as part of any value which is test-specific such as the name of
an S3 bucket being created. For example, `BUCKET_NAME = f'{ENV_ID}-test-bucket'`.

If you choose to define an ENV_ID in your local environment rather than let the helper
generate one, the value must be lower-case alphanumeric with no special characters and
start with a letter.  This ensures maximum compatibility across Amazon services since
different services and resources have different naming requirements.

## DAG ID

`DAG_ID` should be the first constant in the module and will be used only when declaring
the DAG object.  The value should be the same as the module name.  For example, the module
`example_athena.py` should have a DAG_ID value of `example_athena`.  We declare the `DAG_ID`
as a constant as defined in AIP-47, but due to different services using different naming
conventions, the underscore in the `DAG_ID` is not always safe to use.  Therefore, for the
sake of consistency, `ENV_ID` will be used when building asset names instead.

## IAM Role ARNs

When required, an IAM Role ARN should be imported using the `add_variable()` stub with
the key `ROLE_ARN`.  `add_variable()` allows the test runner to store the values in the
environment variables or in a secret manager and standardizing the key name makes using
a secret manager easier.

## Task Dependencies

The task flow definition at the end of the DAG should use the chain() method rather than
the `>>` notation.  It should also include comment lines to denote the Setup, Body, and
Teardown stages of the test if such stages exist.  For example:

```
chain(
    # TEST SETUP
    test_context,
    create_database,
    create_table,
    # TEST BODY
    copy_selected_data,
    # TEST TEARDOWN
    delete_database,
)
```

## Cleanup

If your test uses any cleanup tasks, they should include `trigger_rule=TriggerRule.ALL_DONE`
to ensure they run even if some test steps fail.  This in turn requires that the `watcher()`
method is added as outlined in [AIP-47](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-47+New+design+of+Airflow+System+Tests).
For example:

```
chain(
    # TEST SETUP
    task0,
    # TEST BODY
    task1,
    # TEST TEARDOWN
    task2, # task2 has trigger rule "all done" defined
)

from tests.system.utils.watcher import watcher

# This test needs watcher in order to properly mark success/failure
# when "tearDown" task with trigger rule is part of the DAG
list(dag.tasks) >> watcher
```
