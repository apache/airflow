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

This module is for custom Boto3 waiter configuration files.  Since documentation
on creating custom waiters is pretty sparse out in the wild, this document can
act as a rough quickstart guide.  It is not meant to cover all edge cases.

# To add a new custom waiter

## Create or modify the service waiter config file

Find or create a file for the service it is related to, for example waiters/eks.json

### In the service waiter config file

Build or add to the waiter model config json in that file.  For examples of what these
should look like, have a look through some official waiter models.  Some examples:

* [Cloudwatch](https://github.com/boto/botocore/blob/develop/botocore/data/cloudwatch/2010-08-01/waiters-2.json)
* [EC2](https://github.com/boto/botocore/blob/develop/botocore/data/ec2/2016-11-15/waiters-2.json)
* [EKS](https://github.com/boto/botocore/blob/develop/botocore/data/eks/2017-11-01/waiters-2.json)

Below is an example of a working waiter model config that will make an EKS waiter which will wait for
all Nodegroups in a cluster to be deleted. An explanation follows the code snippet.  Note the backticks
to escape the integers in the "argument" values.

```json
{
    "version": 2,
    "waiters": {
        "all_nodegroups_deleted": {
            "operation": "ListNodegroups",
            "delay": 30,
            "maxAttempts": 60,
            "acceptors": [
                {
                    "matcher": "path",
                    "argument": "length(nodegroups[]) == `0`",
                    "expected": true,
                    "state": "success"
                },
                {
                    "matcher": "path",
                    "expected": true,
                    "argument": "length(nodegroups[]) > `0`",
                    "state": "retry"
                }
            ]
        }
    }
}
```

In the model config above we create a new waiter called `all_nodegroups_deleted` which calls
the `ListNodegroups` API endpoint.  The parameters for the endpoint call must be passed into
the `waiter.wait()` call, the same as when using an official waiter.  The waiter then performs
"argument" (in this case `len(result) == 0`) on the result.  If the argument returns the value
in "expected" (in this case `True`) then the waiter's state is set to `success`, the waiter can
close down, and the operator which called it can continue.  If `len(result) > 0` is `True` then
the state is set to `retry`.  The waiter will "delay" 30 seconds before trying again.  If the
state does not go to `success` before the maxAttempts number of tries, the waiter raises a
WaiterException. Both `retry` and `maxAttempts` can be overridden by the user when calling
`waiter.wait()` like any other waiter.

### That's It!

The AwsBaseHook handles the rest.  Using the above waiter will look like this:
`EksHook().get_waiter("all_nodegroups_deleted").wait(clusterName="my_cluster")`
and for testing purposes, a `list_custom_waiters()` helper method is proved which can
be used the same way: `EksHook().list_custom_waiters()`


### In your Operators (How to use these)

Once configured correctly, the custom waiter will be nearly indistinguishable from an official waiter.
Below is an example of an official waiter followed by a custom one.

```python
EksHook().conn.get_waiter("nodegroup_deleted").wait(clusterName=cluster_name, nodegroupName=nodegroup_name)
EksHook().get_waiter("all_nodegroups_deleted").wait(clusterName=cluster_name)
```

Note that since the get_waiter is in the hook instead of on the client side, a custom waiter is
just `hook.get_waiter` and not `hook.conn.get_waiter`.  Other than that, they should be identical.
