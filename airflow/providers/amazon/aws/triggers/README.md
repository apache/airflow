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

# Writing Deferrable Operators for Amazon Provider Package


Before writing deferrable operators, it is strongly recommended to read and familiarize yourself with the official [documentation](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html) of Deferrable Operators.
The purpose of this guide is to provide a standardized way to convert existing Amazon Provider Package (AMPP) operators to deferrable operators. Due to the varied complexities of available operators, it is impossible to define one method that will work for every operator.
The method described in this guide should work for many of the AMPP operators, but it is important to study each operator before determining whether the steps outlined below are applicable.

Although it varies from operator to operator, a typical AMPP operator has 3 stages:

1. A pre-processing stage, where information is looked up via boto3 API calls, parameters are formatted etc. The complexity of this stage depends on the complexity of the task the operator is attempting to do. Some operators (e.g. Sagemaker) have a lot of pre-processing, whereas others require little to no pre-processing.
2. The "main" call to the boto3 API to start an operation. This is the task that the operator is attempting to complete. This could be a request to provision a resource, request to change the state of a resource, start a job on a resource etc. Regardless of the operation, the boto3 API returns a response instantly (ignoring network delays) with a response detailing the results of the query. For example, in the case of a resource provisioning request, although the resource can take significant time to be allocated, the boto3 API returns a response to the caller without waiting for the operation to be completed.
3. The last, often optional, stage is to wait for the operation initiated in stage 2 to be completed. This usually involves polling the boto3 API at set intervals, and waiting for a certain criteria to be met.

In general, it is the last polling stage where we can defer the operator to a trigger which can handle the polling operation. The botocore library defines waiters for certain services, which are built-in functions that poll a service and wait for a given criteria to be met.
As part of our work for writing deferrable operators, we have extended the built-in waiters to allow custom waiters, which follow the same logic, but for services not included in the botocore library.
We can use these custom waiters, along with the built-in waiters to implement the polling logic of the deferrable operators.

The first step to making an existing operator deferrable is to add `deferrable` as a parameter to the operator, and initialize it in the constructor of the operator.
The next step is to determine where the operator should be deferred. This will be dependent on what the operator does, and how it is written. Although every operator is different, there are a few guidelines to determine the best place to defer an operator.

1. If the operator has a `wait_for_completion` parameter, the `self.defer` method should be called right before the check for wait_for_completion .
2. If there is no `wait_for_completion` , look for the "main" task that the operator does. Often, operators will make various describe calls to the boto3 API to verify certain conditions, or look up some information before performing its "main" task. Often, right after the "main" call to the boto3 API is made is a good place to call `self.defer`.


Once the location to defer is decided in the operator, call the `self.defer` method if the `deferrable` flag is `True`. The `self.defer` method takes in several parameters, listed below:

1. `trigger`: This is the trigger which you want to pass the execution to. We will write this trigger in just a moment.
2. `method_name`: This specifies the name of the method you want to execute once the trigger completes its execution. The trigger cannot pass the execution back to the execute method of the operator. By convention, the name for this method is `execute_complete`.
3. `timeout`: An optional parameter that controls the length of time the Trigger can execute for before timing out. This defaults to `None`, meaning no timeout.
4. `kwargs`: Additional keyword arguments to pass to `method_name`. Default is `{}`.

The Trigger is the main component of deferrable operators. They must be placed in the `airflow/providers/amazon/aws/triggers/` folder. All Triggers must implement the following 3 methods:

1. `__init__`: the constructor which receives parameters from the operator. These must be JSON serializable.
2. `serialize`: a function that returns the classpath, as well as keyword arguments to the `__init__`  method as a tuple
3. `run` : the asynchronous function that is responsible for awaiting the asynchronous operations.

Ideally, when the operator has deferred itself, it has already initiated the "main" task of the operator, and is now waiting for a certain resource to reach a certain state.
As mentioned earlier, the botocore library defines a `Waiter` class for many services, which implements a `wait` method that can be configured via a config file to poll the boto3 API at set intervals, and return if the success criteria is met.
The aiobotocore library, which is used to make asynchronous botocore calls, defines an `AIOWaiter` class, which also implements a wait method that behaves identical to the botocore method, except that it works asynchronously.
Therefore, any botocore waiter is available as an aiobotocore waiter, and can be used to asynchronously poll a service until the desired criteria is met.

To call the asynchronous `wait` function, first create a hook for the particular service. For example, for a Redshift hook, it would look like this:

```python
self.redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
```

With this hook, we can use the async_conn property to get access to the aiobotocore client:

```python
async with self.redshift_hook.async_conn as client:
    await client.get_waiter("cluster_available").wait(
        ClusterIdentifier=self.cluster_identifier,
        WaiterConfig={
            "Delay": int(self.poll_interval),
            "MaxAttempts": int(self.max_attempt),
        },
    )
```

In this case, we are using the built-in cluster_available waiter. If we wanted to use a custom waiter, we would change the code slightly to use the `get_waiter` function from the hook, rather than the aiobotocore client:

```python
async with self.redshift_hook.async_conn as client:
    waiter = self.redshift_hook.get_waiter("cluster_paused", deferrable=True, client=client)
    await waiter.wait(
        ClusterIdentifier=self.cluster_identifier,
        WaiterConfig={
            "Delay": int(self.poll_interval),
            "MaxAttempts": int(self.max_attempt),
        },
    )
```

Here, we are calling the `get_waiter` function defined in `base_aws.py` which takes an optional argument of `deferrable` (set to `True`), and the `aiobotocore` client. `cluster_paused` is a custom boto waiter defined in `redshift.json`  in the `airflow/providers/amazon/aws/waiters` folder. In general, the config file for a custom waiter should be named as `<service_name>.json`. The config for `cluster_paused` is shown below:

```json
{
    "version": 2,
    "waiters": {
        "cluster_paused": {
            "operation": "DescribeClusters",
            "delay": 30,
            "maxAttempts": 60,
            "acceptors": [
                {
                    "matcher": "pathAll",
                    "argument": "Clusters[].ClusterStatus",
                    "expected": "paused",
                    "state": "success"
                },
                {
                    "expected": "ClusterNotFound",
                    "matcher": "error",
                    "state": "retry"
                },
                {
                    "expected": "deleting",
                    "matcher": "pathAny",
                    "state": "failure",
                    "argument": "Clusters[].ClusterStatus"
                }
            ]
        },
    }
}
```

For more information about writing custom waiter, see the [README.md](https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/waiters/README.md) for custom waiters.

In some cases, a built-in or custom waiter may not be able to solve the problem. In such cases, the asynchronous method used to poll the boto3 API would need to be defined in the hook of the service being used. This method is essentially the same as the synchronous version of the method, except that it will use the aiobotocore client, and will be awaited. For the Redshift example, the async `describe_clusters` method would look as follows:

```python
async with self.async_conn as client:
    response = client.describe_clusters(ClusterIdentifier=self.cluster_identifier)
```

This async method can be used in the Trigger to poll the boto3 API. The polling logic will need to be implemented manually, taking care to use `asyncio.sleep()` rather than `time.sleep()`.

The last step in the Trigger is to yield a `TriggerEvent` that will be used to alert the `Triggerer` that the Trigger has finished execution. The `TriggerEvent` can pass information from the trigger to the `method_name` method named in the `self.defer` call in the operator. In the Redshift example, the `TriggerEvent` would look as follows:

```
yield TriggerEvent({"status": "success", "message": "Cluster Created"})
```

The object passed through the `TriggerEvent` can be captured in the `method_name` method through an `event` parameter. This can be used to determine what needs to be done based on the outcome of the Trigger execution. In the Redshift case, we can simply check the status of the event, and raise an Exception if something went wrong.

```python
def execute_complete(self, context, event=None):
    if event["status"] != "success":
        raise AirflowException(f"Error creating cluster: {event}")
    return
```
