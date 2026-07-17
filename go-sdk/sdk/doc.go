// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/*
Package sdk gives task functions access to the Airflow "model" (Variables,
Connections, and XCom) at run time.

A task function does not construct a client itself. The runtime inspects the
function's parameters and injects one by type, so you declare the narrowest
interface you need and use it:

	func mytask(ctx context.Context, client sdk.Client, log *slog.Logger) error {
		val, err := client.GetVariable(ctx, "my_variable")
		if err != nil {
			return err
		}
		log.Info("got variable", "value", val)
		return nil
	}

Ask for [Client] for full access, or a narrower interface such as
[VariableClient] or [ConnectionClient] when the task only reads one kind of
object. The narrower type documents what the task touches and makes it easy to
pass a fake in unit tests.

To publish a result, return a value from the task function: the runtime pushes
it as the task's return-value XCom, so most tasks never call [XComClient]
directly. Lookups that miss return a wrapped sentinel error ([VariableNotFound],
[ConnectionNotFound], [XComNotFound]) you can test for with errors.Is.
*/
package sdk
