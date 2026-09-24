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
Package airflow is what a Dag author imports to write the Go body of a task.

Every task handler takes a [Context] first.
Everything Airflow gives the task arrives as a method on it: the logger, the client for
Variables, Connections and XCom, and the identity of the task instance and its Dag run.

	func transform(actx airflow.Context, country string) error {
		actx.Logger().InfoContext(actx, "transforming", "country", country)

		threshold, err := actx.Client().GetVariable(actx, "etl_threshold")
		if err != nil {
			return err
		}
		return writeRows(actx, country, threshold)
	}

[Context] is a context.Context, so pass it to a client call or to http.NewRequestWithContext,
and select on actx.Done(), which fires when the supervisor asks the task to stop.
Cleanup that must outlive that cancellation runs under context.WithoutCancel(actx).

A helper typed as a plain context.Context recovers the same surface with [FromContext].
*/
package airflow
