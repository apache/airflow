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

// Package bundlev1server implements a server implementation to run bundlev1
// as gRPC servers, making it accessible to the Airflow Go Workers.
//
// This "serving" is to local process only, and is not exposed over any network
//
// Bundle will likely be calling [Serve] from their main function to
// start the server so Airflow's go worker can connect to it.
package bundlev1server
