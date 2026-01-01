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

// Package shared contains shared data between the worker and plugins.
package shared

import (
	"github.com/hashicorp/go-plugin"
)

// Handshake is a common handshake that is shared by plugin and worker.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion: 1,
	MagicCookieKey:  "AIRFLOW_BUNDLE_MAGIC_COOKIE",
	// This value has no particular meaning, it was just a random uuid
	MagicCookieValue: "23C6AB18-91F9-4760-B3E8-328EF3C861AB",
}
