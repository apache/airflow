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

package bundlev1server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecideMode(t *testing.T) {
	tests := []struct {
		name     string
		metadata bool
		comm     string
		logs     string
		want     serveMode
	}{
		{name: "metadata", metadata: true, want: modeAirflowMetadata},
		{name: "coordinator", comm: "127.0.0.1:1", logs: "127.0.0.1:2", want: modeCoordinator},
		{name: "no flags", want: modeCoordinatorUsageError},
		{name: "comm only", comm: "127.0.0.1:1", want: modeCoordinatorUsageError},
		{name: "logs only", logs: "127.0.0.1:2", want: modeCoordinatorUsageError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, decideMode(tt.metadata, tt.comm, tt.logs))
		})
	}
}
