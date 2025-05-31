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

package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func A_B_c() error {
	return nil
}

func NotErrorRet() int {
	return 0
}

func TestRegisterName(t *testing.T) {
	reg := newRegistry()
	reg.RegisterTask("mydag", A_B_c)

	task, found := reg.LookupTask("mydag", "A_B_c")
	assert.True(t, found)
	assert.NotNil(t, task)
}

func TestNotAFunction(t *testing.T) {
	reg := newRegistry()
	assert.PanicsWithError(t, "task fn was a string, not a func", func() {
		reg.RegisterTask("mydag", "not a fn")
	})
}

func TestTaskReturnNotError(t *testing.T) {
	reg := newRegistry()
	assert.PanicsWithError(
		t,
		"error registering task \"NotErrorRet\" for DAG \"mydag\": expected task function github.com/apache/airflow/go-sdk/worker.NotErrorRet last return value to return error but found int",
		func() {
			reg.RegisterTask("mydag", NotErrorRet)
		},
	)
}

func TestRegisterDuplicateTask(t *testing.T) {
	reg := newRegistry()
	reg.RegisterTask("mydag", A_B_c)
	assert.PanicsWithError(t, `taskId "A_B_c" is already registered for DAG "mydag"`, func() {
		reg.RegisterTask("mydag", A_B_c)
	})
}
