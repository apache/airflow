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

// Package genlicense inserts the Apache license header into a generated Go file.
// A generator that writes a file itself carries the header in its template; one
// that shells out to go-jsonschema, which emits none, calls EnsureHeader so that
// the committed file is reproducible from go generate alone and a drift check sees
// no difference.
package genlicense

import (
	"bytes"
	"os"
	"strings"
)

const header = `// Licensed to the Apache Software Foundation (ASF) under one
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
// under the License.`

// EnsureHeader inserts the header into the file at path, after the "Code
// generated" line that has to stay first for golines to leave it alone. It is
// idempotent, so a generator can call it on every run.
func EnsureHeader(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if bytes.Contains(src, []byte("Licensed to the Apache Software Foundation")) {
		return nil
	}
	lines := strings.Split(string(src), "\n")
	insertAt := 0
	for i, line := range lines {
		if strings.HasPrefix(line, "// Code generated") {
			insertAt = i + 1
			break
		}
	}
	out := append([]string{}, lines[:insertAt]...)
	out = append(out, strings.Split(header, "\n")...)
	out = append(out, lines[insertAt:]...)
	return os.WriteFile(path, []byte(strings.Join(out, "\n")), 0o644)
}
