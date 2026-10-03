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

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// testSDKModule is the module path of go-sdk, spelled out so tests do not rely on the packer's.
const testSDKModule = "github.com/apache/airflow/go-sdk"

var (
	fixtureOnce sync.Once
	fixtureDir  string
	fixturePath string
	fixtureErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if fixtureDir != "" {
		_ = os.RemoveAll(fixtureDir)
	}
	os.Exit(code)
}

func requireGo(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("shells out to the go toolchain")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}
}

// bundleBinary returns a host binary built once per test run from testdata/failingbundle. It
// links go-sdk and exits with status 3 when it runs, so a test that packs it shows that the
// packer does not run it.
func bundleBinary(t *testing.T) string {
	t.Helper()
	requireGo(t)
	fixtureOnce.Do(func() {
		fixtureDir, fixtureErr = os.MkdirTemp("", "airflow-go-pack-test-*")
		if fixtureErr != nil {
			return
		}
		fixturePath = filepath.Join(fixtureDir, "failingbundle")
		cmd := exec.Command("go", "build", "-o", fixturePath, "./testdata/failingbundle")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			fixtureErr = fmt.Errorf("building testdata/failingbundle: %w\n%s", err, out)
		}
	})
	require.NoError(t, fixtureErr)
	return fixturePath
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

// crossArchFor returns an architecture different from the host that the Go
// toolchain can target, or "" if we have no safe mapping for this host.
func crossArchFor(hostArch string) string {
	switch hostArch {
	case "amd64":
		return "arm64"
	case "arm64":
		return "amd64"
	default:
		return ""
	}
}

func requireCrossArch(t *testing.T) string {
	t.Helper()
	requireGo(t)
	crossArch := crossArchFor(runtime.GOARCH)
	if crossArch == "" {
		t.Skipf("no cross-arch mapping for host arch %q", runtime.GOARCH)
	}
	return crossArch
}
