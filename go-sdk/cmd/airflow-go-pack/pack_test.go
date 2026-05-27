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
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderManifest_DeterministicDagOrdering(t *testing.T) {
	spec := bundleSpec{
		FormatVersion: "1.0",
		SDK:           bundleSpecSDK{Language: "go", Version: "0.1.0"},
		Dags: map[string]bundleSpecDag{
			"zeta_dag":  {Tasks: []string{"a", "b"}},
			"alpha_dag": {Tasks: []string{"x"}},
		},
	}

	got1, err := renderManifest(spec, "main.go")
	require.NoError(t, err)
	got2, err := renderManifest(spec, "main.go")
	require.NoError(t, err)

	assert.Equal(t, got1, got2, "manifest should be byte-identical for identical input")

	expected := `format_version: "1.0"
sdk:
  language: go
  version: "0.1.0"
source: main.go
dags:
  alpha_dag:
    tasks:
      - x
  zeta_dag:
    tasks:
      - a
      - b
`
	assert.Equal(t, expected, string(got1))
}

func TestRenderManifest_EmptyDags(t *testing.T) {
	spec := bundleSpec{
		FormatVersion: "1.0",
		SDK:           bundleSpecSDK{Language: "go", Version: "0.1.0"},
		Dags:          map[string]bundleSpecDag{},
	}
	got, err := renderManifest(spec, "main.go")
	require.NoError(t, err)
	assert.Contains(t, string(got), "dags: {}")
}

// TestRootArgs_AllowsBuildFlagsAfterDoubleDash regression-tests the
// positional-arg validator: forwarded `go build` flags after "--" must not
// be counted against MaximumNArgs(1). The runtime call would otherwise fail
// with `accepts at most 1 arg(s), received N`.
func TestRootArgs_AllowsBuildFlagsAfterDoubleDash(t *testing.T) {
	cases := [][]string{
		{"--", "-ldflags", "-X main.dagId=foo"},
		{"./pkg", "--", "-ldflags", "-X main.dagId=foo"},
		{"--", "-trimpath", "-tags=prod"},
	}
	for _, argv := range cases {
		cmd := newRootCmd()
		// Stop the command from actually running; we only want arg validation.
		cmd.RunE = func(*cobra.Command, []string) error { return nil }
		cmd.SetArgs(argv)
		assert.NoError(t, cmd.Execute(), "args=%v should validate", argv)
	}
}

func TestRootArgs_RejectsExtraPositionalBeforeDash(t *testing.T) {
	cmd := newRootCmd()
	cmd.RunE = func(*cobra.Command, []string) error { return nil }
	cmd.SetArgs([]string{"./pkg1", "./pkg2", "--", "-ldflags", "-X main.dagId=foo"})
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accepts at most 1 arg")
}
