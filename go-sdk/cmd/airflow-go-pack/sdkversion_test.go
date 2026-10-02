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
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mainModule is a build inside the SDK's own module: the main module has no checksum, and its
// dependencies do.
func mainModule(version string) *debug.BuildInfo {
	return &debug.BuildInfo{
		Main: debug.Module{Path: sdkModulePath, Version: version},
		Deps: []*debug.Module{{Path: "github.com/spf13/cobra", Version: "v1.10.1", Sum: "h1:dep="}},
	}
}

// vendoredTool is the packer run by `go tool` from a module that vendors its dependencies: a
// vendored build records no checksum for any module.
func vendoredTool(version string) *debug.BuildInfo {
	return &debug.BuildInfo{
		Main: debug.Module{Path: sdkModulePath, Version: version},
		Deps: []*debug.Module{{Path: "github.com/spf13/cobra", Version: "v1.10.1"}},
	}
}

// tool is the packer run by `go tool` from another module: it reports the SDK as its main
// module, with the module's version and its checksum or replacement.
func tool(version string, replace *debug.Module) *debug.BuildInfo {
	sum := ""
	if replace == nil {
		sum = "h1:abc="
	}
	return &debug.BuildInfo{
		Main: debug.Module{Path: sdkModulePath, Version: version, Sum: sum, Replace: replace},
		Deps: []*debug.Module{{Path: "github.com/spf13/cobra", Version: "v1.10.1", Sum: "h1:dep="}},
	}
}

func dependency(version string, replace *debug.Module) *debug.BuildInfo {
	return &debug.BuildInfo{
		Main: debug.Module{Path: "example.com/bundle", Version: "(devel)"},
		Deps: []*debug.Module{
			{Path: "github.com/spf13/cobra", Version: "v1.10.1"},
			{Path: sdkModulePath, Version: version, Replace: replace},
		},
	}
}

func withoutSDK() *debug.BuildInfo {
	return &debug.BuildInfo{
		Main: debug.Module{Path: "example.com/other", Version: "(devel)"},
		Deps: []*debug.Module{{Path: "github.com/spf13/cobra", Version: "v1.10.1"}},
	}
}

func TestCheckSDKVersion(t *testing.T) {
	local := &debug.Module{Path: "/src/go-sdk", Version: "(devel)"}
	for _, tc := range []struct {
		name    string
		packer  *debug.BuildInfo
		binary  *debug.BuildInfo
		version string
		errors  []string
	}{
		{
			name:    "the SDK is the main module of both builds, whatever its versions",
			packer:  mainModule("v0.0.0-20261002120000-abcdef123456"),
			binary:  mainModule("(devel)"),
			version: "(devel)",
		},
		{
			name:    "the SDK is the main module of both builds and the binary has no version",
			packer:  mainModule("(devel)"),
			binary:  mainModule(""),
			version: "(devel)",
		},
		{
			name:    "the SDK is a dependency of both builds at one version",
			packer:  dependency("v0.1.0", nil),
			binary:  dependency("v0.1.0", nil),
			version: "v0.1.0",
		},
		{
			name:   "the SDK is a dependency of both builds at two versions",
			packer: dependency("v0.2.0", nil),
			binary: dependency("v0.1.0", nil),
			errors: []string{"go-sdk v0.1.0", "go-sdk v0.2.0", "go tool airflow-go-pack"},
		},
		{
			name:    "both builds replace the SDK with the same local path",
			packer:  dependency("v0.0.0", local),
			binary:  dependency("v0.0.0", local),
			version: "(devel)",
		},
		{
			name:   "one build replaces the SDK",
			packer: dependency("v0.1.0", nil),
			binary: dependency("v0.1.0", local),
			errors: []string{"go-sdk v0.1.0 replaced by /src/go-sdk@(devel)", "go-sdk v0.1.0"},
		},
		{
			name:   "the builds replace the SDK with two paths",
			packer: dependency("v0.1.0", local),
			binary: dependency("v0.1.0", &debug.Module{Path: "/other/go-sdk", Version: "(devel)"}),
			errors: []string{"replaced by /other/go-sdk", "replaced by /src/go-sdk"},
		},
		{
			name:    "both builds replace the SDK with one released fork",
			packer:  dependency("v0.1.0", &debug.Module{Path: "example.com/fork", Version: "v1.2.3"}),
			binary:  dependency("v0.1.0", &debug.Module{Path: "example.com/fork", Version: "v1.2.3"}),
			version: "v1.2.3",
		},
		{
			name:   "the builds replace the SDK with two versions of a fork",
			packer: dependency("v0.1.0", &debug.Module{Path: "example.com/fork", Version: "v1.2.3"}),
			binary: dependency("v0.1.0", &debug.Module{Path: "example.com/fork", Version: "v1.2.4"}),
			errors: []string{"replaced by example.com/fork@v1.2.4", "replaced by example.com/fork@v1.2.3"},
		},
		{
			name:    "the packer is run by go tool and the binary uses the same release",
			packer:  tool("v0.1.0", nil),
			binary:  dependency("v0.1.0", nil),
			version: "v0.1.0",
		},
		{
			name:   "the packer is run by go tool and the binary uses another release",
			packer: tool("v0.2.0", nil),
			binary: dependency("v0.1.0", nil),
			errors: []string{"go-sdk v0.1.0", "go-sdk v0.2.0"},
		},
		{
			name:    "the packer is run by go tool and both replace the SDK with the same local path",
			packer:  tool("v0.0.0", local),
			binary:  dependency("v0.0.0", local),
			version: "(devel)",
		},
		{
			name:   "the packer is run by go tool and the binary is built inside the SDK",
			packer: tool("v0.0.0", local),
			binary: mainModule("(devel)"),
			errors: []string{"go-sdk (devel) (main module)", "go-sdk v0.0.0 replaced by /src/go-sdk@(devel)"},
		},
		{
			name:   "the packer is run by go tool with a released SDK and the binary is built inside the SDK",
			packer: tool("v0.1.0", nil),
			binary: mainModule("(devel)"),
			errors: []string{
				"go-sdk (devel) (main module)",
				"airflow-go-pack was built against go-sdk v0.1.0;",
			},
		},
		{
			name:    "the packer is run by go tool from a vendoring module and the binary uses the same release",
			packer:  vendoredTool("v0.1.0"),
			binary:  dependency("v0.1.0", nil),
			version: "v0.1.0",
		},
		{
			name:   "the packer is run by go tool from a vendoring module and the binary is built inside the SDK",
			packer: vendoredTool("v0.1.0"),
			binary: mainModule("(devel)"),
			errors: []string{
				"go-sdk (devel) (main module)",
				"airflow-go-pack was built against go-sdk v0.1.0;",
			},
		},
		{
			name:    "the packer is built inside a tagged SDK checkout and the binary uses the same release",
			packer:  mainModule("v0.1.0"),
			binary:  dependency("v0.1.0", nil),
			version: "v0.1.0",
		},
		{
			name:   "the packer is built inside the SDK and the binary uses a release",
			packer: mainModule("(devel)"),
			binary: dependency("v0.1.0", nil),
			errors: []string{"go-sdk v0.1.0", "go-sdk (devel) (main module)"},
		},
		{
			name:   "the binary is built inside the SDK and the packer uses a release",
			packer: dependency("v0.1.0", nil),
			binary: mainModule("(devel)"),
			errors: []string{"go-sdk (devel) (main module)", "go-sdk v0.1.0"},
		},
		{
			name:   "the binary does not link the SDK",
			packer: dependency("v0.1.0", nil),
			binary: withoutSDK(),
			errors: []string{"does not link github.com/apache/airflow/go-sdk"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			version, err := checkSDKVersion(tc.packer, tc.binary)
			if len(tc.errors) == 0 {
				require.NoError(t, err)
				assert.Equal(t, tc.version, version)
				return
			}
			require.Error(t, err)
			for _, fragment := range tc.errors {
				assert.Contains(t, err.Error(), fragment)
			}
		})
	}
}

func TestSDKModule_RecordedVersion(t *testing.T) {
	for name, tc := range map[string]struct {
		module sdkModule
		want   string
	}{
		"a version":                 {sdkModule{version: "v0.1.0"}, "v0.1.0"},
		"a replacement version":     {sdkModule{version: "v0.1.0", replaceVersion: "v1.2.3"}, "v1.2.3"},
		"a replacement without one": {sdkModule{version: "v0.0.0", replacePath: "/src/go-sdk"}, "v0.0.0"},
		"no version":                {sdkModule{}, "(devel)"},
		"the main module's version": {sdkModule{version: "v0.1.0", main: true}, "v0.1.0"},
		"a main module without one": {sdkModule{version: "(devel)", main: true}, "(devel)"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.module.recordedVersion())
		})
	}
}

// A test binary reports the SDK module as its main module, as the packer built inside the SDK does.
func TestReadSDK_OfTheFixtureBinary(t *testing.T) {
	sdk, err := readSDK(bundleBinary(t))

	require.NoError(t, err)
	assert.Equal(t, "go", sdk.Language)
	assert.NotEmpty(t, sdk.Version)
	assert.NotEmpty(t, sdk.SupervisorSchemaVersion)
}
