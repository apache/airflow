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
	"debug/buildinfo"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
	"github.com/apache/airflow/go-sdk/pkg/execution"
)

// sdkModulePath is the import path of the Go SDK module.
const sdkModulePath = "github.com/apache/airflow/go-sdk"

// sdkModule is the Go SDK module as one build includes it.
type sdkModule struct {
	// version is the module version, or "(devel)" for a main module without one.
	version string
	// replacePath and replaceVersion are set when a replace directive substitutes the module.
	// A replacement by a local path has the version "(devel)".
	replacePath    string
	replaceVersion string
	// main is set when the SDK is the main module of the build, as when the build runs inside
	// the SDK's own module.
	main bool
}

// findSDKModule returns how info includes the Go SDK module. ok is false when the build does
// not link it.
func findSDKModule(info *debug.BuildInfo) (module sdkModule, ok bool) {
	if info.Main.Path == sdkModulePath {
		module = sdkModule{version: info.Main.Version}
		if module.version == "" {
			module.version = "(devel)"
		}
		module.replacePath, module.replaceVersion = replacement(info.Main.Replace)
		// The packer run by `go tool` from another module also reports the SDK as its main
		// module, because that is the module of its main package. It carries a checksum or a
		// replacement, which the main module of a build never does. A vendored build records no
		// checksum for any module, so it is the SDK's own build only if a dependency has one.
		module.main = info.Main.Replace == nil && info.Main.Sum == "" && hasChecksummedDep(info)
		return module, true
	}
	for _, dep := range info.Deps {
		if dep.Path != sdkModulePath {
			continue
		}
		module = sdkModule{version: dep.Version}
		module.replacePath, module.replaceVersion = replacement(dep.Replace)
		return module, true
	}
	return sdkModule{}, false
}

func hasChecksummedDep(info *debug.BuildInfo) bool {
	for _, dep := range info.Deps {
		if dep.Sum != "" {
			return true
		}
	}
	return false
}

func replacement(replace *debug.Module) (path, version string) {
	if replace == nil {
		return "", ""
	}
	return replace.Path, replace.Version
}

// recordedVersion is the version written to the manifest: the replacement's version when it has
// one, else the module's, else "(devel)".
func (m sdkModule) recordedVersion() string {
	switch {
	case m.replaceVersion != "":
		return m.replaceVersion
	case m.version != "":
		return m.version
	default:
		return "(devel)"
	}
}

// sameBuildAs reports whether both builds include the same code of the SDK: the same version
// and the same replacement.
func (m sdkModule) sameBuildAs(other sdkModule) bool {
	return m.version == other.version &&
		m.replacePath == other.replacePath &&
		m.replaceVersion == other.replaceVersion
}

func (m sdkModule) String() string {
	description := m.version
	if m.main {
		description += " (main module)"
	}
	if m.replacePath != "" {
		description += " replaced by " + m.replacePath
		if m.replaceVersion != "" {
			description += "@" + m.replaceVersion
		}
	}
	return description
}

// checkSDKVersion refuses a bundle binary built against another Go SDK than the packer and
// returns the SDK version to record for it.
//
// The packer writes its own supervisor schema version into the manifest, which is only right
// for a binary built against the same SDK. When the SDK is the main module of both builds,
// as when packing inside the SDK module, the two are accepted without a comparison: Go stamps
// the main module's version from version control, so two builds of the same sources can differ.
// Otherwise the builds must include the same version and the same replacement of it.
func checkSDKVersion(packer, binary *debug.BuildInfo) (string, error) {
	linked, ok := findSDKModule(binary)
	if !ok {
		return "", fmt.Errorf(
			"the bundle binary does not link %s, so it is not a Go SDK bundle",
			sdkModulePath,
		)
	}
	own, ok := findSDKModule(packer)
	if !ok {
		return "", fmt.Errorf("airflow-go-pack's build information does not list %s", sdkModulePath)
	}
	if !(linked.main && own.main) && !own.sameBuildAs(linked) {
		return "", fmt.Errorf(
			"the bundle binary was built against go-sdk %s, but airflow-go-pack was built "+
				"against go-sdk %s; pack with `go tool airflow-go-pack` from the module that "+
				"built the binary, so both use the same go-sdk",
			linked, own,
		)
	}
	return linked.recordedVersion(), nil
}

// readSDK reads the SDK block of the manifest for the bundle binary at path, without running
// it. The version comes from the binary's build information, which Go keeps in a stripped
// binary and for any target platform. The supervisor schema version is the packer's own, which
// checkSDKVersion has shown to be the binary's too.
func readSDK(path string) (airflowmetadata.SDK, error) {
	binary, err := buildinfo.ReadFile(path)
	if err != nil {
		return airflowmetadata.SDK{}, fmt.Errorf(
			"reading the build information of %s: %w; it must be a binary built from a Go package",
			path, err,
		)
	}
	packer, ok := debug.ReadBuildInfo()
	if !ok {
		return airflowmetadata.SDK{}, errors.New(
			"airflow-go-pack has no build information to compare",
		)
	}
	version, err := checkSDKVersion(packer, binary)
	if err != nil {
		return airflowmetadata.SDK{}, err
	}
	return airflowmetadata.SDK{
		Language:                "go",
		Version:                 version,
		SupervisorSchemaVersion: execution.SupervisorSchemaVersion,
	}, nil
}
